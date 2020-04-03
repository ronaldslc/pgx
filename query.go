package pgx

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"time"

	"github.com/pkg/errors"

	"github.com/ronaldslc/pgx/internal/sanitize"
	"github.com/ronaldslc/pgx/pgproto3"
	"github.com/ronaldslc/pgx/pgtype"
)

// Row is a convenience wrapper over Rows that is returned by QueryRow.
type Row Rows

// Scan works the same as (*Rows Scan) with the following exceptions. If no
// rows were found it returns ErrNoRows. If multiple rows are returned it
// ignores all but the first.
func (r *Row) Scan(dest ...interface{}) (err error) {
	rows := (*Rows)(r)

	if rows.Err() != nil {
		return rows.Err()
	}

	if !rows.Next() {
		if rows.Err() == nil {
			return ErrNoRows
		}
		return rows.Err()
	}

	rows.Scan(dest...)
	rows.Close()
	return rows.Err()
}

// Rows is the result set returned from *Conn.Query. Rows must be closed before
// the *Conn can be used again. Rows are closed by explicitly calling Close(),
// calling Next() until it returns false, or when a fatal error occurs.
type Rows struct {
	conn       *Conn
	connPool   *ConnPool
	batch      *Batch
	values     [][][]byte
	fields     []FieldDescription
	rowCount   int
	columnIdx  int
	err        error
	startTime  time.Time
	sql        string
	args       []interface{}
	unlockConn bool
	closed     bool

	// the count of row which need to be read in Scan
	// if it is large then rowIdx, rows.Next will not get the row data from Reader
	// and return the pendingRowCount - rowIdx
	pendingRowCount int
	rowIdx          int // the index of not yet scanned row
}

func (rows *Rows) FieldDescriptions() []FieldDescription {
	return rows.fields
}

// Close closes the rows, making the connection ready for use again. It is safe
// to call Close after rows is already closed.
func (rows *Rows) Close() {
	if rows.closed {
		return
	}

	if rows.unlockConn {
		rows.conn.unlock()
		rows.unlockConn = false
	}

	rows.closed = true

	rows.err = rows.conn.termContext(rows.err)

	if rows.err == nil {
		if rows.conn.shouldLog(LogLevelInfo) {
			endTime := time.Now()
			var ld LogData
			ld.Add("time", endTime.Sub(rows.startTime))
			ld.Add("rowCount", rows.rowCount)
			ld.Add("sql", rows.sql)
			ld.Add("args", logQueryArgs(rows.args))
			rows.conn.log(LogLevelInfo, "Query", ld)
		}
	} else if rows.conn.shouldLog(LogLevelError) {
		var ld LogData
		ld.Add("sql", rows.sql)
		ld.Add("args", logQueryArgs(rows.args))
		rows.conn.log(LogLevelError, "Query", ld)
	}

	if rows.batch != nil && rows.err != nil {
		rows.batch.die(rows.err)
	}

	if rows.connPool != nil {
		rows.connPool.Release(rows.conn)
	}
}

func (rows *Rows) Err() error {
	return rows.err
}

// fatal signals an error occurred after the query was sent to the server. It
// closes the rows automatically.
func (rows *Rows) fatal(err error) {
	if rows.err != nil {
		return
	}

	rows.err = err
	rows.Close()
}

// Next prepares the next rows for reading. It returns true if there is another
// row and false if no more rows are available. It automatically closes rows
// when all rows are read.
func (rows *Rows) Next() bool {
	return rows.BatchNext() > 0
}

// BatchNext prepares the next rows for reading. It returns row count (int) which more then 0 if there is another
// row and 0 if no more rows are available. It automatically closes rows
// when all rows are read.
func (rows *Rows) BatchNext() int {
	if rows.closed {
		return 0
	}

	// if there are rows which are no yet read, return the count
	if rows.pendingRowCount > rows.rowIdx {
		return rows.pendingRowCount - rows.rowIdx
	}

	// reset index and count
	rows.columnIdx = 0
	rows.rowIdx = 0
	rows.pendingRowCount = 0

outer:
	for idx := rows.conn.rmsgs.Readable(); idx > 0; idx-- {
		msg, msgBody, err := rows.conn.rmsgs.Read()
		if err != nil {
			rows.fatal(err)
			return 0
		}

		if _, ok := msg.(*pgproto3.CommandComplete); ok && rows.pendingRowCount > 0 {
			rows.conn.rmsgs.Backward()
			return rows.pendingRowCount
		}

		if err := msg.Decode(msgBody); err != nil {
			rows.fatal(err)
			return 0
		}

		switch msg := msg.(type) {
		case *pgproto3.RowDescription:
			rows.fields = rows.conn.rxRowDescription(msg)
			for i := range rows.fields {
				if dt, ok := rows.conn.ConnInfo.DataTypeForOID(rows.fields[i].DataType); ok {
					rows.fields[i].DataTypeName = dt.Name
					rows.fields[i].FormatCode = TextFormatCode
				} else {
					rows.fatal(errors.Errorf("unknown oid: %d", rows.fields[i].DataType))
					return 0
				}
			}
		case *pgproto3.DataRow:
			if len(msg.Values) != len(rows.fields) {
				rows.fatal(ProtocolError(fmt.Sprintf("Row description field count (%v) and data row field count (%v) do not match", len(rows.fields), len(msg.Values))))
				return 0
			}

			rows.values[rows.pendingRowCount] = make([][]byte, len(msg.Values))
			for i := range msg.Values {
				if msg.Values[i] != nil {
					rows.values[rows.pendingRowCount][i] = make([]byte, len(msg.Values[i]))
					copy(rows.values[rows.pendingRowCount][i], msg.Values[i])
				}
			}
			rows.rowCount++
			rows.pendingRowCount++
			if rows.pendingRowCount >= len(rows.values) {
				break outer
			}
		case *pgproto3.CommandComplete:
			rows.rowCount++
			rows.Close()
			return 0
		default:
			err := rows.conn.processContextFreeMsg(msg)
			if err != nil {
				rows.fatal(err)
				return 0
			}
		}
	}

	if err := rows.batchRead(); err != nil {
		rows.fatal(err)
		return 0
	}

	if rows.pendingRowCount == 0 {
		return rows.BatchNext()
	}
	return rows.pendingRowCount
}

func (rows *Rows) nextColumn() ([]byte, *FieldDescription, bool) {
	if rows.closed {
		return nil, nil, false
	}
	if len(rows.fields) <= rows.columnIdx {
		rows.fatal(ProtocolError("No next column available"))
		return nil, nil, false
	}

	buf := rows.values[rows.rowIdx][rows.columnIdx]
	fd := &rows.fields[rows.columnIdx]
	rows.columnIdx++
	return buf, fd, true
}

// function to batch get message and message body and stored to Rows
func (rows *Rows) batchRead() (err error) {
	if rows.closed {
		return nil
	}
	err = rows.conn.frontend.Receive(rows.conn.rmsgs)
	if err != nil {
		if netErr, ok := err.(net.Error); !(ok && netErr.Timeout()) {
			rows.conn.die(err)
		}
	}
	return
}

type scanArgError struct {
	col int
	err error
}

func (e scanArgError) Error() string {
	return fmt.Sprintf("can't scan into dest[%d]: %v", e.col, e.err)
}

// Scan reads the values from the current row into dest values positionally.
// dest can include pointers to core types, values implementing the Scanner
// interface, []byte, and nil. []byte will skip the decoding process and directly
// copy the raw bytes received from PostgreSQL. nil will skip the value entirely.
// NOTE: Scan can only be called once to get a row data, the next Scan will get the next row data if exist
func (rows *Rows) Scan(dest ...interface{}) (err error) {
	if len(rows.fields) != len(dest) {
		err = errors.Errorf("Scan received wrong number of arguments, got %d but expected %d", len(dest), len(rows.fields))
		rows.fatal(err)
		return err
	}

	if rows.rowIdx >= rows.pendingRowCount {
		err = errors.New("no row data")
		rows.fatal(err)
		return err
	}

	rows.columnIdx = 0

	for i, d := range dest {
		buf, fd, _ := rows.nextColumn()

		if d == nil {
			continue
		}

		if s, ok := d.(pgtype.BinaryDecoder); ok && fd.FormatCode == BinaryFormatCode {
			err = s.DecodeBinary(rows.conn.ConnInfo, buf)
			if err != nil {
				rows.fatal(scanArgError{col: i, err: err})
			}
		} else if s, ok := d.(pgtype.TextDecoder); ok && fd.FormatCode == TextFormatCode {
			err = s.DecodeText(rows.conn.ConnInfo, buf)
			if err != nil {
				rows.fatal(scanArgError{col: i, err: err})
			}
		} else {
			if dt, ok := rows.conn.ConnInfo.DataTypeForOID(fd.DataType); ok {
				value := dt.Value
				switch fd.FormatCode {
				case TextFormatCode:
					if textDecoder, ok := value.(pgtype.TextDecoder); ok {
						err = textDecoder.DecodeText(rows.conn.ConnInfo, buf)
						if err != nil {
							rows.fatal(scanArgError{col: i, err: err})
						}
					} else {
						rows.fatal(scanArgError{col: i, err: errors.Errorf("%T is not a pgtype.TextDecoder", value)})
					}
				case BinaryFormatCode:
					if binaryDecoder, ok := value.(pgtype.BinaryDecoder); ok {
						err = binaryDecoder.DecodeBinary(rows.conn.ConnInfo, buf)
						if err != nil {
							rows.fatal(scanArgError{col: i, err: err})
						}
					} else {
						rows.fatal(scanArgError{col: i, err: errors.Errorf("%T is not a pgtype.BinaryDecoder", value)})
					}
				default:
					rows.fatal(scanArgError{col: i, err: errors.Errorf("unknown format code: %v", fd.FormatCode)})
				}

				if rows.Err() == nil {
					if scanner, ok := d.(sql.Scanner); ok {
						sqlSrc, err := pgtype.DatabaseSQLValue(rows.conn.ConnInfo, value)
						if err != nil {
							rows.fatal(err)
						}
						err = scanner.Scan(sqlSrc)
						if err != nil {
							rows.fatal(scanArgError{col: i, err: err})
						}
					} else if err := value.AssignTo(d); err != nil {
						rows.fatal(scanArgError{col: i, err: err})
					}
				}
			} else {
				rows.fatal(scanArgError{col: i, err: errors.Errorf("unknown oid: %v", fd.DataType)})
			}
		}

		if rows.Err() != nil {
			return rows.Err()
		}
	}
	rows.rowIdx++

	return nil
}

// Values returns an array of the row values
func (rows *Rows) Values() ([]interface{}, error) {
	if rows.closed {
		return nil, errors.New("rows is closed")
	}

	values := make([]interface{}, 0, len(rows.fields))
	rows.columnIdx = 0

	for range rows.fields {
		buf, fd, _ := rows.nextColumn()

		if buf == nil {
			values = append(values, nil)
			continue
		}

		if dt, ok := rows.conn.ConnInfo.DataTypeForOID(fd.DataType); ok {
			value := dt.Value

			switch fd.FormatCode {
			case TextFormatCode:
				decoder := value.(pgtype.TextDecoder)
				if decoder == nil {
					decoder = &pgtype.GenericText{}
				}
				err := decoder.DecodeText(rows.conn.ConnInfo, buf)
				if err != nil {
					rows.fatal(err)
				}
				values = append(values, decoder.(pgtype.Value).Get())
			case BinaryFormatCode:
				decoder := value.(pgtype.BinaryDecoder)
				if decoder == nil {
					decoder = &pgtype.GenericBinary{}
				}
				err := decoder.DecodeBinary(rows.conn.ConnInfo, buf)
				if err != nil {
					rows.fatal(err)
				}
				values = append(values, value.Get())
			default:
				rows.fatal(errors.New("Unknown format code"))
			}
		} else {
			rows.fatal(errors.New("Unknown type"))
		}

		if rows.Err() != nil {
			return nil, rows.Err()
		}
	}
	rows.rowIdx++

	return values, rows.Err()
}

// Query executes sql with args. If there is an error the returned *Rows will
// be returned in an error state. So it is allowed to ignore the error returned
// from Query and handle it in *Rows.
func (c *Conn) Query(sql string, args ...interface{}) (*Rows, error) {
	return c.QueryEx(context.Background(), 0, sql, nil, args...)
}

func (c *Conn) QueryWithBufferSize(bufferSize int, sql string, args ...interface{}) (*Rows, error) {
	return c.QueryEx(context.Background(), bufferSize, sql, nil, args...)
}

func (c *Conn) getRows(bufferSize int, sql string, args []interface{}) *Rows {
	if len(c.preallocatedRows) == 0 {
		c.preallocatedRows = make([]Rows, 64)
	}

	r := &c.preallocatedRows[len(c.preallocatedRows)-1]
	c.preallocatedRows = c.preallocatedRows[0 : len(c.preallocatedRows)-1]

	r.conn = c
	r.startTime = c.lastActivityTime
	r.sql = sql
	r.args = args

	if bufferSize <= 0 {
		// default maximum buffer size to 100
		bufferSize = 100
	}
	// pre-allocated value and message array capacity
	r.values = make([][][]byte, bufferSize)

	if c.rmsgs.Len() < bufferSize {
		c.rmsgs.SetCapacity(bufferSize)
	}

	return r
}

// QueryRow is a convenience wrapper over Query. Any error that occurs while
// querying is deferred until calling Scan on the returned *Row. That *Row will
// error with ErrNoRows if no rows are returned.
func (c *Conn) QueryRow(sql string, args ...interface{}) *Row {
	rows, _ := c.Query(sql, args...)
	return (*Row)(rows)
}

type QueryExOptions struct {
	// When ParameterOIDs are present and the query is not a prepared statement,
	// then ParameterOIDs and ResultFormatCodes will be used to avoid an extra
	// network round-trip.
	ParameterOIDs     []pgtype.OID
	ResultFormatCodes []int16

	SimpleProtocol bool
}

func (c *Conn) QueryEx(ctx context.Context, maxRowCount int, sql string, options *QueryExOptions, args ...interface{}) (rows *Rows, err error) {
	err = c.waitForPreviousCancelQuery(ctx)
	if err != nil {
		return nil, err
	}

	if err := c.ensureConnectionReadyForQuery(); err != nil {
		return nil, err
	}

	c.lastActivityTime = time.Now()

	rows = c.getRows(maxRowCount, sql, args)

	if err := c.lock(); err != nil {
		rows.fatal(err)
		return rows, err
	}
	rows.unlockConn = true

	err = c.initContext(ctx)
	if err != nil {
		rows.fatal(err)
		return rows, rows.err
	}

	if options != nil && options.SimpleProtocol {
		err = c.sanitizeAndSendSimpleQuery(sql, args...)
		if err != nil {
			rows.fatal(err)
			return rows, err
		}

		return rows, nil
	}

	if options != nil && len(options.ParameterOIDs) > 0 {

		buf, err := c.buildOneRoundTripQueryEx(c.wbuf, sql, options, args)
		if err != nil {
			rows.fatal(err)
			return rows, err
		}

		buf = appendSync(buf)

		n, err := c.conn.Write(buf)
		if err != nil && fatalWriteErr(n, err) {
			rows.fatal(err)
			c.die(err)
			return nil, err
		}
		c.pendingReadyForQueryCount++

		fieldDescriptions, err := c.readUntilRowDescription()
		if err != nil {
			rows.fatal(err)
			return nil, err
		}

		if len(options.ResultFormatCodes) == 0 {
			for i := range fieldDescriptions {
				fieldDescriptions[i].FormatCode = TextFormatCode
			}
		} else if len(options.ResultFormatCodes) == 1 {
			fc := options.ResultFormatCodes[0]
			for i := range fieldDescriptions {
				fieldDescriptions[i].FormatCode = fc
			}
		} else {
			for i := range options.ResultFormatCodes {
				fieldDescriptions[i].FormatCode = options.ResultFormatCodes[i]
			}
		}

		rows.sql = sql
		rows.fields = fieldDescriptions
		return rows, nil
	}

	ps, ok := c.preparedStatements[sql]
	if !ok {
		if st, ok := c.config.LazyPreparedStatements[sql]; ok {
			ps, err = c.prepareEx(sql, st, nil)
			if err != nil {
				rows.fatal(err)
				return rows, rows.err
			}
		} else {
			var err error
			ps, err = c.prepareEx("", sql, nil)
			if err != nil {
				rows.fatal(err)
				return rows, rows.err
			}
		}
	}
	rows.sql = sql
	rows.fields = ps.FieldDescriptions

	err = c.sendPreparedQuery(ps, args...)
	if err != nil {
		rows.fatal(err)
	}

	return rows, rows.err
}

func (c *Conn) buildOneRoundTripQueryEx(buf []byte, sql string, options *QueryExOptions, arguments []interface{}) ([]byte, error) {
	if len(arguments) != len(options.ParameterOIDs) {
		return nil, errors.Errorf("mismatched number of arguments (%d) and options.ParameterOIDs (%d)", len(arguments), len(options.ParameterOIDs))
	}

	if len(options.ParameterOIDs) > 65535 {
		return nil, errors.Errorf("Number of QueryExOptions ParameterOIDs must be between 0 and 65535, received %d", len(options.ParameterOIDs))
	}

	buf = appendParse(buf, "", sql, options.ParameterOIDs)
	buf = appendDescribe(buf, 'S', "")
	buf, err := appendBind(buf, "", "", c.ConnInfo, options.ParameterOIDs, arguments, options.ResultFormatCodes)
	if err != nil {
		return nil, err
	}
	buf = appendExecute(buf, "", 0)

	return buf, nil
}

func (c *Conn) readUntilRowDescription() ([]FieldDescription, error) {
	for {
		msg, err := c.rxMsg()
		if err != nil {
			return nil, err
		}

		switch msg := msg.(type) {
		case *pgproto3.ParameterDescription:
		case *pgproto3.RowDescription:
			fieldDescriptions := c.rxRowDescription(msg)
			for i := range fieldDescriptions {
				if dt, ok := c.ConnInfo.DataTypeForOID(fieldDescriptions[i].DataType); ok {
					fieldDescriptions[i].DataTypeName = dt.Name
				} else {
					return nil, errors.Errorf("unknown oid: %d", fieldDescriptions[i].DataType)
				}
			}
			return fieldDescriptions, nil
		default:
			if err := c.processContextFreeMsg(msg); err != nil {
				return nil, err
			}
		}
	}
}

func (c *Conn) sanitizeAndSendSimpleQuery(sql string, args ...interface{}) (err error) {
	if c.RuntimeParams["standard_conforming_strings"] != "on" {
		return errors.New("simple protocol queries must be run with standard_conforming_strings=on")
	}

	if c.RuntimeParams["client_encoding"] != "UTF8" {
		return errors.New("simple protocol queries must be run with client_encoding=UTF8")
	}

	valueArgs := make([]interface{}, len(args))
	for i, a := range args {
		valueArgs[i], err = convertSimpleArgument(c.ConnInfo, a)
		if err != nil {
			return err
		}
	}

	sql, err = sanitize.SanitizeSQL(sql, valueArgs...)
	if err != nil {
		return err
	}

	return c.sendSimpleQuery(sql)
}

func (c *Conn) QueryRowEx(ctx context.Context, sql string, options *QueryExOptions, args ...interface{}) *Row {
	rows, _ := c.QueryEx(ctx, 1, sql, options, args...)
	return (*Row)(rows)
}
