package pgproto3

import (
	"encoding/binary"
	"github.com/mysilkway/rbuf"
	"io"
	"syscall"

	"github.com/pkg/errors"
)

type Frontend struct {
	rawConn syscall.RawConn // used RawConn to make Read function to be non-blocking
	rb      *rbuf.FixedSizeRingBuf
	w       io.Writer

	// Backend message flyweights
	authentication       Authentication
	backendKeyData       BackendKeyData
	bindComplete         BindComplete
	closeComplete        CloseComplete
	commandComplete      CommandComplete
	copyBothResponse     CopyBothResponse
	copyData             CopyData
	copyInResponse       CopyInResponse
	copyOutResponse      CopyOutResponse
	dataRow              DataRow
	emptyQueryResponse   EmptyQueryResponse
	errorResponse        ErrorResponse
	functionCallResponse FunctionCallResponse
	noData               NoData
	noticeResponse       NoticeResponse
	notificationResponse NotificationResponse
	parameterDescription ParameterDescription
	parameterStatus      ParameterStatus
	parseComplete        ParseComplete
	readyForQuery        ReadyForQuery
	rowDescription       RowDescription
}

func NewFrontend(r io.Reader, w io.Writer, rawConn syscall.RawConn) (*Frontend, error) {
	// By historical reasons Postgres currently has 8KB send buffer inside,
	// so here we want to have at least the same size buffer.
	// @see https://github.com/postgres/postgres/blob/249d64999615802752940e017ee5166e726bc7cd/src/backend/libpq/pqcomm.c#L134
	// @see https://www.postgresql.org/message-id/0cdc5485-cb3c-5e16-4a46-e3b2f7a41322%40ya.ru
	rb := rbuf.NewFixedSizeRingBuf(8192)
	return &Frontend{rb: rb, w: w, rawConn: rawConn}, nil
}

func (b *Frontend) Send(msg FrontendMessage) error {
	_, err := b.w.Write(msg.Encode(nil))
	return err
}

// function to batch receive backend message
// given array(msgs, msgBodies) must be already allocated
// non-decoded BackendMessage will be assigned to msgs, and message body ([][]byte) will be assigned to msgBodies
// n is the maximum row count to get data row
func (b *Frontend) Receive(n int, msgs []BackendMessage, msgBodies [][]byte) (int, error) {
	if n < 0 || n > len(msgs) || n > len(msgBodies) {
		return 0, errors.Errorf("invalid array length, n: [%d], msg: [%d], msgBodies: [%d]", n, len(msgs), len(msgBodies))
	}

	idx := 0 // the current index of messages(msgs)
	rowCounts := 0

	header := make([]byte, 5) // current processing header
	headerWp := 0             // the write position of header

	var msgBody []byte // current processing message body
	msgWp := 0         // the write position of message body

	for {
		// read all data from RawConn
		if b.rb.Avail() <= 0 {
			for {
				// non-blocking read to get data
				rn, err := b.rb.ReadFromRawConn(b.rawConn, b.rb.Avail() <= 0)
				if err != nil {
					return 0, err
				}

				// no more data in reader or no space for buffer, break to decode data
				if rn <= 0 || b.rb.Avail() == b.rb.N {
					break
				}
			}
		}

	nextMsg:
		if headerWp < 5 {
			rn, err := b.rb.Read(header[headerWp:])
			if err != nil {
				return 0, err
			}
			headerWp += rn

			if headerWp < 5 {
				continue
			}

			// complete read header
			bodyLen := int(binary.BigEndian.Uint32(header[1:])) - 4
			if bodyLen > 0 {
				msgBody = make([]byte, bodyLen)
			} else {
				msgBody = nil
			}
		}

		if msgBody != nil && b.rb.Avail() > 0 {
			rn, err := b.rb.Read(msgBody[msgWp:])
			if err != nil {
				return 0, err
			}
			msgWp += rn
		}

		if msgWp == len(msgBody) {
			done := false
			var msg BackendMessage
			switch header[0] {
			case '1':
				msg = &b.parseComplete
			case '2':
				msg = &b.bindComplete
			case '3':
				msg = &b.closeComplete
			case 'A':
				msg = &b.notificationResponse
			case 'C':
				msg = &b.commandComplete
				done = true
			case 'd':
				msg = &b.copyData
			case 'D':
				msg = &b.dataRow
				rowCounts++
			case 'E':
				msg = &b.errorResponse
				done = true
			case 'G':
				msg = &b.copyInResponse
			case 'H':
				msg = &b.copyOutResponse
			case 'I':
				msg = &b.emptyQueryResponse
			case 'K':
				msg = &b.backendKeyData
			case 'n':
				msg = &b.noData
			case 'N':
				msg = &b.noticeResponse
			case 'R':
				msg = &b.authentication
			case 'S':
				msg = &b.parameterStatus
			case 't':
				msg = &b.parameterDescription
			case 'T':
				msg = &b.rowDescription
			case 'V':
				msg = &b.functionCallResponse
			case 'W':
				msg = &b.copyBothResponse
			case 'Z':
				msg = &b.readyForQuery
			default:
				return 0, errors.Errorf("unknown message type: %c", header[0])
			}

			msgs[idx] = msg
			msgBodies[idx] = msgBody
			idx++

			if done || idx >= len(msgs) || (n > 0 && rowCounts >= n) {
				return idx, nil
			}

			headerWp = 0
			msgBody = nil
			msgWp = 0

			if b.rb.Avail() > 0 {
				goto nextMsg
			}
		}
	}
}
