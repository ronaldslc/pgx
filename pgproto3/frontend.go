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

// ring buffer structure to store non-decoded backend messages and its body
type ReceivedMessages struct {
	msgs      []BackendMessage
	msgBodies [][]byte
	rp        int // read position
	wp        int // write position
	readable  int // how many message can be read
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
// returning int is the count of received messages
func (b *Frontend) Receive(rmsgs *ReceivedMessages) error {
	rowCounts := 0

	header := make([]byte, 5) // current processing header
	headerWp := 0             // the write position of header

	var msgBody []byte // current processing message body
	msgWp := 0         // the write position of message body

	for {
		if b.rb.Avail() <= 0 {
			// read data from RawConn
			_, err := b.rb.ReadFromRawConn(b.rawConn, true)
			if err != nil {
				return err
			}
		}

		// decode message header and split message bodies
		for {
			if headerWp < 5 {
				rn, err := b.rb.Read(header[headerWp:])
				if err != nil {
					return err
				}
				headerWp += rn

				if headerWp < 5 {
					break
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
					return err
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
					return errors.Errorf("unknown message type: %c", header[0])
				}

				if err := rmsgs.Write(msg, msgBody); err != nil {
					return err
				}

				if done || rmsgs.WriteCapacity() <= 0 {
					return nil
				}

				headerWp = 0
				msgWp = 0
			}

			if b.rb.Avail() <= 0 {
				break
			}
		}
	}
}

func NewReceivedMessages(n int) *ReceivedMessages {
	return &ReceivedMessages{msgs: make([]BackendMessage, n), msgBodies: make([][]byte, n)}
}

// function to get BackendMessage and its body
func (r *ReceivedMessages) Read() (BackendMessage, []byte, error) {
	if r.Readable() <= 0 {
		return nil, nil, errors.New("no message")
	}

	rp := r.rp
	if r.rp == len(r.msgs)-1 {
		r.rp = 0
	} else {
		r.rp++
	}
	r.readable--
	return r.msgs[rp], r.msgBodies[rp], nil
}

// function to store BackendMessage and its body
// this function would not overwrite the messages which are not yet read
func (r *ReceivedMessages) Write(msg BackendMessage, msgBody []byte) error {
	if r.WriteCapacity() <= 0 {
		return io.ErrShortWrite
	}

	r.msgs[r.wp] = msg
	r.msgBodies[r.wp] = msgBody
	if r.wp == len(r.msgs)-1 {
		r.wp = 0
	} else {
		r.wp++
	}
	r.readable++

	return nil
}

// return the count of message that is not yet read
func (r ReceivedMessages) Readable() int {
	return r.readable
}

// return the count of space that allow to be written
func (r ReceivedMessages) WriteCapacity() int {
	return len(r.msgs) - r.Readable()
}

// moved 1 read message backward, it will not backward if all messages are not yet read
func (r *ReceivedMessages) Backward() {
	if r.WriteCapacity() <= 0 {
		return
	}
	r.readable += 1
	if r.wp == 0 {
		r.wp = len(r.msgs) - 1
	} else {
		r.wp -= 1
	}
	if r.rp == 0 {
		r.rp = len(r.msgs) - 1
	} else {
		r.rp -= 1
	}
}

// skip 1 message which is not yet read, it will not forward if no readable messages
func (r *ReceivedMessages) Forward() {
	if r.Readable() <= 0 {
		return
	}
	if r.wp == len(r.msgs)-1 {
		r.wp = 0
	} else {
		r.wp++
	}
	if r.rp == len(r.msgs)-1 {
		r.rp = 0
	} else {
		r.rp++
	}
	r.readable++
}
