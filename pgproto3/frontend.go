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

	backendMsgFlyweights [256]BackendMessage
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
	b := &Frontend{rb: rb, w: w, rawConn: rawConn}

	b.backendMsgFlyweights[uint8('1')] = &ParseComplete{}
	b.backendMsgFlyweights[uint8('2')] = &BindComplete{}
	b.backendMsgFlyweights[uint8('3')] = &CloseComplete{}
	b.backendMsgFlyweights[uint8('A')] = &NotificationResponse{}
	b.backendMsgFlyweights[uint8('C')] = &CommandComplete{}
	b.backendMsgFlyweights[uint8('d')] = &CopyData{}
	b.backendMsgFlyweights[uint8('D')] = &DataRow{}
	b.backendMsgFlyweights[uint8('E')] = &ErrorResponse{}
	b.backendMsgFlyweights[uint8('G')] = &CopyInResponse{}
	b.backendMsgFlyweights[uint8('H')] = &CopyOutResponse{}
	b.backendMsgFlyweights[uint8('I')] = &EmptyQueryResponse{}
	b.backendMsgFlyweights[uint8('K')] = &BackendKeyData{}
	b.backendMsgFlyweights[uint8('n')] = &NoData{}
	b.backendMsgFlyweights[uint8('N')] = &NoticeResponse{}
	b.backendMsgFlyweights[uint8('R')] = &Authentication{}
	b.backendMsgFlyweights[uint8('S')] = &ParameterStatus{}
	b.backendMsgFlyweights[uint8('t')] = &ParameterDescription{}
	b.backendMsgFlyweights[uint8('T')] = &RowDescription{}
	b.backendMsgFlyweights[uint8('V')] = &FunctionCallResponse{}
	b.backendMsgFlyweights[uint8('W')] = &CopyBothResponse{}
	b.backendMsgFlyweights[uint8('Z')] = &ReadyForQuery{}

	return b, nil
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
	var header [5]byte       // the header array to get message type and body length
	headerSlice := header[:] // the header slice to read

	var msgBody []byte      // current processing message body
	var msgBodySlice []byte // the message body slice to read

	// loop until at least 1 message and no data
	for rmsgs.Readable() <= 0 {
		_, err := b.rb.ReadFromRawConn(b.rawConn)
		if err != nil {
			return err
		}

		// decode the message header and write message and its body to ReceivedMessages
		for b.rb.Avail() > 0 {
			// header
			rn, err := b.rb.Read(headerSlice)
			if err != nil {
				return err
			}
			headerSlice = headerSlice[rn:]

			if len(headerSlice) > 0 {
				break
			}

			bodyLen := int(binary.BigEndian.Uint32(header[1:])) - 4
			if bodyLen > 0 {
				msgBody = make([]byte, bodyLen)
				msgBodySlice = msgBody

				for len(msgBodySlice) > 0 {
					_, err = b.rb.ReadFromRawConn(b.rawConn)
					if err != nil {
						return err
					}

					if b.rb.Avail() > 0 {
						rn, err = b.rb.Read(msgBodySlice)
						if err != nil {
							return err
						}
						msgBodySlice = msgBodySlice[rn:]
					}
				}
			}

			msg := b.backendMsgFlyweights[header[0]]
			if msg == nil {
				return errors.Errorf("unknown message type: %c", header[0])
			}

			if err := rmsgs.Write(msg, msgBody); err != nil {
				return err
			}

			// buffer is full, no need to get and decode more message
			if rmsgs.WriteCapacity() <= 0 {
				return nil
			}

			headerSlice = header[:]
			msgBody = nil
		}
	}

	return nil
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
	if r.rp == r.Len()-1 {
		r.rp = 0
	} else {
		r.rp++
	}
	r.readable--
	return r.msgs[rp], r.msgBodies[rp], nil
}

// function to BackendMessage and its body by index (k)
// the start point is (r.rp + k) % r.Len()
// this function will not forward moved message forward
func (r *ReceivedMessages) Kth(k int) (BackendMessage, []byte) {
	rp := (r.rp + k) % r.Len()
	return r.msgs[rp], r.msgBodies[rp]
}

// function to store BackendMessage and its body
// this function would not overwrite the messages which are not yet read
func (r *ReceivedMessages) Write(msg BackendMessage, msgBody []byte) error {
	if r.WriteCapacity() <= 0 {
		return io.ErrShortWrite
	}

	r.msgs[r.wp] = msg
	r.msgBodies[r.wp] = msgBody
	if r.wp == r.Len()-1 {
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

func (r ReceivedMessages) Len() int {
	return len(r.msgs)
}

// moved 1 read message backward, it will not backward if all messages are not yet read
func (r *ReceivedMessages) Backward() {
	if r.WriteCapacity() <= 0 {
		return
	}
	r.readable += 1
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
	if r.rp == len(r.msgs)-1 {
		r.rp = 0
	} else {
		r.rp++
	}
	r.readable++
}

func (r *ReceivedMessages) SetCapacity(capacity int) {
	if r.Len() == capacity {
		return
	}

	nmsgs := make([]BackendMessage, capacity)
	nmsgBodies := make([][]byte, capacity)

	if r.Readable() > capacity {
		r.rp = (r.rp + r.readable - capacity) % r.Len()
		r.readable = capacity
	}

	copy(nmsgs, r.msgs[r.rp:])
	copy(nmsgBodies, r.msgBodies[r.rp:])
	copy(nmsgs[r.rp:], r.msgs[0:r.wp])
	copy(nmsgBodies[r.rp:], r.msgBodies[0:r.wp])

	r.rp = 0
	r.wp = r.readable
	return
}
