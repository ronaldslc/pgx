package pgx

import (
	"encoding/hex"
	"fmt"
	uuid "github.com/ronaldslc/go.uuid"

	"github.com/pkg/errors"
)

// The values for log levels are chosen such that the zero value means that no
// log level was specified.
const (
	LogLevelTrace = 6
	LogLevelDebug = 5
	LogLevelInfo  = 4
	LogLevelWarn  = 3
	LogLevelError = 2
	LogLevelNone  = 1
)

// LogLevel represents the pgx logging level. See LogLevel* constants for
// possible values.
type LogLevel int

func (ll LogLevel) String() string {
	switch ll {
	case LogLevelTrace:
		return "trace"
	case LogLevelDebug:
		return "debug"
	case LogLevelInfo:
		return "info"
	case LogLevelWarn:
		return "warn"
	case LogLevelError:
		return "error"
	case LogLevelNone:
		return "none"
	default:
		return fmt.Sprintf("invalid level %d", ll)
	}
}

type LogData []KV

func (l *LogData) Add(key string, data interface{}) {
	*l = append(*l, KV{Key: key, Value: data})
}

type KV struct {
	Key   string
	Value interface{}
}

// Logger is the interface used to get logging from pgx internals.
type Logger interface {
	// Log a message at the given level with data key/value pairs. data may be nil.
	Log(level LogLevel, msg string, ld LogData)
}

// LogLevelFromString converts log level string to constant
//
// Valid levels:
//	trace
//	debug
//	info
//	warn
//	error
//	none
func LogLevelFromString(s string) (LogLevel, error) {
	switch s {
	case "trace":
		return LogLevelTrace, nil
	case "debug":
		return LogLevelDebug, nil
	case "info":
		return LogLevelInfo, nil
	case "warn":
		return LogLevelWarn, nil
	case "error":
		return LogLevelError, nil
	case "none":
		return LogLevelNone, nil
	default:
		return 0, errors.New("invalid log level")
	}
}

func logQueryArgs(args []interface{}) []interface{} {
	logArgs := make([]interface{}, 0, len(args))

	for _, a := range args {
		switch v := a.(type) {
		case []byte:
			if len(v) < 64 {
				a = hex.EncodeToString(v)
			} else {
				a = fmt.Sprintf("%x (truncated %d bytes)", v[:64], len(v)-64)
			}
		case string:
			if len(v) > 64 {
				a = fmt.Sprintf("%s (truncated %d bytes)", v[:64], len(v)-64)
			}
		case *string:
			if len(*v) > 64 {
				a = fmt.Sprintf("%s (truncated %d bytes)", (*v)[:64], len(*v)-64)
			} else {
				a = fmt.Sprintf("%s", *v)
			}
		case [][16]uint8: // array of raw UUID formats
			out := make([]string, 0, len(v))
			for _, va := range v {
				b := uuid.UUID(va)
				out = append(out, b.String())
			}
			a = out
		default:
			if v, ok := a.(fmt.Stringer); ok {
				vstr := v.String()
				if len(v.String()) > 64 {
					a = fmt.Sprintf("%s (truncated %d bytes)", vstr[:64], len(vstr)-64)
				} else {
					a = vstr
				}
			}
		}

		logArgs = append(logArgs, a)
	}

	return logArgs
}
