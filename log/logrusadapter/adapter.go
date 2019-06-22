// Package logrusadapter provides a logger that writes to a github.com/sirupsen/logrus.Logger
// log.
package logrusadapter

import (
	"github.com/ronaldslc/pgx"
	"github.com/sirupsen/logrus"
)

type Logger struct {
	l *logrus.Logger
}

func NewLogger(l *logrus.Logger) *Logger {
	return &Logger{l: l}
}

func (l *Logger) Log(level pgx.LogLevel, msg string, ld pgx.LogData) {
	var logger logrus.FieldLogger
	if ld != nil {
		f := make(logrus.Fields)
		for _, v := range ld {
			f[v.Key] = v.Value
		}
		logger = l.l.WithFields(f)
	} else {
		logger = l.l
	}

	switch level {
	case pgx.LogLevelTrace:
		logger.WithField("PGX_LOG_LEVEL", level).Debug(msg)
	case pgx.LogLevelDebug:
		logger.Debug(msg)
	case pgx.LogLevelInfo:
		logger.Info(msg)
	case pgx.LogLevelWarn:
		logger.Warn(msg)
	case pgx.LogLevelError:
		logger.Error(msg)
	default:
		logger.WithField("INVALID_PGX_LOG_LEVEL", level).Error(msg)
	}
}
