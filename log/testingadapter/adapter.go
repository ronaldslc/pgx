// Package testingadapter provides a logger that writes to a test or benchmark
// log.
package testingadapter

import (
	"fmt"

	"github.com/ronaldslc/pgx"
)

// TestingLogger interface defines the subset of testing.TB methods used by this
// adapter.
type TestingLogger interface {
	Log(args ...interface{})
}

type Logger struct {
	l TestingLogger
}

func NewLogger(l TestingLogger) *Logger {
	return &Logger{l: l}
}

func (l *Logger) Log(level pgx.LogLevel, msg string, ld pgx.LogData) {
	logArgs := make([]interface{}, 0, 2+len(ld))
	logArgs = append(logArgs, level, msg)
	for _, v := range ld {
		logArgs = append(logArgs, fmt.Sprintf("%s=%v", v.Key, v.Value))
	}
	l.l.Log(logArgs...)
}
