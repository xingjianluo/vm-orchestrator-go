package orchestrator

import (
	"log/slog"
	"os"
)

// slogLogger 把 slog.Logger 适配为 Logger。
type slogLogger struct{ l *slog.Logger }

func (s *slogLogger) Info(msg string, args ...any)  { s.l.Info(msg, args...) }
func (s *slogLogger) Warn(msg string, args ...any)  { s.l.Warn(msg, args...) }
func (s *slogLogger) Error(msg string, args ...any) { s.l.Error(msg, args...) }

// NewSlogLogger 创建输出到 stdout 的 slog 文本日志器。
func NewSlogLogger(level slog.Level) Logger {
	h := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	return &slogLogger{l: slog.New(h)}
}

// nopLogger 是构造参数缺省时的空日志器。
type nopLogger struct{}

func (nopLogger) Info(string, ...any)  {}
func (nopLogger) Warn(string, ...any)  {}
func (nopLogger) Error(string, ...any) {}
