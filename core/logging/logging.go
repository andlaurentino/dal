// Package logging provides a single slog.Logger construction shared by all
// DAL services so log output is consistently structured (JSON, service name
// tag) regardless of which binary is running.
package logging

import (
	"log/slog"
	"os"
)

// New returns a JSON structured logger tagged with the given service name.
func New(service string) *slog.Logger {
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})
	return slog.New(handler).With("service", service)
}
