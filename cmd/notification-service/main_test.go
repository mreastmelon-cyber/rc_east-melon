package main

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
)

func TestInvalidStartupOptions(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, args := range [][]string{{"-mode", "unknown"}, {"-workers", "33"}, {"extra"}} {
		if e := run(context.Background(), args, logger); e == nil {
			t.Error("invalid options accepted")
		}
	}
	t.Setenv("MYSQL_DSN", "")
	if e := run(context.Background(), nil, logger); e == nil || !strings.Contains(e.Error(), "MYSQL_DSN") {
		t.Fatal("missing database not rejected", e)
	}
}
