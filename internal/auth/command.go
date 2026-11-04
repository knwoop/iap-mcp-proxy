package auth

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
)

// maxCommandOutput bounds how much of a command's stdout is read. A JWT
// is a few KiB; anything larger is not a token.
const maxCommandOutput = 64 << 10

// commandSource runs a local command that prints a bearer JWT on stdout,
// such as `gcloud auth print-identity-token`. Wrapped in Cached, it is
// re-run shortly before the token's exp, so a short-lived credential
// can be forwarded for the life of the proxy.
type commandSource struct {
	argv []string
}

// NewCommandSource returns a Source that runs argv (no shell) and uses
// its trimmed stdout as the token.
func NewCommandSource(argv []string) (Source, error) {
	if len(argv) == 0 {
		return nil, errors.New("empty command")
	}
	return &commandSource{argv: argv}, nil
}

func (s *commandSource) Token(ctx context.Context) (string, error) {
	var stdout limitedBuffer
	var stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, s.argv[0], s.argv[1:]...)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("running %q: %w: %s", s.argv[0], err, firstLine(stderr.String()))
	}
	if stdout.overflow {
		return "", fmt.Errorf("%q printed more than %d bytes; want a single token", s.argv[0], maxCommandOutput)
	}
	tok := strings.TrimSpace(stdout.String())
	if tok == "" {
		return "", fmt.Errorf("%q printed no token", s.argv[0])
	}
	return tok, nil
}

// limitedBuffer keeps the first maxCommandOutput bytes and records
// whether more were written, so a runaway command cannot exhaust memory.
// The buffer is a named field, not embedded, so io.Copy cannot bypass
// Write through bytes.Buffer.ReadFrom.
type limitedBuffer struct {
	buf      bytes.Buffer
	overflow bool
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	room := maxCommandOutput - b.buf.Len()
	if len(p) > room {
		b.overflow = true
		if room > 0 {
			b.buf.Write(p[:room])
		}
		return len(p), nil
	}
	return b.buf.Write(p)
}

func (b *limitedBuffer) String() string { return b.buf.String() }

func firstLine(s string) string {
	line, _, _ := strings.Cut(strings.TrimSpace(s), "\n")
	return line
}
