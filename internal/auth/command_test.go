package auth

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

const (
	helperModeEnv  = "IAP_MCP_COMMAND_HELPER"
	helperTokenEnv = "IAP_MCP_COMMAND_TOKEN"
)

// TestCommandHelper is not a real test: commandSource tests run the test
// binary itself as the command, selecting its behavior through
// helperModeEnv, so no shell or external tool is needed on any platform.
func TestCommandHelper(t *testing.T) {
	switch os.Getenv(helperModeEnv) {
	case "":
		return
	case "print":
		fmt.Println("  header.payload.sig  ")
	case "token":
		fmt.Println(os.Getenv(helperTokenEnv))
	case "empty":
	case "fail":
		fmt.Fprintln(os.Stderr, "not logged in\nsecond line")
		os.Exit(3)
	case "flood":
		fmt.Print(strings.Repeat("x", maxCommandOutput+1))
	}
	os.Exit(0)
}

func helperCommand(t *testing.T, mode string) Source {
	t.Helper()
	t.Setenv(helperModeEnv, mode)
	src, err := NewCommandSource([]string{os.Args[0], "-test.run=^TestCommandHelper$"})
	if err != nil {
		t.Fatal(err)
	}
	return src
}

func TestCommandSourceTrimsOutput(t *testing.T) {
	tok, err := helperCommand(t, "print").Token(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if tok != "header.payload.sig" {
		t.Errorf("token = %q, want header.payload.sig", tok)
	}
}

func TestCommandSourceFailureIncludesStderr(t *testing.T) {
	_, err := helperCommand(t, "fail").Token(context.Background())
	if err == nil || !strings.Contains(err.Error(), "not logged in") || strings.Contains(err.Error(), "second line") {
		t.Errorf("err = %v, want the first stderr line only", err)
	}
}

func TestCommandSourceRejectsEmptyOutput(t *testing.T) {
	if _, err := helperCommand(t, "empty").Token(context.Background()); err == nil {
		t.Error("want an error for a command that prints nothing")
	}
}

func TestCommandSourceRejectsOversizedOutput(t *testing.T) {
	if _, err := helperCommand(t, "flood").Token(context.Background()); err == nil {
		t.Error("want an error for output larger than a token")
	}
}

func TestCommandSourceCachedUntilInvalidated(t *testing.T) {
	first := makeJWT(t, time.Now().Add(time.Hour))
	second := makeJWT(t, time.Now().Add(2*time.Hour))
	t.Setenv(helperTokenEnv, first)
	cached := NewCached(helperCommand(t, "token"), 5*time.Minute)
	ctx := context.Background()

	if tok, err := cached.Token(ctx); err != nil || tok != first {
		t.Fatalf("first Token() = %q, %v; want the first JWT", tok, err)
	}
	t.Setenv(helperTokenEnv, second)
	if tok, err := cached.Token(ctx); err != nil || tok != first {
		t.Fatalf("cached Token() = %q, %v; want the first JWT without re-running", tok, err)
	}
	cached.Invalidate()
	if tok, err := cached.Token(ctx); err != nil || tok != second {
		t.Fatalf("Token() after Invalidate = %q, %v; want the second JWT", tok, err)
	}
}

func TestNewCommandSourceRejectsEmptyArgv(t *testing.T) {
	if _, err := NewCommandSource(nil); err == nil {
		t.Error("want an error for an empty command")
	}
}
