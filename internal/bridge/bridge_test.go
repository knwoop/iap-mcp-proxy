package bridge

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeIAP simulates IAP in front of a minimal Streamable HTTP MCP
// server: requests without a Proxy-Authorization bearer are redirected
// to Google sign-in; valid ones reach the MCP handler.
type fakeIAP struct {
	mu      sync.Mutex
	deleted []string
	useSSE  bool
}

func (f *fakeIAP) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.Header.Get("Proxy-Authorization"), "Bearer ") {
			w.Header().Set("X-Goog-Iap-Generated-Response", "true")
			w.Header().Set("Location", "https://accounts.google.com/o/oauth2/v2/auth")
			w.WriteHeader(http.StatusFound)
			return
		}

		switch r.Method {
		case http.MethodDelete:
			f.mu.Lock()
			f.deleted = append(f.deleted, r.Header.Get("Mcp-Session-Id"))
			f.mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
			return
		case http.MethodPost:
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		body, _ := io.ReadAll(r.Body)
		var msg struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		if err := json.Unmarshal(body, &msg); err != nil {
			http.Error(w, "bad json", http.StatusBadRequest)
			return
		}

		// Notifications are accepted with 202.
		if len(msg.ID) == 0 {
			w.WriteHeader(http.StatusAccepted)
			return
		}

		var result string
		switch msg.Method {
		case "initialize":
			w.Header().Set("Mcp-Session-Id", "sess-123")
			result = `{"protocolVersion":"2025-06-18","capabilities":{},"serverInfo":{"name":"echo","version":"1"}}`
		default:
			if r.Header.Get("Mcp-Session-Id") != "sess-123" {
				http.Error(w, "missing session", http.StatusNotFound)
				return
			}
			result = fmt.Sprintf(`{"echoedMethod":%q}`, msg.Method)
		}
		payload := fmt.Sprintf(`{"jsonrpc":"2.0","id":%s,"result":%s}`, msg.ID, result)

		if f.useSSE && msg.Method != "initialize" {
			w.Header().Set("Content-Type", "text/event-stream")
			fmt.Fprintf(w, "event: message\ndata: %s\n\n", payload)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, payload)
	})
}

func runBridge(t *testing.T, f *fakeIAP, stdinLines ...string) (stdout []map[string]any, iap *fakeIAP) {
	t.Helper()
	srv := httptest.NewServer(f.handler())
	defer srv.Close()

	var out bytes.Buffer
	b := &Bridge{
		Upstream: srv.URL + "/mcp",
		Client: &http.Client{
			Transport: authStub{},
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		Timeout: 5 * time.Second,
		Stdin:   strings.NewReader(strings.Join(stdinLines, "\n") + "\n"),
		Stdout:  &syncWriter{w: &out},
	}
	if err := b.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	b.Shutdown(ctx)

	for line := range strings.SplitSeq(strings.TrimSpace(out.String()), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("stdout line is not JSON: %q", line)
		}
		stdout = append(stdout, m)
	}
	return stdout, f
}

// authStub attaches a fixed bearer so the fake IAP lets requests through.
type authStub struct{}

func (authStub) RoundTrip(r *http.Request) (*http.Response, error) {
	r.Header.Set("Proxy-Authorization", "Bearer stub-token")
	return http.DefaultTransport.RoundTrip(r)
}

type syncWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (s *syncWriter) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.w.Write(p)
}

func TestBridgeRoundTripJSON(t *testing.T) {
	out, f := runBridge(t, &fakeIAP{},
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`,
		`{"jsonrpc":"2.0","method":"notifications/initialized"}`,
		`{"jsonrpc":"2.0","id":2,"method":"tools/list"}`,
	)

	if len(out) != 2 {
		t.Fatalf("want 2 responses (notification has none), got %d: %v", len(out), out)
	}
	byID := map[float64]map[string]any{}
	for _, m := range out {
		byID[m["id"].(float64)] = m
	}
	if _, ok := byID[1]["result"]; !ok {
		t.Errorf("initialize response missing result: %v", byID[1])
	}
	res, _ := byID[2]["result"].(map[string]any)
	if res == nil || res["echoedMethod"] != "tools/list" {
		t.Errorf("tools/list not proxied with session: %v", byID[2])
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.deleted) != 1 || f.deleted[0] != "sess-123" {
		t.Errorf("session not DELETEd on shutdown: %v", f.deleted)
	}
}

func TestBridgeRoundTripSSE(t *testing.T) {
	out, _ := runBridge(t, &fakeIAP{useSSE: true},
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`,
		`{"jsonrpc":"2.0","id":2,"method":"tools/call"}`,
	)
	if len(out) != 2 {
		t.Fatalf("want 2 responses, got %d: %v", len(out), out)
	}
	var sawCall bool
	for _, m := range out {
		if res, ok := m["result"].(map[string]any); ok && res["echoedMethod"] == "tools/call" {
			sawCall = true
		}
	}
	if !sawCall {
		t.Errorf("SSE tools/call response not relayed: %v", out)
	}
}

func TestBridgeSurfacesUpstreamFailureAsJSONRPCError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Goog-Iap-Generated-Response", "true")
		http.Error(w, "Invalid IAP credentials", http.StatusUnauthorized)
	}))
	defer srv.Close()

	var out bytes.Buffer
	b := &Bridge{
		Upstream: srv.URL,
		Client:   &http.Client{Transport: authStub{}},
		Timeout:  2 * time.Second,
		Stdin:    strings.NewReader(`{"jsonrpc":"2.0","id":7,"method":"initialize"}` + "\n"),
		Stdout:   &syncWriter{w: &out},
	}
	if err := b.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	var m map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(out.Bytes()), &m); err != nil {
		t.Fatalf("stdout not JSON: %q", out.String())
	}
	if m["id"].(float64) != 7 {
		t.Errorf("error response id = %v, want 7", m["id"])
	}
	if _, ok := m["error"].(map[string]any); !ok {
		t.Errorf("want JSON-RPC error object, got %v", m)
	}
}
