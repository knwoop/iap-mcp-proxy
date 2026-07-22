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

	"github.com/google/go-cmp/cmp"
)

// fakeIAP simulates IAP in front of a minimal Streamable HTTP MCP
// server: requests without a Proxy-Authorization bearer are redirected
// to Google sign-in; valid ones reach the MCP handler.
type fakeIAP struct {
	mu         sync.Mutex
	deleted    []string
	useSSE     bool
	getHandler http.HandlerFunc // standalone GET stream; nil → 405
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
		case http.MethodGet:
			if f.getHandler != nil {
				f.getHandler(w, r)
				return
			}
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	out := &syncBuffer{}
	b := &Bridge{
		Upstream: srv.URL + "/mcp",
		Client: &http.Client{
			Transport: authStub{},
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		Timeout:        5 * time.Second,
		ReconnectDelay: 5 * time.Millisecond,
		Stdin:          strings.NewReader(strings.Join(stdinLines, "\n") + "\n"),
		Stdout:         out,
	}
	if err := b.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	shCtx, shCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer shCancel()
	b.Shutdown(shCtx)
	cancel()

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

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
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
	wantList := map[string]any{
		"jsonrpc": "2.0",
		"id":      float64(2),
		"result":  map[string]any{"echoedMethod": "tools/list"},
	}
	if diff := cmp.Diff(wantList, byID[2]); diff != "" {
		t.Errorf("tools/list response mismatch (-want +got):\n%s", diff)
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if diff := cmp.Diff([]string{"sess-123"}, f.deleted); diff != "" {
		t.Errorf("session DELETE mismatch (-want +got):\n%s", diff)
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
	wantCall := map[string]any{
		"jsonrpc": "2.0",
		"id":      float64(2),
		"result":  map[string]any{"echoedMethod": "tools/call"},
	}
	var sawCall bool
	for _, m := range out {
		if cmp.Equal(wantCall, m) {
			sawCall = true
		}
	}
	if !sawCall {
		t.Errorf("SSE tools/call response not relayed: %v", out)
	}
}

func TestBridgeStandaloneGETStream(t *testing.T) {
	f := &fakeIAP{}
	var (
		streamMu sync.Mutex
		lastIDs  []string
		sessions []string
	)
	f.getHandler = func(w http.ResponseWriter, r *http.Request) {
		streamMu.Lock()
		lastIDs = append(lastIDs, r.Header.Get("Last-Event-ID"))
		sessions = append(sessions, r.Header.Get("Mcp-Session-Id"))
		n := len(lastIDs)
		streamMu.Unlock()
		if n == 1 {
			// First connection: deliver one server-initiated
			// notification with an event ID, then drop the stream.
			w.Header().Set("Content-Type", "text/event-stream")
			fmt.Fprint(w, "id: 5\ndata: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/tools/list_changed\"}\n\n")
			return
		}
		// Reconnections: signal that no stream is offered anymore.
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
	srv := httptest.NewServer(f.handler())
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	out := &syncBuffer{}
	b := &Bridge{
		Upstream: srv.URL + "/mcp",
		Client: &http.Client{
			Transport: authStub{},
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		Timeout:        5 * time.Second,
		ReconnectDelay: 5 * time.Millisecond,
		Stdin:          strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}` + "\n"),
		Stdout:         out,
	}
	if err := b.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Run returns at stdin EOF while the listener keeps working; wait
	// for the notification to be relayed and the reconnect to happen.
	deadline := time.Now().Add(2 * time.Second)
	for {
		streamMu.Lock()
		reconnected := len(lastIDs) >= 2
		streamMu.Unlock()
		if reconnected && strings.Contains(out.String(), "notifications/tools/list_changed") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out; GET connects: %v, stdout: %q", lastIDs, out.String())
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()

	streamMu.Lock()
	defer streamMu.Unlock()
	if lastIDs[0] != "" {
		t.Errorf("first GET carried Last-Event-ID %q, want empty", lastIDs[0])
	}
	if lastIDs[1] != "5" {
		t.Errorf("reconnect Last-Event-ID = %q, want \"5\"", lastIDs[1])
	}
	for i, sid := range sessions {
		if sid != "sess-123" {
			t.Errorf("GET %d carried session %q, want sess-123", i, sid)
		}
	}
}

// restartingServer simulates an upstream that loses its sessions
// mid-conversation (a Cloud Run redeploy): each session is valid for
// exactly one non-handshake request, after which it returns 404 until
// the client re-initializes.
type restartingServer struct {
	mu           sync.Mutex
	gen          int
	validSession string
	initCount    int
	notifyCount  int
}

func (s *restartingServer) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
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

		s.mu.Lock()
		defer s.mu.Unlock()
		switch msg.Method {
		case "initialize":
			s.gen++
			s.initCount++
			s.validSession = fmt.Sprintf("sess-%d", s.gen)
			w.Header().Set("Mcp-Session-Id", s.validSession)
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":{"protocolVersion":"2025-06-18","capabilities":{},"serverInfo":{"name":"flaky","version":"1"}}}`, msg.ID)
		case "notifications/initialized":
			s.notifyCount++
			w.WriteHeader(http.StatusAccepted)
		default:
			if r.Header.Get("Mcp-Session-Id") != s.validSession {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			// One request per session, then the "redeploy" hits.
			s.validSession = ""
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":{"echoedMethod":%q}}`, msg.ID, msg.Method)
		}
	})
}

func TestBridgeRecoversFromSessionExpiry(t *testing.T) {
	s := &restartingServer{}
	srv := httptest.NewServer(s.handler())
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	out := &syncBuffer{}
	b := &Bridge{
		Upstream: srv.URL + "/mcp",
		Client:   &http.Client{Transport: authStub{}},
		Timeout:  5 * time.Second,
		Stdin: strings.NewReader(strings.Join([]string{
			`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`,
			`{"jsonrpc":"2.0","method":"notifications/initialized"}`,
			`{"jsonrpc":"2.0","id":2,"method":"tools/list"}`,
			`{"jsonrpc":"2.0","id":3,"method":"tools/call"}`,
		}, "\n") + "\n"),
		Stdout: out,
	}
	if err := b.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	cancel()

	got := map[float64]map[string]any{}
	for line := range strings.SplitSeq(strings.TrimSpace(out.String()), "\n") {
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("stdout line is not JSON: %q", line)
		}
		got[m["id"].(float64)] = m
	}

	// The client must see exactly its three responses — the replayed
	// initialize is internal and must never surface.
	if len(got) != 3 {
		t.Fatalf("want 3 responses on stdout, got %d: %v", len(got), got)
	}
	for _, id := range []float64{2, 3} {
		if errObj, ok := got[id]["error"]; ok {
			t.Errorf("request %v failed instead of recovering: %v", id, errObj)
		}
		if _, ok := got[id]["result"]; !ok {
			t.Errorf("request %v missing result: %v", id, got[id])
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	// tools/list kills session 1, so tools/call must have triggered
	// exactly one transparent re-initialize (handshake replayed too).
	if s.initCount != 2 {
		t.Errorf("initialize count = %d, want 2 (original + recovery)", s.initCount)
	}
	if s.notifyCount != 2 {
		t.Errorf("initialized notification count = %d, want 2 (original + recovery)", s.notifyCount)
	}
}

func TestBridgeSurfacesUpstreamFailureAsJSONRPCError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Goog-Iap-Generated-Response", "true")
		http.Error(w, "Invalid IAP credentials", http.StatusUnauthorized)
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	out := &syncBuffer{}
	b := &Bridge{
		Upstream: srv.URL,
		Client:   &http.Client{Transport: authStub{}},
		Timeout:  2 * time.Second,
		Stdin:    strings.NewReader(`{"jsonrpc":"2.0","id":7,"method":"initialize"}` + "\n"),
		Stdout:   out,
	}
	if err := b.Run(ctx); err != nil {
		t.Fatal(err)
	}
	cancel()

	var m map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(out.String())), &m); err != nil {
		t.Fatalf("stdout not JSON: %q", out.String())
	}
	if m["id"].(float64) != 7 {
		t.Errorf("error response id = %v, want 7", m["id"])
	}
	if _, ok := m["error"].(map[string]any); !ok {
		t.Errorf("want JSON-RPC error object, got %v", m)
	}
}
