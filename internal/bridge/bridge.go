// Package bridge implements the stdio ⇄ Streamable HTTP bridge: it
// reads newline-delimited JSON-RPC messages from stdin, POSTs them to
// the upstream MCP endpoint, and writes responses (plain JSON or SSE
// streams) back to stdout as single lines.
package bridge

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	sessionHeader  = "Mcp-Session-Id"
	protocolHeader = "MCP-Protocol-Version"
)

// Bridge forwards MCP traffic between a stdio client and a Streamable
// HTTP upstream.
type Bridge struct {
	Upstream string       // full URL of the remote MCP endpoint
	Client   *http.Client // must carry the auth RoundTripper
	Timeout  time.Duration
	Logger   *slog.Logger

	// ReconnectDelay is the initial wait before re-opening the
	// standalone GET stream after it drops. Defaults to 1s and backs
	// off exponentially to 30s.
	ReconnectDelay time.Duration

	Stdin  io.Reader
	Stdout io.Writer

	writeMu sync.Mutex // serializes stdout writes

	stateMu   sync.Mutex
	sessionID string
	protocol  string

	listenOnce sync.Once
}

// Run processes stdin until EOF or ctx cancellation. Each inbound
// message is forwarded concurrently: a long-running tool call must not
// block a client response to a server-initiated request that arrived
// inside another request's SSE stream.
func (b *Bridge) Run(ctx context.Context) error {
	in := bufio.NewReaderSize(b.Stdin, 1<<20)
	var wg sync.WaitGroup
	defer wg.Wait()

	for {
		line, err := in.ReadBytes('\n')
		line = bytes.TrimSpace(line)
		if len(line) > 0 {
			msg := make([]byte, len(line))
			copy(msg, line)
			// initialize is forwarded synchronously so its session ID is
			// captured before any pipelined follow-up message goes out.
			// Everything else runs concurrently: a long tool call must
			// not block a client response to a server-initiated request
			// arriving inside another request's SSE stream.
			if isInitialize(msg) {
				b.forward(ctx, msg)
				// With the session established, open the standalone GET
				// stream for server-initiated messages that arrive
				// outside any request (list_changed notifications,
				// sampling/elicitation requests, logs).
				b.listenOnce.Do(func() { go b.listen(ctx) })
			} else {
				wg.Go(func() { b.forward(ctx, msg) })
			}
		}
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("reading stdin: %w", err)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

func isInitialize(msg []byte) bool {
	if !bytes.Contains(msg, []byte(`"initialize"`)) {
		return false
	}
	var probe struct {
		Method string `json:"method"`
	}
	return json.Unmarshal(msg, &probe) == nil && probe.Method == "initialize"
}

// forward POSTs one JSON-RPC message upstream and relays the response.
func (b *Bridge) forward(ctx context.Context, msg []byte) {
	ctx, cancel := context.WithTimeout(ctx, b.timeout())
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, b.Upstream, bytes.NewReader(msg))
	if err != nil {
		b.replyError(msg, fmt.Sprintf("building upstream request: %v", err))
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	if sid := b.session(); sid != "" {
		req.Header.Set(sessionHeader, sid)
	}
	if pv := b.protocolVersion(); pv != "" {
		req.Header.Set(protocolHeader, pv)
	}

	resp, err := b.Client.Do(req)
	if err != nil {
		b.replyError(msg, fmt.Sprintf("upstream request failed: %v", err))
		return
	}
	defer resp.Body.Close()

	if sid := resp.Header.Get(sessionHeader); sid != "" {
		b.setSession(sid)
	}

	switch {
	case resp.StatusCode == http.StatusAccepted || resp.StatusCode == http.StatusNoContent:
		// Notification/response accepted; nothing to relay.
		return
	case resp.StatusCode >= 300:
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		b.log().Error("upstream error", "status", resp.StatusCode, "body", firstLine(body))
		b.replyError(msg, fmt.Sprintf("upstream returned HTTP %d", resp.StatusCode))
		return
	}

	switch mediaType(resp.Header.Get("Content-Type")) {
	case "application/json":
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			b.replyError(msg, fmt.Sprintf("reading upstream response: %v", err))
			return
		}
		b.emit(body)
	case "text/event-stream":
		err := readEvents(resp.Body, func(ev Event) error {
			b.emit(ev.Data)
			return nil
		})
		if err != nil && ctx.Err() == nil {
			b.log().Warn("SSE stream ended with error", "error", err)
		}
	default:
		b.log().Warn("unexpected upstream content type", "content_type", resp.Header.Get("Content-Type"))
	}
}

// emit writes one JSON-RPC message to stdout as a single line, and
// opportunistically captures the negotiated protocol version from an
// initialize result.
func (b *Bridge) emit(data []byte) {
	data = bytes.TrimSpace(data)
	if len(data) == 0 {
		return
	}
	b.captureProtocolVersion(data)

	b.writeMu.Lock()
	defer b.writeMu.Unlock()
	b.Stdout.Write(data)
	io.WriteString(b.Stdout, "\n")
}

// replyError surfaces a transport-level failure to the client as a
// JSON-RPC error response, if the inbound message was a request (had
// an id). Failures of notifications are only logged.
func (b *Bridge) replyError(inbound []byte, detail string) {
	b.log().Error(detail)
	var probe struct {
		ID json.RawMessage `json:"id"`
	}
	if err := json.Unmarshal(inbound, &probe); err != nil || len(probe.ID) == 0 || string(probe.ID) == "null" {
		return
	}
	out, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      probe.ID,
		"error": map[string]any{
			"code":    -32603,
			"message": "iap-mcp-proxy: " + detail,
		},
	})
	if err != nil {
		return
	}
	b.writeMu.Lock()
	defer b.writeMu.Unlock()
	b.Stdout.Write(out)
	io.WriteString(b.Stdout, "\n")
}

// listen maintains the standalone GET SSE stream (Streamable HTTP
// spec: "Listening for Messages from the Server"), reconnecting with
// exponential backoff and Last-Event-ID resumption until ctx is
// canceled or the server signals the stream is not available.
func (b *Bridge) listen(ctx context.Context) {
	initial := b.ReconnectDelay
	if initial <= 0 {
		initial = time.Second
	}
	const maxDelay = 30 * time.Second

	delay := initial
	var lastEventID string
	for ctx.Err() == nil {
		gotEvents, retry := b.listenStream(ctx, &lastEventID)
		if !retry {
			return
		}
		if gotEvents {
			delay = initial
		}
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return
		}
		delay = min(delay*2, maxDelay)
	}
}

// listenStream opens one GET stream and relays its events until it
// ends. It reports whether any events arrived and whether the listener
// should reconnect.
func (b *Bridge) listenStream(ctx context.Context, lastEventID *string) (gotEvents, retry bool) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, b.Upstream, nil)
	if err != nil {
		b.log().Warn("building GET stream request", "error", err)
		return false, false
	}
	req.Header.Set("Accept", "text/event-stream")
	if sid := b.session(); sid != "" {
		req.Header.Set(sessionHeader, sid)
	}
	if pv := b.protocolVersion(); pv != "" {
		req.Header.Set(protocolHeader, pv)
	}
	if *lastEventID != "" {
		req.Header.Set("Last-Event-ID", *lastEventID)
	}

	resp, err := b.Client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return false, false
		}
		b.log().Debug("GET stream connection failed; will retry", "error", err)
		return false, true
	}
	defer resp.Body.Close()

	switch {
	case resp.StatusCode == http.StatusMethodNotAllowed:
		// Server does not offer a standalone stream; that's fine.
		b.log().Debug("upstream does not offer a standalone GET stream")
		return false, false
	case resp.StatusCode == http.StatusNotFound:
		// Session gone; only a client re-initialize can fix that.
		b.log().Warn("GET stream rejected: session no longer valid")
		return false, false
	case resp.StatusCode == http.StatusUnauthorized:
		// The auth transport already refreshed and retried once; a
		// persistent 401 will not heal by reconnecting.
		b.log().Error("GET stream rejected: authentication failed")
		return false, false
	case resp.StatusCode != http.StatusOK:
		b.log().Debug("GET stream unavailable; will retry", "status", resp.StatusCode)
		return false, true
	}
	if mediaType(resp.Header.Get("Content-Type")) != "text/event-stream" {
		b.log().Warn("GET stream returned unexpected content type", "content_type", resp.Header.Get("Content-Type"))
		return false, false
	}

	err = readEvents(resp.Body, func(ev Event) error {
		gotEvents = true
		if ev.ID != "" {
			*lastEventID = ev.ID
		}
		b.emit(ev.Data)
		return nil
	})
	if err != nil && ctx.Err() == nil {
		b.log().Debug("GET stream ended; will reconnect", "error", err)
	}
	return gotEvents, ctx.Err() == nil
}

// Shutdown best-effort terminates the upstream session with an HTTP
// DELETE, per the Streamable HTTP spec.
func (b *Bridge) Shutdown(ctx context.Context) {
	sid := b.session()
	if sid == "" {
		return
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, b.Upstream, nil)
	if err != nil {
		return
	}
	req.Header.Set(sessionHeader, sid)
	if pv := b.protocolVersion(); pv != "" {
		req.Header.Set(protocolHeader, pv)
	}
	resp, err := b.Client.Do(req)
	if err != nil {
		b.log().Debug("session DELETE failed", "error", err)
		return
	}
	io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
	resp.Body.Close()
	b.log().Debug("session terminated", "status", resp.StatusCode)
}

func (b *Bridge) captureProtocolVersion(data []byte) {
	if b.protocolVersion() != "" || !bytes.Contains(data, []byte("protocolVersion")) {
		return
	}
	var probe struct {
		Result struct {
			ProtocolVersion string `json:"protocolVersion"`
		} `json:"result"`
	}
	if json.Unmarshal(data, &probe) == nil && probe.Result.ProtocolVersion != "" {
		b.stateMu.Lock()
		b.protocol = probe.Result.ProtocolVersion
		b.stateMu.Unlock()
	}
}

func (b *Bridge) session() string {
	b.stateMu.Lock()
	defer b.stateMu.Unlock()
	return b.sessionID
}

func (b *Bridge) setSession(sid string) {
	b.stateMu.Lock()
	defer b.stateMu.Unlock()
	if b.sessionID == "" {
		b.sessionID = sid
	}
}

func (b *Bridge) protocolVersion() string {
	b.stateMu.Lock()
	defer b.stateMu.Unlock()
	return b.protocol
}

func (b *Bridge) timeout() time.Duration {
	if b.Timeout > 0 {
		return b.Timeout
	}
	return 120 * time.Second
}

func (b *Bridge) log() *slog.Logger {
	if b.Logger != nil {
		return b.Logger
	}
	return slog.Default()
}

func mediaType(ct string) string {
	mt, _, err := mime.ParseMediaType(ct)
	if err != nil {
		return strings.ToLower(strings.TrimSpace(strings.Split(ct, ";")[0]))
	}
	return mt
}

func firstLine(b []byte) string {
	s := strings.TrimSpace(string(b))
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	if len(s) > 200 {
		s = s[:200] + "..."
	}
	return s
}
