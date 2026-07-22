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
	"errors"
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
	// Cached client handshake messages, replayed to transparently
	// re-establish the session when the upstream reports it expired
	// (e.g. after a Cloud Run redeploy).
	initMsg        []byte
	initializedMsg []byte

	// reinitMu serializes session recovery so concurrent 404s trigger
	// a single re-initialize.
	reinitMu sync.Mutex

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
			switch probeMethod(msg) {
			case "initialize":
				b.setHandshake(&b.initMsg, msg)
				b.forward(ctx, msg)
				// With the session established, open the standalone GET
				// stream for server-initiated messages that arrive
				// outside any request (list_changed notifications,
				// sampling/elicitation requests, logs).
				b.listenOnce.Do(func() { go b.listen(ctx) })
			case "notifications/initialized":
				b.setHandshake(&b.initializedMsg, msg)
				wg.Go(func() { b.forward(ctx, msg) })
			default:
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

func probeMethod(msg []byte) string {
	var probe struct {
		Method string `json:"method"`
	}
	if json.Unmarshal(msg, &probe) != nil {
		return ""
	}
	return probe.Method
}

// post sends one JSON-RPC message upstream, attaching the current
// session (returned as sentSession) and protocol version headers, and
// records any session ID the response carries.
func (b *Bridge) post(ctx context.Context, msg []byte) (resp *http.Response, sentSession string, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, b.Upstream, bytes.NewReader(msg))
	if err != nil {
		return nil, "", fmt.Errorf("building upstream request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	sentSession = b.session()
	if sentSession != "" {
		req.Header.Set(sessionHeader, sentSession)
	}
	if pv := b.protocolVersion(); pv != "" {
		req.Header.Set(protocolHeader, pv)
	}

	resp, err = b.Client.Do(req)
	if err != nil {
		return nil, sentSession, fmt.Errorf("upstream request failed: %w", err)
	}
	if sid := resp.Header.Get(sessionHeader); sid != "" {
		b.setSession(sid)
	}
	return resp, sentSession, nil
}

// forward POSTs one JSON-RPC message upstream and relays the response.
// A 404 on a request that carried a session ID means the session
// expired (spec: the client MUST start a new session); the bridge
// recovers transparently by replaying the cached initialize handshake
// and retrying the message, since a stdio client will never
// re-initialize on its own.
func (b *Bridge) forward(ctx context.Context, msg []byte) {
	ctx, cancel := context.WithTimeout(ctx, b.timeout())
	defer cancel()

	resp, sentSession, err := b.post(ctx, msg)
	if err != nil {
		b.replyError(msg, err.Error())
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound && sentSession != "" {
		drainBody(resp)
		if err := b.recoverSession(ctx, sentSession); err != nil {
			b.replyError(msg, fmt.Sprintf("upstream session expired and re-initialize failed: %v", err))
			return
		}
		resp, _, err = b.post(ctx, msg)
		if err != nil {
			b.replyError(msg, err.Error())
			return
		}
		defer resp.Body.Close()
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

// recoverSession transparently re-establishes an expired session by
// replaying the cached initialize request (and the initialized
// notification), without emitting anything to the stdio client, which
// believes its original session is still alive. Concurrent callers are
// serialized; only the first one whose stale session is still current
// performs the replay.
func (b *Bridge) recoverSession(ctx context.Context, stale string) error {
	b.reinitMu.Lock()
	defer b.reinitMu.Unlock()
	if stale == "" {
		return errors.New("request carried no session")
	}
	if b.session() != stale {
		// Another goroutine already re-established the session.
		return nil
	}
	init := b.handshake(&b.initMsg)
	if init == nil {
		return errors.New("no cached initialize request to replay")
	}
	b.log().Warn("upstream session expired; re-initializing transparently", "stale_session", stale)
	b.clearSession(stale)

	resp, _, err := b.post(ctx, init)
	if err != nil {
		return err
	}
	sid := resp.Header.Get(sessionHeader)
	drainBody(resp)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("replayed initialize returned HTTP %d", resp.StatusCode)
	}
	if sid == "" {
		return errors.New("replayed initialize returned no session ID")
	}
	b.forceSession(sid)

	if note := b.handshake(&b.initializedMsg); note != nil {
		if resp, _, err := b.post(ctx, note); err == nil {
			drainBody(resp)
		} else {
			b.log().Warn("replaying initialized notification failed", "error", err)
		}
	}
	b.log().Info("session re-established", "session", sid)
	return nil
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
	sid := b.session()
	if sid != "" {
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
		// Session gone (e.g. upstream redeploy): re-establish it
		// transparently, then reconnect.
		if err := b.recoverSession(ctx, sid); err != nil {
			b.log().Warn("GET stream rejected and session recovery failed", "error", err)
			return false, false
		}
		return false, true
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

// setSession records a session ID if none is set; the first response
// carrying one wins. Recovery replaces it via clearSession/forceSession.
func (b *Bridge) setSession(sid string) {
	b.stateMu.Lock()
	defer b.stateMu.Unlock()
	if b.sessionID == "" {
		b.sessionID = sid
	}
}

// clearSession drops the session only if it still matches the stale
// value, so a session established by a concurrent recovery survives.
func (b *Bridge) clearSession(stale string) {
	b.stateMu.Lock()
	defer b.stateMu.Unlock()
	if b.sessionID == stale {
		b.sessionID = ""
	}
}

func (b *Bridge) forceSession(sid string) {
	b.stateMu.Lock()
	defer b.stateMu.Unlock()
	b.sessionID = sid
}

func (b *Bridge) setHandshake(slot *[]byte, msg []byte) {
	b.stateMu.Lock()
	defer b.stateMu.Unlock()
	*slot = msg
}

func (b *Bridge) handshake(slot *[]byte) []byte {
	b.stateMu.Lock()
	defer b.stateMu.Unlock()
	return *slot
}

// drainBody fully consumes and closes a response body; used for
// responses handled internally (recovery handshake, expired-session
// errors) whose content is never relayed to the client.
func drainBody(resp *http.Response) {
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
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
