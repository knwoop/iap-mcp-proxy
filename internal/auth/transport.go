package auth

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"github.com/knwoop/iap-mcp-proxy/internal/iap"
)

// Transport is an http.RoundTripper that attaches an IAP ID token as
// Proxy-Authorization (IAP consumes and strips it, so the upstream app
// never sees it) and optionally a downstream Authorization header.
//
// On an IAP auth failure (401, or 302 to accounts.google.com) it
// refreshes the token once and retries once; a second consecutive
// failure is returned to the caller.
type Transport struct {
	Base           http.RoundTripper
	Source         *Cached
	Audience       string
	DownstreamAuth string
	Logger         *slog.Logger
}

func (t *Transport) base() http.RoundTripper {
	if t.Base != nil {
		return t.Base
	}
	return http.DefaultTransport
}

func (t *Transport) log() *slog.Logger {
	if t.Logger != nil {
		return t.Logger
	}
	return slog.Default()
}

func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Buffer the body so the request can be replayed on retry. MCP
	// messages are small; this is not a general-purpose proxy body.
	var body []byte
	if req.Body != nil {
		var err error
		body, err = io.ReadAll(req.Body)
		req.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("reading request body: %w", err)
		}
	}

	resp, err := t.send(req, body)
	if err != nil {
		return nil, err
	}
	if !iap.IsAuthFailure(resp) {
		return resp, nil
	}

	// Auth failure: drop the cached token, mint a fresh one, retry once.
	t.log().Debug("upstream IAP auth failure; refreshing token and retrying once",
		"status", resp.StatusCode, "iap_generated", iap.IsIAPResponse(resp))
	drain(resp)
	t.Source.Invalidate()

	resp, err = t.send(req, body)
	if err != nil {
		return nil, err
	}
	if iap.IsAuthFailure(resp) {
		t.log().Error(iap.ActionableMessage(resp, t.Audience))
	}
	return resp, nil
}

func (t *Transport) send(req *http.Request, body []byte) (*http.Response, error) {
	r := req.Clone(req.Context())
	if body != nil {
		r.Body = io.NopCloser(bytes.NewReader(body))
		r.ContentLength = int64(len(body))
		r.GetBody = func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(body)), nil
		}
	}

	tok, err := t.Source.Token(r.Context())
	if err != nil {
		return nil, fmt.Errorf("obtaining IAP ID token: %w", err)
	}
	r.Header.Set("Proxy-Authorization", "Bearer "+tok)
	if t.DownstreamAuth != "" {
		r.Header.Set("Authorization", t.DownstreamAuth)
	}
	return t.base().RoundTrip(r)
}

func drain(resp *http.Response) {
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	resp.Body.Close()
}
