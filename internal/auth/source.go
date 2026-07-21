// Package auth provides OIDC ID token sources for authenticating to
// Google Cloud IAP, plus an http.RoundTripper that attaches them.
package auth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
)

// Source mints raw OIDC ID tokens for a fixed audience.
type Source interface {
	Token(ctx context.Context) (string, error)
}

// Config selects and configures a credential source.
type Config struct {
	// Mode is one of "auto", "adc", "impersonate", "oauth".
	Mode string
	// Audience is the OIDC token audience (IAP client ID or service URL).
	Audience string
	// ImpersonateSA is the service account email to impersonate.
	ImpersonateSA string
	// OAuthClientID / OAuthClientSecret configure the desktop OAuth flow.
	OAuthClientID     string
	OAuthClientSecret string
	// RefreshMargin is how long before expiry a cached token is refreshed.
	RefreshMargin time.Duration

	Logger *slog.Logger
}

// NewSource selects a credential source per the precedence rules:
// impersonate (if an SA is configured) > adc > oauth.
// The returned Source caches tokens and refreshes them RefreshMargin
// before expiry.
func NewSource(ctx context.Context, cfg Config) (*Cached, error) {
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}

	var (
		src Source
		err error
	)
	switch cfg.Mode {
	case "impersonate":
		if cfg.ImpersonateSA == "" {
			return nil, errors.New("--credentials=impersonate requires --impersonate-service-account")
		}
		src, err = newImpersonatedSource(ctx, cfg.ImpersonateSA, cfg.Audience)
	case "adc":
		src, err = newADCSource(ctx, cfg.Audience)
	case "oauth":
		src, err = newOAuthSource(ctx, cfg)
	case "auto", "":
		if cfg.ImpersonateSA != "" {
			src, err = newImpersonatedSource(ctx, cfg.ImpersonateSA, cfg.Audience)
			break
		}
		src, err = newADCSource(ctx, cfg.Audience)
		if err != nil && isUserCredentialErr(err) {
			log.Info("ADC holds user credentials which cannot mint ID tokens for an arbitrary audience; falling back to the desktop OAuth flow", "adc_error", err)
			src, err = newOAuthSource(ctx, cfg)
			if err != nil {
				return nil, fmt.Errorf("no usable credentials: run 'gcloud auth application-default login' with a service account, configure --impersonate-service-account, or set IAP_MCP_OAUTH_CLIENT_ID/IAP_MCP_OAUTH_CLIENT_SECRET for --credentials=oauth (%w)", err)
			}
		}
	default:
		return nil, fmt.Errorf("unknown --credentials mode %q (want auto, adc, impersonate, or oauth)", cfg.Mode)
	}
	if err != nil {
		return nil, err
	}
	return NewCached(src, cfg.RefreshMargin), nil
}

// isUserCredentialErr reports whether err indicates that ADC resolved to
// end-user (gcloud) credentials, which cannot mint arbitrary-audience ID
// tokens.
func isUserCredentialErr(err error) bool {
	s := err.Error()
	return strings.Contains(s, "unsupported credentials type") ||
		strings.Contains(s, "authorized_user")
}

// Cached wraps a Source with in-memory caching and margin-based refresh.
type Cached struct {
	src    Source
	margin time.Duration
	now    func() time.Time

	mu     sync.Mutex
	token  string
	expiry time.Time
}

// NewCached returns a caching wrapper around src that refreshes tokens
// margin before their expiry.
func NewCached(src Source, margin time.Duration) *Cached {
	return &Cached{src: src, margin: margin, now: time.Now}
}

// Token returns a cached ID token, minting a fresh one if the cache is
// empty or within the refresh margin of expiry.
func (c *Cached) Token(ctx context.Context) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.token != "" && c.now().Before(c.expiry.Add(-c.margin)) {
		return c.token, nil
	}
	tok, err := c.src.Token(ctx)
	if err != nil {
		return "", err
	}
	exp, err := tokenExpiry(tok)
	if err != nil {
		return "", fmt.Errorf("credential source returned a malformed ID token: %w", err)
	}
	c.token, c.expiry = tok, exp
	return tok, nil
}

// Invalidate drops the cached token so the next Token call mints a fresh
// one. Used after an upstream 401.
func (c *Cached) Invalidate() {
	c.mu.Lock()
	c.token = ""
	c.mu.Unlock()
}

// tokenExpiry extracts the exp claim from an unverified JWT. The proxy
// never trusts the token content for authorization — IAP verifies it —
// so decoding without signature verification is fine here.
func tokenExpiry(jwt string) (time.Time, error) {
	parts := strings.Split(jwt, ".")
	if len(parts) != 3 {
		return time.Time{}, errors.New("not a JWT")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return time.Time{}, fmt.Errorf("decoding claims: %w", err)
	}
	var claims struct {
		Exp int64 `json:"exp"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return time.Time{}, fmt.Errorf("parsing claims: %w", err)
	}
	if claims.Exp == 0 {
		return time.Time{}, errors.New("missing exp claim")
	}
	return time.Unix(claims.Exp, 0), nil
}
