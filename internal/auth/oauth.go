package auth

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"time"

	"github.com/zalando/go-keyring"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

const keyringService = "iap-mcp-proxy"

// oauthSource implements the installed-app (desktop) OAuth 2.0 flow
// against Google. The first run opens a browser to obtain a refresh
// token, which is persisted (keychain, falling back to a 0600 file).
// Subsequent runs silently exchange the refresh token for ID tokens.
//
// IAP programmatic access requires the OAuth client to live in the same
// project as the IAP resource; see
// https://cloud.google.com/iap/docs/authentication-howto
type oauthSource struct {
	cfg *oauth2.Config
	log *slog.Logger
}

func newOAuthSource(ctx context.Context, c Config) (Source, error) {
	if c.OAuthClientID == "" || c.OAuthClientSecret == "" {
		return nil, errors.New("--credentials=oauth requires IAP_MCP_OAUTH_CLIENT_ID and IAP_MCP_OAUTH_CLIENT_SECRET (a desktop OAuth client in the same project as the IAP resource; see https://cloud.google.com/iap/docs/authentication-howto)")
	}
	log := c.Logger
	if log == nil {
		log = slog.Default()
	}
	return &oauthSource{
		cfg: &oauth2.Config{
			ClientID:     c.OAuthClientID,
			ClientSecret: c.OAuthClientSecret,
			Endpoint:     google.Endpoint,
			Scopes:       []string{"openid", "email"},
		},
		log: log,
	}, nil
}

func (s *oauthSource) Token(ctx context.Context) (string, error) {
	rt, err := s.loadRefreshToken()
	if err == nil && rt != "" {
		if id, err := s.idTokenFromRefresh(ctx, rt); err == nil {
			return id, nil
		} else {
			s.log.Warn("stored refresh token rejected; re-running browser sign-in", "error", err)
		}
	}

	rt, id, err := s.browserFlow(ctx)
	if err != nil {
		return "", err
	}
	if err := s.storeRefreshToken(rt); err != nil {
		s.log.Warn("could not persist refresh token; you will be asked to sign in again next run", "error", err)
	}
	return id, nil
}

// idTokenFromRefresh exchanges a stored refresh token for a fresh ID token.
func (s *oauthSource) idTokenFromRefresh(ctx context.Context, refreshToken string) (string, error) {
	tok, err := s.cfg.TokenSource(ctx, &oauth2.Token{RefreshToken: refreshToken}).Token()
	if err != nil {
		return "", fmt.Errorf("refreshing token: %w", err)
	}
	id, _ := tok.Extra("id_token").(string)
	if id == "" {
		return "", errors.New("token response contained no id_token")
	}
	return id, nil
}

// browserFlow runs the interactive installed-app flow: local callback
// listener, browser launch, code exchange (with PKCE).
func (s *oauthSource) browserFlow(ctx context.Context) (refreshToken, idToken string, err error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", "", fmt.Errorf("starting OAuth callback listener: %w", err)
	}
	defer ln.Close()

	cfg := *s.cfg
	cfg.RedirectURL = fmt.Sprintf("http://%s/callback", ln.Addr().String())

	state, err := randomState()
	if err != nil {
		return "", "", err
	}
	verifier := oauth2.GenerateVerifier()
	authURL := cfg.AuthCodeURL(state,
		oauth2.AccessTypeOffline,
		oauth2.S256ChallengeOption(verifier),
		// Force a refresh token even if the user consented before.
		oauth2.SetAuthURLParam("prompt", "consent"),
	)

	codeCh := make(chan string, 1)
	errCh := make(chan error, 1)
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/callback" {
			http.NotFound(w, r)
			return
		}
		q := r.URL.Query()
		if q.Get("state") != state {
			errCh <- errors.New("OAuth callback state mismatch")
			http.Error(w, "state mismatch", http.StatusBadRequest)
			return
		}
		if e := q.Get("error"); e != "" {
			errCh <- fmt.Errorf("authorization failed: %s", e)
			fmt.Fprintf(w, "Sign-in failed: %s. You can close this window.", e)
			return
		}
		codeCh <- q.Get("code")
		fmt.Fprint(w, "Signed in. You can close this window and return to your MCP client.")
	})}
	go srv.Serve(ln)
	defer srv.Close()

	s.log.Info("opening browser for Google sign-in", "url", authURL)
	fmt.Fprintf(os.Stderr, "iap-mcp-proxy: opening browser for Google sign-in.\nIf it does not open, visit:\n%s\n", authURL)
	openBrowser(authURL)

	var code string
	select {
	case code = <-codeCh:
	case err := <-errCh:
		return "", "", err
	case <-time.After(5 * time.Minute):
		return "", "", errors.New("timed out waiting for browser sign-in")
	case <-ctx.Done():
		return "", "", ctx.Err()
	}

	tok, err := cfg.Exchange(ctx, code, oauth2.VerifierOption(verifier))
	if err != nil {
		return "", "", fmt.Errorf("exchanging authorization code: %w", err)
	}
	id, _ := tok.Extra("id_token").(string)
	if id == "" {
		return "", "", errors.New("token response contained no id_token")
	}
	if tok.RefreshToken == "" {
		return "", "", errors.New("token response contained no refresh token")
	}
	return tok.RefreshToken, id, nil
}

func randomState() (string, error) {
	tok, err := generateRandomToken()
	if err != nil {
		return "", fmt.Errorf("generating OAuth state: %w", err)
	}
	return tok, nil
}

// --- refresh token persistence ---

func (s *oauthSource) keyringKey() string { return "refresh-token:" + s.cfg.ClientID }

func (s *oauthSource) loadRefreshToken() (string, error) {
	if v, err := keyring.Get(keyringService, s.keyringKey()); err == nil {
		return v, nil
	}
	b, err := os.ReadFile(s.fallbackPath())
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func (s *oauthSource) storeRefreshToken(rt string) error {
	if err := keyring.Set(keyringService, s.keyringKey(), rt); err == nil {
		return nil
	} else {
		s.log.Warn("OS keychain unavailable; storing refresh token in a 0600 file instead", "path", s.fallbackPath(), "keychain_error", err)
	}
	p := s.fallbackPath()
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		return err
	}
	return os.WriteFile(p, []byte(rt), 0o600)
}

func (s *oauthSource) fallbackPath() string {
	dir, err := os.UserConfigDir()
	if err != nil {
		dir = "."
	}
	return filepath.Join(dir, "iap-mcp-proxy", "refresh-token-"+s.cfg.ClientID)
}

func openBrowser(url string) {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("open", url)
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", url)
	default:
		cmd = exec.Command("xdg-open", url)
	}
	_ = cmd.Start()
}
