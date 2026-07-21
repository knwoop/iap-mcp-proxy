// Command iap-mcp-proxy bridges a stdio MCP client to a remote MCP
// server protected by Google Cloud Identity-Aware Proxy.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/knwoop/iap-mcp-proxy/internal/auth"
	"github.com/knwoop/iap-mcp-proxy/internal/bridge"
)

// version is set via -ldflags at release time.
var version = "dev"

const (
	exitOK          = 0
	exitConfigError = 1
	exitAuthError   = 2
)

func main() {
	os.Exit(run())
}

func run() int {
	fs := flag.NewFlagSet("iap-mcp-proxy", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: iap-mcp-proxy [flags] <UPSTREAM_URL>\n\nBridges a stdio MCP client to an IAP-protected Streamable HTTP MCP server.\n\nFlags:\n")
		fs.PrintDefaults()
	}

	var (
		audience       = fs.String("audience", envOr("IAP_MCP_AUDIENCE", ""), "OIDC token audience: IAP OAuth client ID (LB-backed IAP) or service URL (direct Cloud Run IAP). Default: origin of UPSTREAM_URL.")
		credentials    = fs.String("credentials", envOr("IAP_MCP_CREDENTIALS", "auto"), "credential source: auto, adc, impersonate, oauth")
		impersonateSA  = fs.String("impersonate-service-account", envOr("IAP_MCP_IMPERSONATE_SA", ""), "service account email to impersonate (implies --credentials=impersonate)")
		downstreamAuth = fs.String("downstream-auth", envOr("IAP_MCP_DOWNSTREAM_AUTH", ""), "value forwarded as the upstream Authorization header; supports env:VAR_NAME indirection")
		refreshMargin  = fs.Duration("refresh-margin", 5*time.Minute, "refresh the ID token this long before expiry")
		timeout        = fs.Duration("timeout", 120*time.Second, "per-request upstream timeout")
		logLevel       = fs.String("log-level", envOr("IAP_MCP_LOG", "warn"), "log level: debug, info, warn, error")
		showVersion    = fs.Bool("version", false, "print version and exit")
	)
	if err := fs.Parse(os.Args[1:]); err != nil {
		return exitConfigError
	}
	if *showVersion {
		fmt.Println("iap-mcp-proxy " + version)
		return exitOK
	}
	if fs.NArg() != 1 {
		fs.Usage()
		return exitConfigError
	}

	logger, err := newLogger(*logLevel)
	if err != nil {
		fmt.Fprintln(os.Stderr, "iap-mcp-proxy:", err)
		return exitConfigError
	}

	upstream := fs.Arg(0)
	u, err := url.Parse(upstream)
	if err != nil || u.Scheme == "" || u.Host == "" {
		fmt.Fprintf(os.Stderr, "iap-mcp-proxy: invalid upstream URL %q (want e.g. https://my-mcp-xxxx.a.run.app/mcp)\n", upstream)
		return exitConfigError
	}

	// Audience derivation: default to the upstream origin, which is
	// correct for direct Cloud Run IAP. LB-backed IAP needs the IAP
	// OAuth client ID passed explicitly.
	aud := *audience
	if aud == "" {
		aud = u.Scheme + "://" + u.Host
		logger.Info("no --audience given; derived from upstream origin (correct for direct Cloud Run IAP; LB-backed IAP needs the IAP OAuth client ID)", "audience", aud)
	}

	downstream, err := resolveDownstreamAuth(*downstreamAuth)
	if err != nil {
		fmt.Fprintln(os.Stderr, "iap-mcp-proxy:", err)
		return exitConfigError
	}

	mode := *credentials
	if *impersonateSA != "" && (mode == "auto" || mode == "") {
		mode = "impersonate"
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	source, err := auth.NewSource(ctx, auth.Config{
		Mode:              mode,
		Audience:          aud,
		ImpersonateSA:     *impersonateSA,
		OAuthClientID:     os.Getenv("IAP_MCP_OAUTH_CLIENT_ID"),
		OAuthClientSecret: os.Getenv("IAP_MCP_OAUTH_CLIENT_SECRET"),
		RefreshMargin:     *refreshMargin,
		Logger:            logger,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "iap-mcp-proxy:", err)
		return exitAuthError
	}

	// Fail fast: mint one token before touching stdin so auth problems
	// surface immediately with a clear message and exit code.
	if _, err := source.Token(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "iap-mcp-proxy: auth bootstrap failed:", err)
		return exitAuthError
	}

	client := &http.Client{
		Transport: &auth.Transport{
			Source:         source,
			Audience:       aud,
			DownstreamAuth: downstream,
			Logger:         logger,
		},
		// Never follow redirects: a 302 to accounts.google.com is an
		// auth failure signal, not a path to follow.
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	b := &bridge.Bridge{
		Upstream: upstream,
		Client:   client,
		Timeout:  *timeout,
		Logger:   logger,
		Stdin:    os.Stdin,
		Stdout:   os.Stdout,
	}

	runErr := b.Run(ctx)

	// Best-effort session termination on the way out (stdin EOF or
	// signal), independent of the (possibly canceled) run context.
	shCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	b.Shutdown(shCtx)

	if runErr != nil && runErr != context.Canceled {
		logger.Error("bridge terminated", "error", runErr)
	}
	return exitOK
}

// resolveDownstreamAuth resolves env:VAR_NAME indirection so secrets
// don't have to appear in MCP client config files.
func resolveDownstreamAuth(v string) (string, error) {
	name, ok := strings.CutPrefix(v, "env:")
	if !ok {
		return v, nil
	}
	val := os.Getenv(name)
	if val == "" {
		return "", fmt.Errorf("--downstream-auth points at env:%s but %s is empty or unset", name, name)
	}
	return val, nil
}

// newLogger builds a stderr-only slog logger; stdout is reserved for
// the MCP channel.
func newLogger(level string) (*slog.Logger, error) {
	var l slog.Level
	switch strings.ToLower(level) {
	case "debug":
		l = slog.LevelDebug
	case "info":
		l = slog.LevelInfo
	case "warn":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		return nil, fmt.Errorf("unknown --log-level %q (want debug, info, warn, or error)", level)
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: l})), nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
