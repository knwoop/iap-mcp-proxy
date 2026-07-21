package auth

import (
	"context"
	"fmt"

	"golang.org/x/oauth2"
	"google.golang.org/api/impersonate"
)

// impersonatedSource mints ID tokens via the IAM Credentials API
// (generateIdToken) for a target service account, using the caller's
// ADC as the base identity.
type impersonatedSource struct {
	ts oauth2.TokenSource
	sa string
}

func newImpersonatedSource(ctx context.Context, sa, audience string) (Source, error) {
	ts, err := impersonate.IDTokenSource(ctx, impersonate.IDTokenConfig{
		TargetPrincipal: sa,
		Audience:        audience,
		IncludeEmail:    true,
	})
	if err != nil {
		return nil, fmt.Errorf("creating impersonated ID token source for %s: %w", sa, err)
	}
	return &impersonatedSource{ts: ts, sa: sa}, nil
}

func (s *impersonatedSource) Token(ctx context.Context) (string, error) {
	tok, err := s.ts.Token()
	if err != nil {
		return "", fmt.Errorf("impersonating %s: %w (does the caller have roles/iam.serviceAccountTokenCreator on it?)", s.sa, err)
	}
	return tok.AccessToken, nil
}
