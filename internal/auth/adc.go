package auth

import (
	"context"
	"fmt"

	"golang.org/x/oauth2"
	"google.golang.org/api/idtoken"
)

// adcSource mints ID tokens from Application Default Credentials
// (service account key, workload identity, GCE metadata, ...).
type adcSource struct {
	ts oauth2.TokenSource
}

func newADCSource(ctx context.Context, audience string) (Source, error) {
	ts, err := idtoken.NewTokenSource(ctx, audience)
	if err != nil {
		return nil, fmt.Errorf("creating ID token source from ADC: %w", err)
	}
	return &adcSource{ts: ts}, nil
}

func (s *adcSource) Token(ctx context.Context) (string, error) {
	tok, err := s.ts.Token()
	if err != nil {
		return "", fmt.Errorf("minting ID token from ADC: %w", err)
	}
	// idtoken sources return the ID token in AccessToken.
	return tok.AccessToken, nil
}
