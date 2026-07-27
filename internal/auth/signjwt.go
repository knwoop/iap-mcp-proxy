package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"google.golang.org/api/iamcredentials/v1"
)

// signJWTSource mints self-signed service-account JWTs via the IAM
// Credentials signJwt API, using ADC as the base identity.
//
// Unlike the OIDC modes (adc/impersonate/oauth), these tokens are
// accepted by modern auto-managed direct-Cloud-Run IAP, which rejects
// Google-issued OIDC ID tokens outright ("Invalid JWT audience"). Since
// the IAP OAuth Admin API was shut down, new IAP deployments use the
// auto-managed OAuth client, whose documented programmatic-access path
// is a self-signed SA JWT rather than an ID token.
//
// The caller's ADC identity needs roles/iam.serviceAccountTokenCreator
// on the target SA, and the SA needs roles/iap.httpsResourceAccessor on
// the IAP resource.
type signJWTSource struct {
	svc      *iamcredentials.Service
	name     string // projects/-/serviceAccounts/<email>
	sa       string
	audience string
	now      func() time.Time
}

func newSignJWTSource(ctx context.Context, sa, audience string) (Source, error) {
	svc, err := iamcredentials.NewService(ctx)
	if err != nil {
		return nil, fmt.Errorf("creating IAM Credentials client: %w", err)
	}
	return &signJWTSource{
		svc:      svc,
		name:     "projects/-/serviceAccounts/" + sa,
		sa:       sa,
		audience: audience,
		now:      time.Now,
	}, nil
}

func (s *signJWTSource) Token(ctx context.Context) (string, error) {
	payload, err := signJWTClaims(s.sa, s.audience, s.now())
	if err != nil {
		return "", err
	}
	resp, err := s.svc.Projects.ServiceAccounts.
		SignJwt(s.name, &iamcredentials.SignJwtRequest{Payload: payload}).
		Context(ctx).Do()
	if err != nil {
		return "", fmt.Errorf("signing JWT as %s: %w (does the caller have roles/iam.serviceAccountTokenCreator on it?)", s.sa, err)
	}
	return resp.SignedJwt, nil
}

// signJWTClaims builds the self-signed SA JWT payload that
// auto-managed IAP accepts. The audience must be the canonical
// run.app URL plus "/*" (or the exact request path); origin-only and
// the project-number URL are rejected. Tokens are valid for one hour.
func signJWTClaims(sa, audience string, now time.Time) (string, error) {
	claims := map[string]any{
		"iss": sa,
		"sub": sa,
		"aud": audience,
		"iat": now.Unix(),
		"exp": now.Add(time.Hour).Unix(),
	}
	b, err := json.Marshal(claims)
	if err != nil {
		return "", fmt.Errorf("marshaling JWT claims: %w", err)
	}
	return string(b), nil
}
