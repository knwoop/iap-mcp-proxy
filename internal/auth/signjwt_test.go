package auth

import (
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func b64URL(s string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(s))
}

func TestSignJWTClaims(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	sa := "mcp-caller@proj.iam.gserviceaccount.com"
	aud := "https://svc-xxxx-an.a.run.app/*"

	payload, err := signJWTClaims(sa, aud, now)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal([]byte(payload), &got); err != nil {
		t.Fatalf("payload is not JSON: %v (%q)", err, payload)
	}

	// iss and sub are the SA; aud is passed through verbatim; the token
	// is valid for exactly one hour from iat.
	want := map[string]any{
		"iss": sa,
		"sub": sa,
		"aud": aud,
		"iat": float64(now.Unix()),
		"exp": float64(now.Add(time.Hour).Unix()),
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("claims mismatch (-want +got):\n%s", diff)
	}
}

func TestSignJWTClaimsExpiryParsesInCache(t *testing.T) {
	// The self-signed JWT is not a real signed token, but tokenExpiry
	// only reads the (unsigned) claims segment, so a 3-part token with
	// our payload as the middle segment must parse. This guards the
	// assumption that Cached refresh works for signjwt tokens.
	now := time.Unix(1_700_000_000, 0)
	payload, err := signJWTClaims("sa@p.iam.gserviceaccount.com", "aud", now)
	if err != nil {
		t.Fatal(err)
	}
	jwt := "eyJhbGciOiJSUzI1NiJ9." + b64URL(payload) + ".sig"
	exp, err := tokenExpiry(jwt)
	if err != nil {
		t.Fatalf("tokenExpiry: %v", err)
	}
	if !exp.Equal(now.Add(time.Hour)) {
		t.Errorf("exp = %v, want %v", exp, now.Add(time.Hour))
	}
}
