package auth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

// makeJWT builds an unsigned JWT with the given expiry, good enough for
// the cache's exp parsing (signatures are IAP's job, not ours).
func makeJWT(t *testing.T, exp time.Time) string {
	t.Helper()
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"RS256"}`))
	payload, err := json.Marshal(map[string]any{"exp": exp.Unix(), "aud": "test"})
	if err != nil {
		t.Fatal(err)
	}
	return header + "." + base64.RawURLEncoding.EncodeToString(payload) + ".sig"
}

type fakeSource struct {
	calls int
	mint  func(n int) (string, error)
}

func (f *fakeSource) Token(ctx context.Context) (string, error) {
	f.calls++
	return f.mint(f.calls)
}

func TestCachedReusesUntilMargin(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	exp := now.Add(time.Hour)
	fake := &fakeSource{mint: func(n int) (string, error) {
		return makeJWT(t, exp), nil
	}}
	c := NewCached(fake, 5*time.Minute)
	c.now = func() time.Time { return now }

	for range 3 {
		if _, err := c.Token(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if fake.calls != 1 {
		t.Fatalf("want 1 mint, got %d", fake.calls)
	}

	// Advance to just inside the refresh margin: must re-mint.
	now = exp.Add(-4 * time.Minute)
	exp = now.Add(time.Hour)
	if _, err := c.Token(context.Background()); err != nil {
		t.Fatal(err)
	}
	if fake.calls != 2 {
		t.Fatalf("want 2 mints after entering margin, got %d", fake.calls)
	}
}

func TestCachedInvalidate(t *testing.T) {
	fake := &fakeSource{mint: func(n int) (string, error) {
		return makeJWT(t, time.Now().Add(time.Hour)), nil
	}}
	c := NewCached(fake, 5*time.Minute)

	if _, err := c.Token(context.Background()); err != nil {
		t.Fatal(err)
	}
	c.Invalidate()
	if _, err := c.Token(context.Background()); err != nil {
		t.Fatal(err)
	}
	if fake.calls != 2 {
		t.Fatalf("want re-mint after Invalidate, got %d calls", fake.calls)
	}
}

func TestCachedRejectsMalformedToken(t *testing.T) {
	fake := &fakeSource{mint: func(n int) (string, error) {
		return "not-a-jwt", nil
	}}
	c := NewCached(fake, time.Minute)
	if _, err := c.Token(context.Background()); err == nil {
		t.Fatal("want error for malformed token")
	}
}

func TestCachedPropagatesMintError(t *testing.T) {
	fake := &fakeSource{mint: func(n int) (string, error) {
		return "", fmt.Errorf("boom %d", n)
	}}
	c := NewCached(fake, time.Minute)
	if _, err := c.Token(context.Background()); err == nil {
		t.Fatal("want mint error")
	}
}
