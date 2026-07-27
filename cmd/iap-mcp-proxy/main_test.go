package main

import (
	"net/url"
	"testing"
)

func TestDefaultAudience(t *testing.T) {
	u, err := url.Parse("https://svc-xxxx-an.a.run.app/mcp")
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		mode string
		want string
	}{
		// signjwt needs the canonical run.app URL plus "/*"; the path is
		// dropped and origin-only is not sufficient for auto-managed IAP.
		{"signjwt", "https://svc-xxxx-an.a.run.app/*"},
		// OIDC modes use the origin.
		{"adc", "https://svc-xxxx-an.a.run.app"},
		{"impersonate", "https://svc-xxxx-an.a.run.app"},
		{"oauth", "https://svc-xxxx-an.a.run.app"},
		{"auto", "https://svc-xxxx-an.a.run.app"},
	}
	for _, c := range cases {
		if got := defaultAudience(c.mode, u); got != c.want {
			t.Errorf("defaultAudience(%q) = %q, want %q", c.mode, got, c.want)
		}
	}
}
