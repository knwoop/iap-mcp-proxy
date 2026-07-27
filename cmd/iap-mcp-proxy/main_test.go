package main

import (
	"net/url"
	"testing"
)

func TestDefaultAudience(t *testing.T) {
	cases := []struct {
		mode     string
		upstream string
		want     string
	}{
		// signjwt defaults to the exact upstream endpoint (least
		// privilege); the path is kept.
		{"signjwt", "https://svc-xxxx-an.a.run.app/mcp", "https://svc-xxxx-an.a.run.app/mcp"},
		// The escaped path is preserved verbatim (u.Path would decode
		// %2F and collapse this to /mcp/v1).
		{"signjwt", "https://svc-xxxx-an.a.run.app/mcp%2Fv1", "https://svc-xxxx-an.a.run.app/mcp%2Fv1"},
		// Query and fragment are excluded from the audience.
		{"signjwt", "https://svc-xxxx-an.a.run.app/mcp?foo=bar", "https://svc-xxxx-an.a.run.app/mcp"},
		{"signjwt", "https://svc-xxxx-an.a.run.app/mcp#frag", "https://svc-xxxx-an.a.run.app/mcp"},
		// A pathless (or root) upstream falls back to the "/*" wildcard,
		// since origin-only is rejected by auto-managed IAP.
		{"signjwt", "https://svc-xxxx-an.a.run.app", "https://svc-xxxx-an.a.run.app/*"},
		{"signjwt", "https://svc-xxxx-an.a.run.app/", "https://svc-xxxx-an.a.run.app/*"},
		// OIDC modes use the origin.
		{"adc", "https://svc-xxxx-an.a.run.app/mcp", "https://svc-xxxx-an.a.run.app"},
		{"impersonate", "https://svc-xxxx-an.a.run.app/mcp", "https://svc-xxxx-an.a.run.app"},
		{"oauth", "https://svc-xxxx-an.a.run.app/mcp", "https://svc-xxxx-an.a.run.app"},
		{"auto", "https://svc-xxxx-an.a.run.app/mcp", "https://svc-xxxx-an.a.run.app"},
	}
	for _, c := range cases {
		u, err := url.Parse(c.upstream)
		if err != nil {
			t.Fatal(err)
		}
		if got := defaultAudience(c.mode, u); got != c.want {
			t.Errorf("defaultAudience(%q, %q) = %q, want %q", c.mode, c.upstream, got, c.want)
		}
	}
}
