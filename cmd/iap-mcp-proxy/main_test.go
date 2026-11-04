package main

import (
	"net/url"
	"testing"

	"github.com/google/go-cmp/cmp"
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

func TestResolveDownstreamAuth(t *testing.T) {
	t.Setenv("IAP_MCP_TEST_TOKEN", "Bearer from-env")
	t.Setenv("IAP_MCP_TEST_EMPTY", "")
	cases := []struct {
		in      string
		want    downstreamAuth
		wantErr bool
	}{
		{in: "Bearer literal", want: downstreamAuth{value: "Bearer literal"}},
		{in: "env:IAP_MCP_TEST_TOKEN", want: downstreamAuth{value: "Bearer from-env"}},
		{in: "env:IAP_MCP_TEST_EMPTY", wantErr: true},
		{in: "env:IAP_MCP_TEST_UNSET", wantErr: true},
		{in: "cmd:gcloud auth  print-identity-token", want: downstreamAuth{command: []string{"gcloud", "auth", "print-identity-token"}}},
		{in: "cmd:", wantErr: true},
		{in: "cmd:   ", wantErr: true},
	}
	for _, c := range cases {
		got, err := resolveDownstreamAuth(c.in)
		if c.wantErr {
			if err == nil {
				t.Errorf("resolveDownstreamAuth(%q) = %+v, want error", c.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("resolveDownstreamAuth(%q) error: %v", c.in, err)
			continue
		}
		if diff := cmp.Diff(c.want, got, cmp.AllowUnexported(downstreamAuth{})); diff != "" {
			t.Errorf("resolveDownstreamAuth(%q) mismatch (-want +got):\n%s", c.in, diff)
		}
	}
}
