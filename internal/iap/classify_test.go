package iap

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

func makeResp(status int, headers map[string]string, body string) *http.Response {
	h := http.Header{}
	for k, v := range headers {
		h.Set(k, v)
	}
	return &http.Response{
		StatusCode: status,
		Header:     h,
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

func TestIsIAPResponse(t *testing.T) {
	if !IsIAPResponse(makeResp(401, map[string]string{"X-Goog-Iap-Generated-Response": "true"}, "")) {
		t.Error("want true for IAP-generated response")
	}
	if IsIAPResponse(makeResp(401, nil, "")) {
		t.Error("want false without the IAP header")
	}
}

func TestIsAuthFailure(t *testing.T) {
	cases := []struct {
		name string
		resp *http.Response
		want bool
	}{
		{"401", makeResp(401, nil, ""), true},
		{"302 to google sign-in", makeResp(302, map[string]string{"Location": "https://accounts.google.com/o/oauth2/v2/auth"}, ""), true},
		{"302 elsewhere", makeResp(302, map[string]string{"Location": "https://example.com/login"}, ""), false},
		{"403", makeResp(403, nil, ""), false},
		{"200", makeResp(200, nil, ""), false},
		{"500", makeResp(500, nil, ""), false},
	}
	for _, c := range cases {
		if got := IsAuthFailure(c.resp); got != c.want {
			t.Errorf("%s: IsAuthFailure = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestActionableMessageAudienceMismatch(t *testing.T) {
	resp := makeResp(401, map[string]string{"X-Goog-Iap-Generated-Response": "true"},
		"Invalid IAP credentials: JWT audience doesn't match this application")
	msg := ActionableMessage(resp, "https://x.a.run.app")
	if !strings.Contains(msg, "--audience") || !strings.Contains(msg, "apps.googleusercontent.com") {
		t.Errorf("message not actionable for audience mismatch: %q", msg)
	}
}

func TestActionableMessagePermissionDenied(t *testing.T) {
	resp := makeResp(403, map[string]string{"X-Goog-Iap-Generated-Response": "true"}, "access denied")
	msg := ActionableMessage(resp, "aud")
	if !strings.Contains(msg, "iap.httpsResourceAccessor") {
		t.Errorf("message not actionable for 403: %q", msg)
	}
}

func TestActionableMessageRedirect(t *testing.T) {
	resp := makeResp(302, map[string]string{"Location": "https://accounts.google.com/x"}, "")
	msg := ActionableMessage(resp, "aud")
	if !strings.Contains(msg, "sign-in") {
		t.Errorf("unexpected message for redirect: %q", msg)
	}
}
