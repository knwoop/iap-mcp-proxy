package auth

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func newTestCached(t *testing.T) (*Cached, *fakeSource) {
	t.Helper()
	fake := &fakeSource{mint: func(n int) (string, error) {
		return makeJWT(t, time.Now().Add(time.Hour)), nil
	}}
	return NewCached(fake, 5*time.Minute), fake
}

func TestTransportSetsHeaders(t *testing.T) {
	var gotProxy, gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotProxy = r.Header.Get("Proxy-Authorization")
		gotAuth = r.Header.Get("Authorization")
	}))
	defer srv.Close()

	cached, _ := newTestCached(t)
	client := &http.Client{Transport: &Transport{Source: cached, DownstreamAuth: "Bearer app-token"}}
	resp, err := client.Post(srv.URL, "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	if !strings.HasPrefix(gotProxy, "Bearer ey") {
		t.Errorf("Proxy-Authorization = %q, want Bearer <jwt>", gotProxy)
	}
	if gotAuth != "Bearer app-token" {
		t.Errorf("Authorization = %q", gotAuth)
	}
}

func TestTransportRetriesOnceOn401(t *testing.T) {
	var attempts int
	var bodies []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		b, _ := io.ReadAll(r.Body)
		bodies = append(bodies, string(b))
		if attempts == 1 {
			w.Header().Set("X-Goog-Iap-Generated-Response", "true")
			http.Error(w, "Invalid IAP credentials", http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cached, fake := newTestCached(t)
	client := &http.Client{Transport: &Transport{Source: cached}}
	resp, err := client.Post(srv.URL, "application/json", strings.NewReader(`{"id":1}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200 after retry", resp.StatusCode)
	}
	if attempts != 2 {
		t.Errorf("attempts = %d, want 2", attempts)
	}
	if fake.calls != 2 {
		t.Errorf("token mints = %d, want 2 (refresh on 401)", fake.calls)
	}
	if bodies[0] != `{"id":1}` || bodies[1] != `{"id":1}` {
		t.Errorf("body not replayed on retry: %q", bodies)
	}
}

func TestTransportGivesUpAfterSecond401(t *testing.T) {
	var attempts int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		w.Header().Set("X-Goog-Iap-Generated-Response", "true")
		http.Error(w, "Invalid IAP credentials: audience mismatch", http.StatusUnauthorized)
	}))
	defer srv.Close()

	cached, _ := newTestCached(t)
	client := &http.Client{Transport: &Transport{Source: cached, Audience: "https://x.a.run.app"}}
	resp, err := client.Post(srv.URL, "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401 surfaced", resp.StatusCode)
	}
	if attempts != 2 {
		t.Errorf("attempts = %d, want exactly 2 (no retry storm)", attempts)
	}
}

func TestTransportTreats302ToGoogleAsAuthFailure(t *testing.T) {
	var attempts int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts == 1 {
			w.Header().Set("Location", "https://accounts.google.com/o/oauth2/v2/auth?x=y")
			w.WriteHeader(http.StatusFound)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cached, _ := newTestCached(t)
	client := &http.Client{
		Transport: &Transport{Source: cached},
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	resp, err := client.Post(srv.URL, "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK || attempts != 2 {
		t.Errorf("status=%d attempts=%d, want 200 after one retry", resp.StatusCode, attempts)
	}
}

func TestTransportTokenError(t *testing.T) {
	fake := &fakeSource{mint: func(n int) (string, error) {
		return "", context.DeadlineExceeded
	}}
	client := &http.Client{Transport: &Transport{Source: NewCached(fake, time.Minute)}}
	_, err := client.Get("http://127.0.0.1:0/never")
	if err == nil || !strings.Contains(err.Error(), "obtaining IAP ID token") {
		t.Fatalf("want token error, got %v", err)
	}
}
