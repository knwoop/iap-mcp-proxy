# iap-mcp-proxy

A client-side bridge that lets generic MCP clients (Claude Desktop, Claude Code, Cursor, ...) connect to remote MCP servers protected by **Google Cloud Identity-Aware Proxy (IAP)**.

The GCP counterpart to [`aws/mcp-proxy-for-aws`](https://github.com/aws/mcp-proxy-for-aws).

```
┌──────────────┐        stdio          ┌───────────────┐   HTTPS + ID token   ┌─────┐      ┌────────────┐
│  MCP client  │ ────────────────────► │ iap-mcp-proxy │ ───────────────────► │ IAP │ ───► │ MCP server │
│ (Claude etc.)│                       │  (this tool)  │  Proxy-Authorization │     │      │ (Cloud Run)│
└──────────────┘                       └───────────────┘                      └─────┘      └────────────┘
```

IAP expects a Google-issued OIDC ID token; the MCP spec's OAuth 2.1 flow can't produce one, so generic clients get a 401/redirect and stop. This proxy runs locally, obtains and refreshes Google credentials, attaches them as `Proxy-Authorization` (IAP consumes and strips this header), and forwards MCP traffic (Streamable HTTP) upstream.

## Install

```sh
go install github.com/knwoop/iap-mcp-proxy/cmd/iap-mcp-proxy@latest
```

or grab a binary from [Releases](https://github.com/knwoop/iap-mcp-proxy/releases) (darwin/linux/windows, amd64/arm64).

## Quick start

1. Make sure you have credentials IAP will accept (see [Credentials](#credentials)):

   ```sh
   gcloud auth application-default login
   ```

2. Add the proxy to your MCP client config. Claude Desktop:

   ```json
   {
     "mcpServers": {
       "internal-tools": {
         "command": "iap-mcp-proxy",
         "args": [
           "--audience", "1234567890-abc.apps.googleusercontent.com",
           "https://mcp.internal.example.com/mcp"
         ]
       }
     }
   }
   ```

   Claude Code:

   ```sh
   claude mcp add internal-tools -- iap-mcp-proxy \
     --audience 1234567890-abc.apps.googleusercontent.com \
     https://mcp.internal.example.com/mcp
   ```

## Usage

```
iap-mcp-proxy [flags] <UPSTREAM_URL>
```

| Flag | Env var | Default | Description |
|---|---|---|---|
| `--audience` | `IAP_MCP_AUDIENCE` | origin of `UPSTREAM_URL` | OIDC token audience. LB-backed IAP: the IAP OAuth client ID (`NNN.apps.googleusercontent.com`). Direct Cloud Run IAP: the service URL (the default usually works). |
| `--credentials` | `IAP_MCP_CREDENTIALS` | `auto` | `auto`, `adc`, `impersonate`, `oauth`. |
| `--impersonate-service-account` | `IAP_MCP_IMPERSONATE_SA` | — | SA email to impersonate (implies `--credentials=impersonate`). |
| `--downstream-auth` | `IAP_MCP_DOWNSTREAM_AUTH` | — | Value forwarded as the upstream `Authorization` header. Supports `env:VAR_NAME` indirection so secrets stay out of client config files. |
| `--refresh-margin` | `IAP_MCP_REFRESH_MARGIN` | `5m` | Refresh the ID token this long before expiry. |
| `--timeout` | `IAP_MCP_TIMEOUT` | `120s` | Upstream timeout: total for JSON responses, idle (time between reads) for SSE streams — so long-running streaming tool calls are not killed while data or keepalives keep arriving. |
| `--log-level` | `IAP_MCP_LOG` | `warn` | `debug` / `info` / `warn` / `error`. Logs go to stderr only. |
| `--version` | — | — | Print version and exit. |

## Credentials

With `--credentials=auto` (the default), sources are tried in this order:

1. **Impersonation** — if `--impersonate-service-account` is set, mint ID tokens via the IAM Credentials API (`generateIdToken`) using your ADC as the base identity. Requires `roles/iam.serviceAccountTokenCreator` on the target SA. Best for CI and shared team setups.
2. **ADC** — if Application Default Credentials are a service account key or workload credential, mint an ID token directly.
3. **Desktop OAuth** — gcloud *user* credentials can't mint arbitrary-audience ID tokens, so the proxy falls back to an installed-app OAuth flow: first run opens a browser for Google sign-in; the refresh token is stored in your OS keychain (fallback: `0600` file under your user config dir). Requires a desktop OAuth client in the same project as the IAP resource, supplied via `IAP_MCP_OAUTH_CLIENT_ID` / `IAP_MCP_OAUTH_CLIENT_SECRET` — see [Google's docs on programmatic IAP authentication](https://cloud.google.com/iap/docs/authentication-howto).

The principal must hold `roles/iap.httpsResourceAccessor` on the IAP resource.

## Notes

- The IAP token travels in `Proxy-Authorization`, which IAP consumes and strips — your app never sees it. If your app has its own auth, pass it with `--downstream-auth` and it is forwarded verbatim as `Authorization`.
- On a 401 (or a 302 into Google sign-in) the proxy refreshes the token and retries once; a second failure is surfaced to the MCP client as a JSON-RPC error with an actionable message on stderr.
- If the upstream reports the session expired (HTTP 404 — e.g. after a Cloud Run redeploy), the proxy transparently replays the cached `initialize` handshake to obtain a fresh session and retries the request; the stdio client never notices.
- If a streaming (SSE) response drops mid-tool-call and the server tags events with IDs, the proxy resumes it with `Last-Event-ID` instead of losing the response.
- After `initialize`, the proxy opens the standalone GET SSE stream so server-initiated messages (`notifications/tools/list_changed`, sampling/elicitation requests, log notifications) reach the client, reconnecting with `Last-Event-ID` if the stream drops. Servers that respond 405 (no standalone stream) are handled silently.
- Exit codes: `0` clean shutdown, `1` fatal error (bad configuration or unrecoverable runtime failure), `2` auth bootstrap failure.

See [`docs/setup-gcp.md`](docs/setup-gcp.md) for setting up IAP in both deployment modes (direct Cloud Run IAP and IAP behind a global external Application Load Balancer).

## License

Apache-2.0
