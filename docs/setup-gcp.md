# Setting up IAP for your MCP server

`iap-mcp-proxy` supports both IAP deployment modes. The mode determines what `--audience` you pass.

## Mode A — direct IAP on Cloud Run

Enable IAP directly on the Cloud Run service (no load balancer):

```sh
gcloud beta run services update MY-MCP-SERVICE \
  --region=REGION \
  --iap

gcloud beta iap web add-iam-policy-binding \
  --member="user:you@example.com" \
  --role="roles/iap.httpsResourceAccessor" \
  --region=REGION \
  --resource-type=cloud-run \
  --service=MY-MCP-SERVICE
```

`gcloud ... --iap` enables IAP with Google's **auto-managed OAuth client**. This client **rejects Google-issued OIDC ID tokens** (`adc`/`impersonate`/`oauth` modes) — every audience format returns `Invalid IAP credentials: Invalid JWT audience`. You must use a **self-signed service-account JWT** (`--credentials=signjwt`):

```sh
# Grant your ADC identity permission to sign as the SA, and the SA
# access to the IAP resource.
gcloud iam service-accounts add-iam-policy-binding \
  mcp-caller@PROJECT.iam.gserviceaccount.com \
  --member="user:you@example.com" \
  --role="roles/iam.serviceAccountTokenCreator"

gcloud beta iap web add-iam-policy-binding \
  --member="serviceAccount:mcp-caller@PROJECT.iam.gserviceaccount.com" \
  --role="roles/iap.httpsResourceAccessor" \
  --region=REGION --resource-type=cloud-run --service=MY-MCP-SERVICE

# Use the CANONICAL run.app URL (status.url), not the project-number URL.
CANONICAL_URL="$(gcloud run services describe MY-MCP-SERVICE \
  --region=REGION --format='value(status.url)')"

iap-mcp-proxy \
  --credentials=signjwt \
  --impersonate-service-account mcp-caller@PROJECT.iam.gserviceaccount.com \
  "${CANONICAL_URL}/mcp"
```

Audience: defaults to the canonical origin plus `/*` (e.g. `https://my-mcp-xxxx.a.run.app/*`). Origin-only and the project-number URL are **not** accepted; override `--audience` only to use the exact request path instead.

> Older direct-Cloud-Run IAP created with a **custom OAuth client** (before the OAuth Admin API shutdown) still takes OIDC ID tokens — use `--credentials=impersonate`/`adc` with the service URL as `--audience` there. New deployments cannot create such a client and must use `signjwt`.

## Mode B — IAP behind a global external Application Load Balancer

Classic backend-service IAP: Cloud Run (or GCE/GKE) behind a global external ALB with IAP enabled on the backend service.

1. Enable IAP on the backend service (Console: *Security → Identity-Aware Proxy*, or `gcloud iap web enable --resource-type=backend-services ...`). This creates/uses an OAuth client.
2. Find the IAP OAuth client ID (`NNN-xxxx.apps.googleusercontent.com`) under *APIs & Services → Credentials*.
3. Grant access:

   ```sh
   gcloud iap web add-iam-policy-binding \
     --member="user:you@example.com" \
     --role="roles/iap.httpsResourceAccessor" \
     --resource-type=backend-services \
     --service=MY-BACKEND-SERVICE
   ```

Audience: **the IAP OAuth client ID** — it must be passed explicitly:

```sh
iap-mcp-proxy --audience NNN-xxxx.apps.googleusercontent.com https://mcp.internal.example.com/mcp
```

## Creating a desktop OAuth client (for `--credentials=oauth`)

Needed only when your local credentials are gcloud *user* credentials (no service account, no impersonation). IAP's programmatic access requires an OAuth client in the same project as the IAP resource — see [Programmatic authentication](https://cloud.google.com/iap/docs/authentication-howto).

1. *APIs & Services → Credentials → Create credentials → OAuth client ID*, application type **Desktop app**, in the same project as the IAP resource.
2. Export its ID and secret where the proxy runs:

   ```sh
   export IAP_MCP_OAUTH_CLIENT_ID="NNN-yyyy.apps.googleusercontent.com"
   export IAP_MCP_OAUTH_CLIENT_SECRET="..."
   ```

3. First run opens a browser for sign-in; the refresh token is stored in the OS keychain and reused silently afterwards.

Depending on your IAP configuration you may need to allow the desktop client's ID as a valid programmatic audience — check the current IAP documentation, as this behavior has changed over time.

## Service-account impersonation (recommended for teams/CI)

```sh
gcloud iam service-accounts add-iam-policy-binding mcp-caller@PROJECT.iam.gserviceaccount.com \
  --member="user:you@example.com" \
  --role="roles/iam.serviceAccountTokenCreator"

iap-mcp-proxy \
  --impersonate-service-account mcp-caller@PROJECT.iam.gserviceaccount.com \
  --audience NNN-xxxx.apps.googleusercontent.com \
  https://mcp.internal.example.com/mcp
```

Grant `roles/iap.httpsResourceAccessor` to the *service account* on the IAP resource.

## Debugging

- Run with `--log-level=debug` (or `IAP_MCP_LOG=debug`); all logs go to stderr, so they show up in your MCP client's server logs without corrupting the protocol stream.
- IAP-generated responses carry the `x-goog-iap-generated-response: true` header; the proxy uses it to distinguish IAP errors from application errors.
- Test outside an MCP client:

  ```sh
  echo '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"probe","version":"0"}}}' \
    | iap-mcp-proxy --log-level=debug https://my-mcp-xxxx.a.run.app/mcp
  ```
