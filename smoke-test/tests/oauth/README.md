# Local OAuth smoke test (Keycloak + GMS)

Proves a DataHub SDK/CLI client authenticates to GMS using a real OAuth token
fetched from an OSS OIDC provider (Keycloak) — no cloud, no AKS/EKS required. It
exercises the SDK token-provider stack end to end (`oidc_client_credentials`
provider → `TokenProviderAuth` → GMS external-OAuth validation).

## What's here

- `keycloak/datahub-realm.json` — realm import with two confidential clients
  (client-credentials enabled): `datahub-executor` (audience `datahub-gms`,
  accepted) and `datahub-wrong-aud` (audience `not-datahub`, rejected).
- `docker-compose.oauth.yml` — overlay that adds Keycloak, configures GMS's
  `EXTERNAL_OAUTH_*` to trust the realm, and provides a `tester` runner.
- `test_oauth_cli_gms.py` — positive (valid audience → `me` query succeeds) and
  negative (wrong audience → rejected) tests.

## Why run from inside the compose network

GMS validates the token's `iss` claim and fetches the IdP's JWKS. The token's
issuer is fixed to `http://keycloak:8080/realms/datahub` (internal hostname). If
the client ran on the host (`localhost:8083`), GMS could not reach that issuer's
JWKS. Running the test from the `tester` service keeps the token endpoint, the
issuer, and the JWKS URL all on internal hostnames and consistent.

## Run

1. Start the standard DataHub quickstart (GMS + dependencies).
2. Apply this overlay:

   ```bash
   docker compose -f <quickstart-compose> -f smoke-test/tests/oauth/docker-compose.oauth.yml up -d
   ```

3. Wait for GMS health and the Keycloak realm import, then run the test from
   inside the network:

   ```bash
   docker compose -f <quickstart-compose> -f smoke-test/tests/oauth/docker-compose.oauth.yml \
     exec tester sh -c "pip install -e /repo/metadata-ingestion && pytest /smoke/tests/oauth/test_oauth_cli_gms.py -v"
   ```

   Expected: 2 passed.

## Deterministic variant (no IdP), for fast CI

If Keycloak proves flaky in CI, swap it for a fully deterministic setup: generate
an RSA keypair, serve a static `jwks.json` from a tiny container, mint a JWT with
`pyjwt` (chosen `iss`/`aud`/`sub`/`exp`), write it to a file, and use
`auth: {type: k8s_oidc, config: {token_file: ...}}` with GMS pointed at the static
issuer + JWKS. This removes IdP flakiness but does not exercise a token endpoint.
