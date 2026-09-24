- Start Date: 2026-04-10
- RFC PR: https://github.com/datahub-project/datahub/pull/16977
- Implementation PR(s):
  - https://github.com/datahub-project/datahub/pull/16652
  - https://github.com/acryldata/mcp-server-datahub/pull/109

# MCP Server Improvements: FastMCP v3, Token Optimization, and Enterprise SSO

## Summary

Two features we need for our DataHub MCP deployment, plus the framework
upgrade that both of them require:

1. **OIDC authentication with an On-Behalf-Of (OBO) token-exchange flow.**
   The MCP server accepts an OIDC token from the calling AI client,
   validates it, exchanges it for a DataHub-scoped token, and runs each
   request as the calling user. Without this, every DataHub mutation
   initiated through MCP is attributed to a shared service account, which
   defeats DataHub's audit trail and per-user policies in our environment.

   The only provider implemented today is **Microsoft Entra ID**, because
   that is the IdP our users authenticate against. The implementation is
   a single `TokenVerifier` subclass (`EntraOBOVerifier`) that composes
   FastMCP v3's shipped `AzureJWTVerifier` with an MSAL-backed OBO
   exchange. Adding Okta, Keycloak, Google, or Auth0 later means writing
   sibling `TokenVerifier` subclasses.

2. **Token-optimization transports** that cut how many tool schemas the
   agent has to hold in context:

   - **Tool Search** swaps the tool list for two synthetic tools
     (`search_tools`, `call_tool`) so the agent discovers schemas on
     demand. No sandbox, base install.
   - **Code Mode** swaps the tool list for (`search_tools`, `execute`)
     and lets the agent compose short Python snippets in a sandbox.

   Both target tool-catalog context bloat when DataHub is mounted
   alongside several other MCP servers. Opt-in and mutually exclusive.
   The benefit scales with the catalog size — the ~20 tools today are
   already significant, and the number grows each time a new DataHub
   capability surfaces through MCP.

Both features depend on primitives that only exist in **FastMCP v3**
(per-request dependency injection, the `CodeMode` transform,
multi-auth), so the RFC also includes a v3 upgrade as a prerequisite.

A working implementation already exists at
[`manuschillerdev/mcp-server-datahub`](https://github.com/manuschillerdev/mcp-server-datahub).

## Basic example

### 1. FastMCP v3 — idiomatic tool definition

```python
from fastmcp import FastMCP, Context
from fastmcp.exceptions import ToolError
from fastmcp.server.dependencies import Depends

mcp = FastMCP(
    "datahub",
    strict_input_validation=True,
    on_duplicate_tools="error",
)

@mcp.tool(
    annotations={"readOnlyHint": True, "idempotentHint": True},
    timeout_seconds=60,
)
async def search(
    query: str,
    *,
    ctx: Context,
    client: DataHubGraph = Depends(get_datahub_client),
) -> SearchResult:
    await ctx.report_progress(0, 1, "querying datahub")
    try:
        return await _search_implementation(client, query)
    except DataHubError as e:
        raise ToolError(f"DataHub search failed: {e}") from e
```

Key v3 affordances used here, none of which exist in v2:

- `ToolError` produces a structured, agent-friendly error instead of a Python
  traceback serialized into the response body.
- `Depends(get_datahub_client)` replaces the v2 pattern of a module-level
  global plus a threading lock. In OBO mode this is what resolves to the
  _per-user_ DataHub client on each request.
- `timeout_seconds` is enforced by the framework, so a single slow GraphQL
  query can no longer hang the entire server.
- `strict_input_validation=True` + `on_duplicate_tools="error"` catch schema
  drift and accidental double-registration at startup.
- `Context` lets us emit progress events and structured logs that clients
  like Claude Code surface inline.

Catalog metadata (entity types, platforms, supported filter keys) is exposed
as MCP _resources_ instead of being crammed into tool docstrings:

```python
@mcp.resource("datahub://catalog/entity-types")
def entity_types() -> list[str]:
    return SUPPORTED_ENTITY_TYPES
```

### 2. Token optimization

Two transforms are available. Both collapse the advertised tool catalog
down to two synthetic tools; the tool-schema footprint in the agent's
context shrinks from ~20 schemas (~8–12k tokens) to 2 meta-tools. They
differ in what the agent does next.

**Tool Search**:

```
$ mcp-server-datahub --transport http --tool-search
```

The agent sees two synthetic tools:

```
search_tools(query: str) -> list[ToolDescriptor]
call_tool(name: str, arguments: dict) -> any
```

The underlying DataHub tools remain callable through normal tool-calls;
only `list_tools` is trimmed. `call_tool` is a proxy for MCP clients
that only route through `list_tools` output.

**Code Mode**:

```
$ mcp-server-datahub --transport http --code-mode
```

The agent sees:

```
search_tools(query: str) -> list[ToolDescriptor]
execute(code: str, timeout_seconds: int = 30) -> ExecutionResult
```

Example agent turn:

```python
# code sent to execute()
hits = search(query="pet adoptions", filter="platform = snowflake")
urns = [h.urn for h in hits.results[:5]]
entities = get_entities(urns=urns, include=["properties", "schema"])
return {
    "candidates": [
        {"urn": e.urn, "fields": [f.name for f in e.schema.fields]}
        for e in entities
    ],
}
```

The sandbox (Monty) enforces a 30-second wall-clock timeout, ~50 MB
memory cap, and recursion depth 50. Only the DataHub tool surface is
exposed — no filesystem, no network, no `os`, no `subprocess`.

Tool Search keeps the agent in the normal one-tool-at-a-time loop. Code
Mode lets the agent fold a "search → rank → fetch details → build
answer" sequence into a single sandboxed `execute()` block.

### 3. OIDC On-Behalf-Of

Deployment runs behind an HTTP transport, reachable by an AI client that
authenticates the end user with an OIDC provider.

```bash
mcp-server-datahub --transport http
# env vars
DATAHUB_MCP_AUTH_ENABLED=true
MCP_OIDC_PROVIDER=entra
AZURE_TENANT_ID=<tenant-guid>
MCP_OAUTH_CLIENT_ID=<app-reg-client-id>
MCP_OAUTH_CLIENT_SECRET=<app-reg-secret>   # or use Managed Identity
DATAHUB_OAUTH_SCOPE=api://<datahub-app-id>/.default
MCP_SERVER_BASE_URL=https://mcp.example.com
```

Request flow:

1. GitHub Copilot / Claude / Copilot Studio authenticates the end user against
   Entra ID and calls the MCP server with the user's Entra access token as
   the `Authorization: Bearer …` header.
2. The server's `EntraOBOVerifier` validates the JWT signature, issuer,
   audience, expiry, and (optionally) required scopes.
3. The verifier calls MSAL `acquire_token_on_behalf_of(...)` to exchange
   the user JWT for a DataHub-scoped access token, which it stashes in
   `AccessToken.claims[DATAHUB_TOKEN_CLAIM]`.
4. `PerUserClientMiddleware` reads that claim and binds a user-scoped
   `DataHubClient` into a context-local variable for the duration of the
   request. `get_datahub_client()` (the DI provider from improvement #1)
   returns that client to any tool that asks for it.
5. All subsequent mutation tools — `add_tags`, `set_domains`,
   `update_description`, … — are attributed to the calling user in DataHub's
   audit log, not the service account.
6. If `MCP_SERVER_BASE_URL` is set, the server publishes
   `/.well-known/oauth-protected-resource` so MCP clients can auto-discover
   the authorization server.

A PAT-only fallback path is preserved through FastMCP's multi-auth: if the
incoming bearer token is not a valid JWT, the server falls back to verifying
it as a DataHub PAT against `/me`. This lets a single deployment serve both
Copilot-like clients (OBO) and script/CLI clients (PAT).

## Motivation

### Why FastMCP v3?

v3 supplies the two primitives the features above depend on:

- **Per-request dependency injection + multi-auth.** OBO needs to swap
  in a per-user DataHub client on each request. v2 has only a
  module-level client behind a threading lock; v3's `Depends(...)` and
  multi-`TokenVerifier` chains are what make per-user auth clean.
- **`CodeMode` and Tool Search transforms.** Token optimization is a
  v3-only feature surface; neither transform exists in v2 and they
  are not going to be backported.

The upgrade also retires a few v2 workarounds the server has
accumulated (bare `ValueError` / `RuntimeError` raising, no per-tool
timeouts, catalog metadata inlined into every tool docstring), but
those are cleanup — not the reason for the upgrade.

### Why token optimization?

A fully configured DataHub MCP server currently ships ~20 tools. For an
agent running Claude, each tool schema costs ~300–700 tokens in the
system prompt, which means the DataHub tool catalog alone consumes
8–12k tokens _before_ the user asks a question. On smaller context
windows, or when several MCP servers are mounted at once, this is
prohibitive — and the catalog keeps growing as new DataHub capabilities
surface through MCP.

Both Tool Search and Code Mode collapse that catalog to two synthetic
tools and let the agent discover the rest on demand. They differ in
what the agent does next: Tool Search keeps the agent in the normal
one-tool-at-a-time loop, so no sandbox runtime is needed. Code Mode
lets the agent compose a "search → rank → fetch details → build
answer" sequence in one sandboxed `execute()` block, which in our
testing drops a typical multi-step turn from 4 MCP tool calls (each
paying full serialization overhead) to one.

### Why OIDC + OBO?

The upstream server has one authentication story: a single DataHub PAT,
shared by every user of the server. That is acceptable for a personal
stdio-mode deployment but not for a multi-tenant HTTP deployment where:

- Mutations must be attributed to the real user for audit & compliance.
- The AI client (GitHub Copilot, Copilot Studio, a corporate chatbot) already
  holds an OIDC token for the user — forcing the user to _also_ manage a
  DataHub PAT negates the SSO experience.
- Fine-grained DataHub policies (per-domain, per-platform) cannot apply if
  everything runs as the service account.

OBO is the standard OAuth 2.0 pattern for this: the intermediate service (us)
exchanges the caller's user token for a token scoped to the downstream API
(DataHub).

## Requirements

### FastMCP v3 upgrade

- The server MUST run on FastMCP ≥ 3.0.
- The DataHub client MUST be resolved via a DI provider so OBO can swap
  in a per-user client without a threading lock.
- All tool errors MUST use `ToolError` (or subclasses), never bare
  `ValueError` / `RuntimeError`.
- Every tool MUST declare a timeout. Defaults: read-only tools 60 s,
  mutations 30 s.
- Catalog metadata (entity types, platforms, filter grammar) MUST be
  exposed as MCP resources in addition to, or in place of, tool
  docstring padding.
- DataHub client bootstrap and shutdown MUST run inside a FastMCP
  lifespan hook; nothing that can fail at runtime may execute at
  import time.
- The server MUST start with `strict_input_validation=True` and
  `on_duplicate_tools="error"`.
- The upgrade MUST be a no-op from the agent's point of view: tool
  names, parameters, and response shapes stay identical.

### Token optimization

Tool Search and Code Mode both exist to reduce the tool-schema footprint
in the agent's context. Both are opt-in, mutually exclusive, and
orthogonal to authentication (OBO/PAT work with either). The default
tool-list transport is unchanged.

**Tool Search:**

- MUST be opt-in via `--tool-search` CLI flag or
  `DATAHUB_MCP_TOOL_SEARCH=true` env var.
- When enabled, the server MUST expose the two synthetic tools defined
  by FastMCP's search transform (`search_tools`, `call_tool`). The
  underlying tools remain callable via direct tool-call; only
  `list_tools` output is trimmed.
- Search strategy MUST be configurable via
  `DATAHUB_MCP_TOOL_SEARCH_STRATEGY` (`bm25` | `regex`, default `bm25`).
- Results MUST pass through the normal middleware / authorization
  pipeline, so a tool the caller is not allowed to invoke does not
  appear in search results.
- MUST ship in the base install — no extras group, no new runtime
  dependency.

**Code Mode:**

- MUST be opt-in via `--code-mode` CLI flag or
  `DATAHUB_MCP_CODE_MODE=true` env var.
- When enabled, the server MUST expose exactly the meta-tools defined
  by FastMCP's `CodeMode` transform (`search_tools`, `execute`).
- Sandbox limits MUST be enforced: wall-clock timeout (default 30 s),
  memory cap (~50 MB), recursion depth (50). Values MUST be overridable
  via env vars.
- The sandbox MUST NOT expose filesystem, network, `os`, `subprocess`,
  or any module beyond the DataHub tool surface.
- The `code-mode` dependency group MUST be optional so that operators
  who do not enable Code Mode do not pay the install cost of the
  sandbox runtime.

### OIDC + OBO

- The implementation MUST extend FastMCP v3's shipped primitives
  (`TokenVerifier`, `AzureJWTVerifier`, `RemoteAuthProvider`,
  `Middleware`) rather than defining a parallel auth framework.
- JWT validation (signature via JWKS, issuer, audience, expiry,
  optional `required_scopes`) MUST be delegated to FastMCP's
  `AzureJWTVerifier` — not reimplemented.
- JWKS caching MUST come from FastMCP's verifier as well (we do not
  manage a cache ourselves).
- The Entra OBO exchange MUST use MSAL's
  `ConfidentialClientApplication.acquire_token_on_behalf_of` —
  the same call pattern used by Microsoft's own published reference
  ([Pamela Fox, _Using on-behalf-of flow for Entra-based MCP servers_](https://blog.pamelafox.org/2026/01/using-on-behalf-of-flow-for-entra-based.html))
  and documented in Microsoft's OAuth 2.0 OBO protocol reference.
- The exchanged DataHub token MUST be written into
  `AccessToken.claims["datahub_token"]` so that a single
  `PerUserClientMiddleware` can install a per-request `DataHubClient`
  without touching any tool definitions.
- Multi-auth MUST be supported: if OIDC/OBO is configured and the
  incoming bearer token is not a valid Entra JWT, the server MUST fall
  back to DataHub PAT verification. This is what allows one deployment
  to serve both Copilot-style clients and CLI/script clients.
- When `MCP_SERVER_BASE_URL` is set, the server MUST publish
  `/.well-known/oauth-protected-resource` by wrapping the verifier in
  FastMCP's `RemoteAuthProvider` (no custom discovery code).
- When no OIDC env vars are set, the server MUST behave exactly as it
  does today — this is a pure additive change for existing deployments.
- STDIO transport is out of scope: it continues to use the
  service-account client unconditionally, since there is no HTTP
  request to attach a token to.

### Extensibility

- The extension point for additional IdPs is **FastMCP's
  `TokenVerifier`** — not a bespoke base class in this repo. Adding an
  Okta / Keycloak / Auth0 / Google provider means either (a) reusing
  FastMCP's corresponding verifier if it ships one (as we do for
  `AzureJWTVerifier`) or (b) writing a new `TokenVerifier` subclass
  that composes that IdP's JWT verification with its OBO exchange.
  Either way, no changes to the server core are needed.
- Code Mode's sandbox is extensible via FastMCP's `CodeMode` transform —
  as new DataHub tools are added, they automatically appear inside the
  sandbox without additional wiring.

## Non-Requirements

- **Replacing the default tool-list transport.** Tool Search and Code
  Mode are both additive and opt-in; the default transport is unchanged.
- **Token caching / refresh tokens across requests.** Each request does
  an OBO exchange. MSAL's in-process cache is sufficient for our scale;
  distributed caching is left for a future RFC.
- **Authorization beyond authentication.** This RFC ensures the _caller_
  is identified and a DataHub client is created on their behalf.
  Authorization decisions (which tags can this user set? which datasets
  can they see?) are already enforced by DataHub itself and are
  intentionally not duplicated in the MCP server.

## Detailed design

### 1. FastMCP v3 upgrade

#### 1.1 Dependency and compatibility

- Bump `fastmcp` to `>=3.0,<4`.
- Drop the v2 compat shims in `mcp_server.py`: module-level `_datahub_client`
  global, threading lock, duplicate-registration guards.
- Keep the file structure that already exists upstream (`tools/search.py`,
  `tools/entities.py`, …). The refactor is additive.

#### 1.2 Dependency injection

A single `ContextVar` plus a context-manager binder plus a DI provider:

```python
# dependencies.py
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Iterator

_datahub_client: ContextVar[DataHubClient | None] = ContextVar(
    "datahub_client", default=None
)


@contextmanager
def with_datahub_client(client: DataHubClient) -> Iterator[None]:
    token = _datahub_client.set(client)
    try:
        yield
    finally:
        _datahub_client.reset(token)


def get_datahub_client() -> DataHubClient:
    """Return the DataHub client for the current request.

    Resolution order:
      1. Per-request client bound by `PerUserClientMiddleware` (OBO or PAT).
      2. Service-account client (the only path for STDIO).
    """
    per_request = _datahub_client.get()
    if per_request is not None:
        return per_request
    return _service_account_client
```

Every tool that talks to DataHub takes
`client: DataHubClient = Depends(get_datahub_client)` instead of reading a
module-level global. The same `with_datahub_client(...)` context manager
that `PerUserClientMiddleware` uses (§3.3) also drives test fixtures that
swap in a mock client.

#### 1.3 Errors

A small adapter wraps DataHub exceptions:

```python
def _to_tool_error(exc: Exception) -> ToolError:
    if isinstance(exc, GmsUnauthorizedError):
        return ToolError("Not authorized in DataHub", code="unauthorized")
    if isinstance(exc, GmsNotFoundError):
        return ToolError(str(exc), code="not_found")
    return ToolError(f"DataHub error: {exc}", code="internal")
```

All `tools/*.py` modules use this adapter in their `except` blocks. No bare
`raise` of arbitrary Python exceptions.

#### 1.4 Timeouts

Two presets, applied via the `@mcp.tool` decorator:

| Preset   | Timeout | Applies to                                         |
| -------- | ------- | -------------------------------------------------- |
| `READ`   | 60 s    | `search`, `get_entities`, `get_lineage`, …         |
| `MUTATE` | 30 s    | `add_tags`, `set_domains`, `update_description`, … |

Tunable via `TOOL_READ_TIMEOUT_SECONDS` / `TOOL_MUTATE_TIMEOUT_SECONDS` env
vars for operators with unusually slow DataHub backends.

#### 1.5 Resources

Three initial resources:

| URI                                | Content                                          |
| ---------------------------------- | ------------------------------------------------ |
| `datahub://catalog/entity-types`   | JSON list of supported entity types              |
| `datahub://catalog/platforms`      | JSON list of known platforms in this DataHub     |
| `datahub://catalog/filter-grammar` | The SQL-like filter grammar reference (markdown) |

The filter grammar resource replaces ~1 KB of boilerplate currently inlined
into every search-related tool's docstring. Clients that don't read MCP
resources are unaffected — tool docstrings still contain a short pointer.

#### 1.6 Lifespans

The upstream server initialises the DataHub client at module load. v3's
lifespan hook replaces that with explicit startup / shutdown:

```python
@asynccontextmanager
async def lifespan(mcp: FastMCP) -> AsyncIterator[None]:
    _init_service_account_client()  # fails fast if GMS is unreachable
    try:
        yield
    finally:
        await _close_clients()
```

Bootstrap errors now surface at server-start rather than on the first
tool call. This is also where OBO's MSAL `ConfidentialClientApplication`
is instantiated once, rather than per-request.

### 2. Token optimization

Both transforms target the same problem: the tool-schema footprint the
agent has to hold in context. They differ in what the agent does after
discovering a tool.

#### 2.1 Tool Search

Tool Search replaces the advertised tool list with two synthetic tools:

- `search_tools(query)` returns the top-N matching tool descriptors.
- `call_tool(name, arguments)` is a proxy that invokes a matched tool.

The underlying tools remain callable via the normal tool-call path;
only `list_tools` is trimmed. The `call_tool` proxy exists for MCP
clients that only route through `list_tools` output.

```python
# __main__.py
if args.tool_search or get_boolean_env_variable("DATAHUB_MCP_TOOL_SEARCH"):
    strategy = os.environ.get("DATAHUB_MCP_TOOL_SEARCH_STRATEGY", "bm25")
    if strategy == "regex":
        from fastmcp.server.transforms.search import RegexSearchTransform
        transform = RegexSearchTransform(
            max_results=_env_int("DATAHUB_MCP_TOOL_SEARCH_MAX_RESULTS", 5),
        )
    else:
        from fastmcp.server.transforms.search import BM25SearchTransform
        transform = BM25SearchTransform(
            max_results=_env_int("DATAHUB_MCP_TOOL_SEARCH_MAX_RESULTS", 5),
        )
    mcp.add_transform(transform)
```

Authorization, middleware, and visibility rules apply at search time:
`search_tools` runs `list_tools()` through the full pipeline, so a tool
the caller is not allowed to call does not appear in its results.

#### 2.2 Code Mode — transport activation

```python
# __main__.py
if args.code_mode or get_boolean_env_variable("DATAHUB_MCP_CODE_MODE"):
    from fastmcp.contrib.code_mode import CodeMode
    mcp = CodeMode(
        mcp,
        sandbox="monty",
        default_timeout_seconds=_env_int("CODE_MODE_TIMEOUT_SECONDS", 30),
        memory_limit_mb=_env_int("CODE_MODE_MEMORY_MB", 50),
        max_recursion_depth=_env_int("CODE_MODE_MAX_RECURSION", 50),
    )
```

`CodeMode` is a FastMCP v3 transform: it wraps an existing `FastMCP` server
and rewrites the exposed tool list to `(search_tools, execute)`. The
underlying tools are not removed — they are registered inside the sandbox's
Python namespace and callable as plain functions from agent-supplied code.

#### 2.3 Code Mode — sandbox

Monty is a constrained Python subset (bytecode-level sandbox, no `eval`,
no imports beyond an allowlist, no syscall access). The DataHub tools are
injected into the namespace as callables that dispatch back through the
FastMCP tool registry, which in turn runs each call through the normal
middleware stack — so authentication, logging, and per-user client
resolution all still apply inside Code Mode. There is no auth bypass.

#### 2.4 Code Mode — packaging

Code Mode ships as an optional dependency group:

```toml
[project.optional-dependencies]
code-mode = ["fastmcp[code-mode]>=3.0"]
```

Users who never enable Code Mode never install the sandbox runtime, keeping
`uv pip install mcp-server-datahub` lean.

#### 2.5 Code Mode — operational concerns

- **Observability:** each `execute()` call is logged as a single MCP
  request with a `code_mode=true` tag. Tool calls _inside_ the sandbox
  are logged as child spans so existing dashboards still show per-tool
  metrics.
- **Timeout composition:** a tool invoked from inside the sandbox is
  still subject to its own `@mcp.tool(timeout_seconds=...)`. The
  sandbox's `default_timeout_seconds` bounds the _total_ wall-clock of
  the enclosing `execute()` call across every tool invocation in it.
- **Error surface:** sandbox exceptions bubble up as `ToolError` with
  `code="sandbox_error"`.

### 3. OIDC On-Behalf-Of

#### 3.0 Building on FastMCP v3's shipped primitives

The design below is **not a custom OAuth framework**. It reuses four
classes that ship with FastMCP v3 (`fastmcp[azure]`) as-is and adds one
thin composition class. The shipped primitives are:

| Class                | Source module                         | What we use it for                                    |
| -------------------- | ------------------------------------- | ----------------------------------------------------- |
| `TokenVerifier`      | `fastmcp.server.auth.auth`            | Base class for our custom verifiers                   |
| `AccessToken`        | `fastmcp.server.auth.auth`            | Result type returned by `verify_token`                |
| `RemoteAuthProvider` | `fastmcp.server.auth.auth`            | Wraps the verifier to publish `.well-known` metadata  |
| `AzureJWTVerifier`   | `fastmcp.server.auth.providers.azure` | JWT signature / issuer / audience / expiry validation |
| `Middleware`         | `fastmcp.server.middleware`           | Base class for `PerUserClientMiddleware`              |

We deliberately do **not** use FastMCP's `EntraOBOToken` dependency.
`EntraOBOToken` is designed for the per-tool-call case (a single tool
needs to call, e.g., Microsoft Graph). In our case _every_ DataHub tool
talks to GMS with the user-scoped token, so it is much cleaner to do
the OBO exchange once at auth-verification time and stash the exchanged
DataHub token in `AccessToken.claims["datahub_token"]` — the per-request
middleware then resolves the user-scoped `DataHubClient` without any
tool-level plumbing.

Prior art for the overall pattern:

- **FastMCP itself** documents Azure/Entra integration and ships
  `AzureProvider`, `AzureJWTVerifier`, `RemoteAuthProvider`, and
  `EntraOBOToken` as first-class features — see the
  [FastMCP Azure integration docs](https://gofastmcp.com/integrations/azure).
- **Pamela Fox (Microsoft)** published a working reference
  implementation of an Entra-authenticated MCP server that performs OBO
  via MSAL's `acquire_token_on_behalf_of`, which is the exact call
  pattern used below —
  [_Using on-behalf-of flow for Entra-based MCP servers_](https://blog.pamelafox.org/2026/01/using-on-behalf-of-flow-for-entra-based.html).
- **Microsoft's OAuth 2.0 On-Behalf-Of protocol reference** is the
  canonical spec for the exchange —
  [_Microsoft identity platform and OAuth 2.0 On-Behalf-Of flow_](https://learn.microsoft.com/en-us/entra/identity-platform/v2-oauth2-on-behalf-of-flow).

So the OBO pattern proposed here is a documented, supported, and
already-implemented-by-others primitive — not a design we invented.

#### 3.1 `EntraOBOVerifier`

A single `TokenVerifier` subclass composes FastMCP's `AzureJWTVerifier`
(for the validation half) with an MSAL-backed token exchanger (for the
OBO half), and writes the exchanged DataHub token into
`AccessToken.claims["datahub_token"]`:

```python
# _auth_obo.py (abridged)
from fastmcp.server.auth.auth import AccessToken, TokenVerifier
from fastmcp.server.auth.providers.azure import AzureJWTVerifier

# Custom claim keys, referenced by both the verifier and the middleware.
DATAHUB_TOKEN_CLAIM = "datahub_token"
AUTH_METHOD_CLAIM = "auth_method"


class EntraOBOVerifier(TokenVerifier):
    def __init__(self, config: OBOConfig) -> None:
        super().__init__(
            base_url=config.base_url,
            required_scopes=config.required_scopes,
        )
        self._jwt_verifier = AzureJWTVerifier(
            client_id=config.client_id,
            tenant_id=config.tenant_id,
            required_scopes=config.required_scopes,
            jwks_cache_ttl_seconds=config.jwks_cache_ttl_seconds,
        )
        self._exchanger = OBOTokenExchanger(
            tenant_id=config.tenant_id,
            client_id=config.client_id,
            client_secret=config.client_secret,
            datahub_scope=config.datahub_scope,
        )

    async def verify_token(self, token: str) -> AccessToken | None:
        # 1. Validate the Entra JWT using FastMCP's built-in verifier.
        access_token = await self._jwt_verifier.verify_token(token)
        if access_token is None:
            return None

        # 2. Exchange the user assertion for a DataHub-scoped token via OBO.
        try:
            datahub_token = await asyncio.to_thread(
                self._exchanger.exchange, token
            )
        except Exception as exc:
            # Never log `exc_info=True`: MSAL exceptions can embed the
            # user assertion in their context. Log the exception type and
            # sanitized claim identifiers only.
            logger.warning(
                "OBO token exchange failed: %s",
                type(exc).__name__,
                extra={
                    "sub": access_token.claims.get("sub"),
                    "aud": access_token.claims.get("aud"),
                    "tid": access_token.claims.get("tid"),
                },
            )
            return None

        # 3. Return an AccessToken carrying the exchanged DataHub token.
        claims = dict(access_token.claims)
        claims[DATAHUB_TOKEN_CLAIM] = datahub_token
        claims[AUTH_METHOD_CLAIM] = "entra_obo"
        return AccessToken(
            token=datahub_token,
            client_id=access_token.client_id,
            scopes=access_token.scopes,
            expires_at=access_token.expires_at,
            claims=claims,
        )
```

A few design points worth calling out:

- `DATAHUB_TOKEN_CLAIM` and `AUTH_METHOD_CLAIM` are module-level
  constants, not inline string literals. A misspelled key would fail
  silently at runtime; constants force the misspelling to surface at
  import time and keep the verifier and the middleware in sync.
- The custom claim keys piggyback on `AccessToken.claims` rather than
  introducing a parallel carrier. Consumers reading `claims` need to
  know these keys exist, which is why they are defined in one place and
  referenced from both §3.1 and §3.3.
- Exception handling is `except Exception`, not `except RuntimeError`:
  MSAL surfaces `ValueError`, network errors, and token-validation
  errors that do not all inherit from `RuntimeError`. A narrower
  `except` would let real failures crash the verifier.

`OBOTokenExchanger` is a ~40-line wrapper around
`msal.ConfidentialClientApplication.acquire_token_on_behalf_of`. This
MSAL call performs the RFC-compliant OBO exchange documented in
Microsoft's OAuth 2.0 OBO reference.

There is **no abstract `OIDCOboProvider` base class**. If/when we want
to support a non-Azure IdP (Okta, Keycloak, …), one of two things
happens:

1. FastMCP ships a provider for it (e.g. if FastMCP adds `OktaProvider`
   we reuse it the same way we reuse `AzureJWTVerifier` here), or
2. We add a second `TokenVerifier` subclass analogous to
   `EntraOBOVerifier`, wiring that IdP's JWT verifier to its OBO
   equivalent.

Either way, the extension point is already FastMCP's `TokenVerifier`
interface. We do not need our own abstraction.

#### 3.2 `.well-known/oauth-protected-resource` discovery

When `MCP_SERVER_BASE_URL` is set, the verifier is wrapped in FastMCP's
`RemoteAuthProvider`, which publishes the standard discovery metadata so
that MCP clients like GitHub Copilot can auto-discover the authorization
server:

```python
# _auth_obo.py (factory)
from fastmcp.server.auth.auth import RemoteAuthProvider

def build_obo_auth(config: OBOConfig):
    verifier = EntraOBOVerifier(config)
    if config.base_url:
        return RemoteAuthProvider(
            token_verifier=verifier,
            authorization_servers=[
                AnyHttpUrl(
                    f"https://login.microsoftonline.com/{config.tenant_id}/v2.0"
                )
            ],
            base_url=config.base_url,
            resource_name="DataHub MCP Server",
        )
    return verifier
```

Resulting discovery document:

```json
{
  "resource": "https://mcp.example.com",
  "authorization_servers": ["https://login.microsoftonline.com/<tenant>/v2.0"],
  "bearer_methods_supported": ["header"],
  "scopes_supported": ["api://<datahub-app-id>/.default"]
}
```

#### 3.3 `PerUserClientMiddleware`

A thin subclass of FastMCP's `Middleware` reads the verified
`AccessToken` from the auth context, picks up the exchanged DataHub
token out of `claims[DATAHUB_TOKEN_CLAIM]`, and binds a per-request
`DataHubClient` into the context-local variable defined in §1.2 via the
`with_datahub_client` context manager:

```python
# _auth.py (abridged)
from fastmcp.server.middleware import CallNext, Middleware
from mcp.server.auth.middleware.auth_context import get_access_token

from ._auth_obo import DATAHUB_TOKEN_CLAIM


class PerUserClientMiddleware(Middleware):
    def __init__(self, gms_url: str) -> None:
        self._gms_url = gms_url.rstrip("/")

    async def on_message(self, context, call_next: CallNext):
        access_token = get_access_token()
        datahub_token = (
            access_token.claims.get(DATAHUB_TOKEN_CLAIM)
            if access_token
            else None
        )
        if datahub_token:
            user_client = DataHubClient(
                config=DatahubClientConfig(
                    server=self._gms_url,
                    token=datahub_token,
                    client_mode=ClientMode.SDK,
                ),
                datahub_component=f"mcp-server-datahub/{__version__}",
            )
            with with_datahub_client(user_client):
                return await call_next(context)
        return await call_next(context)
```

When no access token is present (STDIO transport, or auth disabled),
the middleware is a no-op and the existing service-account client is
used.

#### 3.4 Multi-auth fallback (OBO + PAT)

A single deployment can serve both Copilot-style clients (presenting an
Entra JWT) and script/CLI clients (presenting a DataHub PAT) by chaining
two `TokenVerifier`s — the `EntraOBOVerifier` first, with a
`DataHubTokenVerifier` (which calls GMS's `me` query) as the fallback.
FastMCP's multi-auth support picks the first verifier that returns a
non-`None` `AccessToken`; the OBO verifier rejects non-JWT tokens
cheaply (local signature check, no network call), so the PAT path does
not pay an OBO penalty.

When both verifiers reject the token, FastMCP's auth middleware returns
HTTP 401 with a minimal body and no Python traceback. There is no
silent fall-through to the service-account client — that would mask a
broken client configuration and risk privilege escalation.

#### 3.5 Security considerations

- JWKS keys are cached with a default TTL of 1 hour, refreshed on `kid`
  miss to handle key rotation without restart. Tunable via
  `DATAHUB_MCP_JWKS_CACHE_TTL_SECONDS` for operators in high-rotation
  environments.
- Client secrets should be provisioned via Entra Managed Identity where
  available. When `MCP_OAUTH_CLIENT_SECRET` is used directly, the
  server reads it once at lifespan startup and has to be restarted to
  pick up a rotated secret — operators should run behind a secret
  manager that restarts the process on rotation.
- The server never logs client secrets, bearer tokens, or `exc_info` on
  OBO-exchange failures (MSAL exceptions can embed the user assertion in
  their context).
- All auth failures log `sub`, `aud`, `tid`, and a sanitized reason —
  never the token itself.
- OBO-issued DataHub tokens are held only for the lifetime of a single
  request. They are not written to disk, not cached across requests, and
  not attached to any logging record.
- If OBO exchange fails, the server returns `401` — it does NOT fall
  back to the service account. Silent privilege escalation would be a
  security bug.

#### 3.6 Environment variable reference

Variables added or newly referenced by this RFC. Existing upstream
variables (`DATAHUB_GMS_URL`, `DATAHUB_GMS_TOKEN`, etc.) are unchanged
and omitted here.

| Variable                              | Default | Purpose                                                 |
| ------------------------------------- | ------- | ------------------------------------------------------- |
| `DATAHUB_MCP_AUTH_ENABLED`            | false   | Turn on OIDC/OBO verification                           |
| `DATAHUB_MCP_CODE_MODE`               | false   | Enable Code Mode transport                              |
| `DATAHUB_MCP_TOOL_SEARCH`             | false   | Enable Tool Search transform                            |
| `DATAHUB_MCP_TOOL_SEARCH_STRATEGY`    | bm25    | `bm25` (relevance-ranked) or `regex` (pattern match)    |
| `DATAHUB_MCP_TOOL_SEARCH_MAX_RESULTS` | 5       | Max tool descriptors returned by `search_tools`         |
| `DATAHUB_MCP_JWKS_CACHE_TTL_SECONDS`  | 3600    | JWKS cache TTL                                          |
| `TOOL_READ_TIMEOUT_SECONDS`           | 60      | Read-tool timeout                                       |
| `TOOL_MUTATE_TIMEOUT_SECONDS`         | 30      | Mutation-tool timeout                                   |
| `CODE_MODE_TIMEOUT_SECONDS`           | 30      | Sandbox wall-clock (bounds the whole `execute()` call)  |
| `CODE_MODE_MEMORY_MB`                 | 50      | Sandbox memory cap                                      |
| `CODE_MODE_MAX_RECURSION`             | 50      | Sandbox recursion depth                                 |
| `AZURE_TENANT_ID`                     | —       | Entra tenant (MSAL-native)                              |
| `MCP_OAUTH_CLIENT_ID`                 | —       | Entra app-registration client id                        |
| `MCP_OAUTH_CLIENT_SECRET`             | —       | Entra app-registration secret (prefer Managed Identity) |
| `DATAHUB_OAUTH_SCOPE`                 | —       | Scope requested for the DataHub-scoped token            |
| `MCP_SERVER_BASE_URL`                 | —       | Public URL for `.well-known` discovery                  |

Everything with a default of `false` or unset is a pure additive knob —
an existing PAT-only deployment can run 0.7.0 with no config changes.

## How we teach this

New sections in the MCP server README:

1. **Authentication** (rewritten): Service Account → DataHub PAT →
   OIDC/OBO → Multi-auth, in order of sophistication.
2. **Token optimization**: one page covering Tool Search and Code Mode
   with a worked example of each and the Code Mode sandbox limits table.
3. **Provider Reference**: one subsection per OIDC provider with the
   exact env vars. Starts with Entra; placeholders invite community
   PRs for Okta, Keycloak, etc.

DataHub's existing MCP feature guide
(`docs.datahub.com/docs/features/feature-guides/mcp`) gets two new
links: "Running behind an enterprise SSO" and "Reducing context usage
with Tool Search / Code Mode".

## Drawbacks

- **Dependency surface grows.** MSAL and the Monty sandbox (optional) are
  new dependencies. MSAL in particular pulls in cryptography transitive
  deps. Mitigation: Code Mode is opt-in via an extras group; MSAL only loads
  if OIDC env vars are set.
- **FastMCP v3 is a major upgrade.** Some downstream forks or private
  plugins pinned to v2 will need to move. Mitigation: v3 is the active
  upstream branch and v2 is in maintenance; this is a delay, not a
  different direction.
- **Code Mode's sandbox is not a security boundary against malicious code
  the agent chooses to run.** It is a resource boundary: CPU/memory/time,
  not a confinement from, e.g., data exfiltration through legitimate
  DataHub calls. Operators enabling Code Mode need to understand this.
- **The first OIDC provider is Entra-only.** Until at least one additional
  provider lands, there is a risk the abstraction calcifies around Entra's
  quirks. Mitigation: the extension point is FastMCP's `TokenVerifier`,
  not a bespoke base class, and MSAL-specific types are confined to
  `EntraOBOVerifier` — a sibling `OktaOBOVerifier` would compose a
  different IdP verifier with a different OBO exchanger behind the same
  interface.
- **More env vars.** The configuration matrix roughly doubles. Mitigation:
  everything defaults off; a PAT-only deployment is unchanged.

## Alternatives

### Ship from a downstream Entra fork instead of upstreaming

- **Upstream (this RFC).** OBO, Tool Search, and Code Mode land in
  `acryldata/mcp-server-datahub`. Future OIDC providers plug in as
  sibling `TokenVerifier` subclasses.
- **Ship from a downstream Entra fork of
  `acryldata/mcp-server-datahub`.** Keeps local changes decoupled from
  upstream release cadence, at the cost of rebasing on every upstream
  release and the community never getting these features from the
  fork.

### Technical alternative to OIDC OBO — pass-through user PATs

Instead of validating an OIDC token and exchanging it for a DataHub token
via OBO, we could require every end user to create a personal DataHub PAT
and configure their MCP client to send it as the bearer token. The MCP
server would forward this token verbatim to DataHub.

- **Pros.** No OIDC integration code, no MSAL dependency, no token
  exchange. The MCP server stays close to what it is today.
- **Cons.** Terrible UX for our users: every user has to log in to
  DataHub, generate a PAT, copy it into their agent client, and rotate
  it manually. That is exactly the SSO bypass we are trying to avoid —
  the user already has a valid Entra token through their normal
  corporate login; forcing them to also manage a DataHub PAT negates
  the single-sign-on experience and adds a per-user credential store to
  operate. Rejected on UX grounds, not technical ones.
- **Variant: user PAT configured once on the server** (the status quo
  service-account model). Fails the audit and per-user-policy
  requirements — every mutation is attributed to the service account.

## Rollout / Adoption Strategy

### Release plan

1. **0.7.0 — FastMCP v3 upgrade.** Covers §1: DI, `ToolError`, per-tool
   timeouts, resources, lifespans. No agent-visible behaviour change.
   Shipped first so v3 regressions can be isolated from the auth and
   token-optimization changes.
2. **0.8.0 — OIDC OBO (Entra).** Additive per §3. Default behaviour
   unchanged (no env vars set → pure PAT). Operators opt in by setting
   `DATAHUB_MCP_AUTH_ENABLED=true` and the Entra env vars.
3. **0.9.0 — Token-optimization transports.** Tool Search (§2.1) in
   the base install, Code Mode (§2.2–§2.5) behind the `code-mode`
   extras group so the Monty sandbox runtime stays optional. Both are
   opt-in and mutually exclusive.

Each release ships with the previous release's deployment guide still
valid. There is no flag day.

### Migration notes

- **Existing stdio users:** no change. They do not see any new env vars.
- **Existing HTTP-transport users with a service-account PAT:** no change.
  They run 0.7.0 with zero config changes.
- **New HTTP deployments that want OBO:** set the four Entra env vars and
  flip `DATAHUB_MCP_AUTH_ENABLED=true`. If the Entra app registration is
  misconfigured, valid JWTs will fail exchange and callers receive 401;
  PAT clients on the same deployment are unaffected because the PAT
  verifier is a separate chain step.
- **Code Mode users:** install with `uv pip install "mcp-server-datahub[code-mode]"`
  and pass `--code-mode`.
- **Tool Search users:** no extras needed; pass `--tool-search`.

### Backwards compatibility

- MCP tool names, parameters, and response shapes are unchanged.
- Environment variables are all additive; none are renamed or removed.
- CLI: one new flag (`--code-mode`). Existing `--transport` and `--debug`
  flags are unchanged.
- Python API (for embedders): the DI hook is additive. Embedders that
  previously set a module-level client can keep doing so; the fallback path
  preserves that behaviour.

## Future Work

- **Additional OIDC providers:** Okta, Keycloak, Auth0, Google. Each is
  a sibling `TokenVerifier` subclass composing whichever FastMCP
  verifier ships for that IdP with its OBO exchange (see §3.1).
- **Cursor-based result pagination** for list-returning tools. DataHub
  GraphQL already exposes cursors (`scrollId` on `scrollAcrossEntities`,
  etc.); current tools use a mix of `count` / `start` / neither. A
  standard `PaginatedResult[T]` + `next_cursor` contract across every
  list-returning tool removes the parameter drift and lets agents page
  through large result sets without blowing their context budget.
- **Operational middleware** — FastMCP v3 ships rate-limiting,
  response-size-limiting, and authorization middleware. Wiring these
  in gives per-user quotas keyed on the OIDC `sub` claim, a hard cap
  on oversized payloads, and a `DATAHUB_MCP_READ_ONLY` deployment mode
  that refuses mutation tools at the middleware layer.
- **OpenTelemetry instrumentation** — FastMCP v3 ships native OTel.
  When enabled, spans cover every tool call, middleware hop, and
  sandbox invocation; attributes can include `auth.method` and
  (opt-in) `user.sub` for a per-user audit trail in an OTel backend.
- **Tool surface strategy for the full GraphQL API** — today the
  server ships ~20 hand-written tools, each a thin GraphQL wrapper,
  leaving large parts of the GraphQL API unreachable. Options to
  cover the rest without per-endpoint code: a `graphql(query,
variables)` escape-hatch tool (mirrors the
  [`blurrah/mcp-graphql`](https://github.com/blurrah/mcp-graphql)
  pattern, mutations off by default); persisted-query tools sourced
  from `datahub-web-react/src/graphql/*.graphql`
  ([Apollo MCP Server](https://www.apollographql.com/docs/apollo-mcp-server)
  is the mature reference); or auto-generated tools via FastMCP v3's
  OpenAPI Provider against GMS's `/openapi/v3/` surface.
- **Background Tasks for long-running traversals.** FastMCP v3 ships a
  background-task primitive that runs an operation asynchronously and
  lets the agent poll for progress. Fits multi-hop lineage traversal
  and bulk tag operations better than streaming, because many MCP
  clients do not render streamed tool output.
- **Approval gates on destructive mutations.** FastMCP v3's `Approval`
  primitive surfaces an inline "apply / cancel" prompt in supporting
  clients. Wire this into bulk tag/domain mutations so the calling
  user confirms before the change hits DataHub.
- **Rich tool responses via Apps / Prefab UI.** FastMCP v3 supports
  interactive UIs rendered inline in clients that implement them
  (Copilot Studio, VS Code MCP). Candidates: lineage as a graph
  widget, schema as a table, search results as a paginated list.
  Clients that do not support Apps continue to see the JSON response.
- **Code Mode recipes resource** — publish vetted snippets (top-N
  popular datasets, impact analysis for a column change, etc.) as MCP
  resources so agents can paste a known-good snippet instead of
  synthesising from scratch.
- **Token exchange cache** (distributed, Redis-backed) if OBO exchange
  latency becomes a bottleneck at scale. Pairs with FastMCP v3's
  storage-backend abstraction, so the same store can hold JWKS and OBO
  results.
- **Generalised audit trail** emitting per-request user identity to
  DataHub's platform event stream, so DataHub's existing audit UI
  shows MCP-originated mutations alongside UI/API ones. Pairs with
  the OTel `user.sub` attribute above.

## Unresolved questions

- **Should Code Mode or Tool Search become the default transport once
  they stabilise?** Either would reduce context usage for every client,
  but both change the observable tool surface for agents that are not
  expecting a meta-tool catalog. Current inclination: keep both opt-in
  through the 0.x series and reassess at 1.0.
- **Sandbox language surface.** Monty is a Python subset; some standard
  library idioms (e.g., `dataclasses`, `itertools.groupby`) may or may
  not be available. An explicit allowlist needs to be published as part
  of the Code Mode docs so agents do not waste cycles on unsupported
  imports.
