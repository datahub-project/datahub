import base64
import hashlib
import http.server
import logging
import secrets
import threading
import urllib.parse
import webbrowser
from typing import Any, Dict, NamedTuple, Optional, Tuple, Type

import click
import requests

logger = logging.getLogger(__name__)

_SUCCESS_HTML = b"""<!DOCTYPE html>
<html><head><title>DataHub Login</title></head>
<body style="font-family:sans-serif;text-align:center;padding-top:60px">
<h2 style="color:#1e88e5">&#10003; Logged in to DataHub</h2>
<p>You can close this tab and return to the terminal.</p>
</body></html>"""

_ERROR_HTML = b"""<!DOCTYPE html>
<html><head><title>DataHub Login</title></head>
<body style="font-family:sans-serif;text-align:center;padding-top:60px">
<h2 style="color:#e53935">&#10007; Login failed</h2>
<p>An error occurred. Please return to the terminal for details.</p>
</body></html>"""


class OAuthResult(NamedTuple):
    client_id: str
    access_token: str
    refresh_token: Optional[str]
    token_endpoint: str


def _check_cloud_instance(gms_url: str) -> None:
    """Raise a clear ClickException if /config identifies this as a non-cloud instance."""
    try:
        resp = requests.get(f"{gms_url.rstrip('/')}/config", timeout=5)
        if resp.status_code == 200:
            server_env = resp.json().get("datahub", {}).get("serverEnv")
            if server_env is not None and server_env != "cloud":
                raise click.ClickException(
                    f"This DataHub instance is not a Cloud instance (serverEnv={server_env!r}).\n"
                    "--oauth requires DataHub Cloud. Use `datahub init` for standard login."
                )
    except click.ClickException:
        raise
    except (requests.RequestException, ValueError, KeyError):
        pass  # Can't determine — fall through to the discovery endpoint check


def _validate_endpoint_origin(endpoint_url: str, gms_url: str, field: str) -> None:
    """Ensure a discovered endpoint belongs to the same origin as the GMS URL."""
    gms_parsed = urllib.parse.urlparse(gms_url.rstrip("/"))
    ep_parsed = urllib.parse.urlparse(endpoint_url)
    if ep_parsed.netloc != gms_parsed.netloc:
        raise click.ClickException(
            f"Discovery document returned {field!r} on a different origin "
            f"({ep_parsed.netloc!r} vs {gms_parsed.netloc!r}). "
            "This may indicate a misconfigured or malicious server."
        )


def _discover_oauth_server(gms_url: str) -> Dict[str, Any]:
    """Fetch /.well-known/oauth-authorization-server. Raises ClickException if unavailable."""
    discovery_url = f"{gms_url.rstrip('/')}/.well-known/oauth-authorization-server"
    try:
        resp = requests.get(discovery_url, timeout=10)
    except requests.RequestException as e:
        raise click.ClickException(
            f"Cannot reach DataHub at {gms_url}: {e}\nCheck the URL and try again."
        ) from e

    if resp.status_code == 404:
        raise click.ClickException(
            f"OAuth2 server is not enabled at {gms_url}.\n"
            "The --oauth flag requires a DataHub Cloud instance with the OAuth2 server enabled.\n"
            "Use `datahub init --sso` for SSO login or `datahub init` for username/password login."
        )

    if resp.status_code != 200:
        raise click.ClickException(
            f"Unexpected response from {discovery_url}: HTTP {resp.status_code}.\n"
            "Use `datahub init --sso` for SSO login or `datahub init` for username/password login."
        )

    try:
        metadata: Dict[str, Any] = resp.json()
    except ValueError as e:
        raise click.ClickException(
            f"OAuth2 discovery document at {discovery_url} is not valid JSON. "
            "This URL may not be a DataHub Cloud instance."
        ) from e

    if "authorization_endpoint" not in metadata or "token_endpoint" not in metadata:
        raise click.ClickException(
            f"OAuth2 discovery document at {discovery_url} is missing required fields.\n"
            "Use `datahub init --sso` for SSO login or `datahub init` for username/password login."
        )

    # Reject endpoints that point to a different origin — guards against a
    # compromised server redirecting token requests to an attacker-controlled host.
    _validate_endpoint_origin(
        metadata["authorization_endpoint"], gms_url, "authorization_endpoint"
    )
    _validate_endpoint_origin(metadata["token_endpoint"], gms_url, "token_endpoint")
    if "registration_endpoint" in metadata:
        _validate_endpoint_origin(
            metadata["registration_endpoint"], gms_url, "registration_endpoint"
        )

    return metadata


def _register_cli_client(registration_endpoint: str, redirect_uri: str) -> str:
    """RFC 7591 DCR: register a public PKCE client. Returns client_id."""
    try:
        resp = requests.post(
            registration_endpoint,
            json={
                "client_name": "DataHub CLI",
                "grant_types": ["authorization_code", "refresh_token"],
                "redirect_uris": [redirect_uri],
                "token_endpoint_auth_method": "none",
                "scope": "openid datahub:account",
            },
            timeout=10,
        )
    except requests.RequestException as e:
        raise click.ClickException(f"Failed to register OAuth2 client: {e}") from e

    if resp.status_code in (401, 403):
        raise click.ClickException(
            "Dynamic Client Registration (DCR) is not enabled on this DataHub instance.\n"
            "Use `datahub init --sso` for SSO login or `datahub init` for username/password login."
        )

    if resp.status_code not in (200, 201):
        raise click.ClickException(
            f"OAuth2 client registration failed: HTTP {resp.status_code}: {resp.text[:200]}"
        )

    try:
        data = resp.json()
        client_id: str = data["client_id"]
    except (ValueError, KeyError) as e:
        raise click.ClickException(f"Invalid DCR response: {e}") from e

    return client_id


def _generate_pkce_pair() -> Tuple[str, str]:
    """Returns (code_verifier, code_challenge) using S256 method."""
    verifier = secrets.token_urlsafe(32)
    digest = hashlib.sha256(verifier.encode("ascii")).digest()
    challenge = base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")
    return verifier, challenge


def _make_callback_handler(
    result: Dict[str, Any], expected_state: str
) -> Type[http.server.BaseHTTPRequestHandler]:
    """Return an HTTP handler class that captures the OAuth2 authorization code callback."""

    class _Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            parsed = urllib.parse.urlparse(self.path)
            params = urllib.parse.parse_qs(parsed.query)
            result["code"] = params.get("code", [None])[0]
            result["error"] = params.get("error", [None])[0]
            result["error_description"] = params.get("error_description", [None])[0]
            result["state"] = params.get("state", [None])[0]

            # Show error page when state is missing/wrong even if a code was returned,
            # so the user knows something went wrong before they close the tab.
            state_ok = result.get("state") == expected_state
            body = _SUCCESS_HTML if (result.get("code") and state_ok) else _ERROR_HTML
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

            # Shut down from a separate thread to avoid deadlock inside serve_forever
            threading.Thread(target=self.server.shutdown, daemon=True).start()

        def log_message(self, format: str, *args: object) -> None:
            pass  # silence per-request logs

    return _Handler


def pkce_login(gms_url: str, timeout_seconds: int = 120) -> OAuthResult:
    """
    Run a PKCE authorization code flow against the DataHub OAuth2 server.

    Steps:
      1. Fetch /.well-known/oauth-authorization-server to confirm OAuth2 is available
         and discover endpoints. Fails gracefully if not a cloud/OAuth2-enabled instance.
      2. Register a public PKCE client via RFC 7591 Dynamic Client Registration.
         Fails gracefully if DCR is not enabled.
      3. Start a loopback HTTP server on a random free port (bound atomically by the OS).
      4. Open the browser to the authorization endpoint.
      5. Wait for the loopback redirect, verify the state parameter, and extract the code.
      6. Exchange code + PKCE verifier for access_token + refresh_token.

    Args:
        gms_url: DataHub GMS base URL (e.g. https://your-instance.acryl.io/gms)
        timeout_seconds: Seconds to wait for the user to complete browser login

    Returns:
        OAuthResult with client_id, access_token, refresh_token, and token_endpoint

    Raises:
        click.ClickException: on any failure with a user-friendly message
    """
    click.echo("Checking DataHub instance...")
    _check_cloud_instance(gms_url)
    metadata = _discover_oauth_server(gms_url)

    authorization_endpoint: str = metadata["authorization_endpoint"]
    token_endpoint: str = metadata["token_endpoint"]
    registration_endpoint: Optional[str] = metadata.get("registration_endpoint")

    if not registration_endpoint:
        raise click.ClickException(
            "Dynamic Client Registration (DCR) is not enabled on this DataHub instance.\n"
            "Use `datahub init --sso` for SSO login or `datahub init` for username/password login."
        )

    code_verifier, code_challenge = _generate_pkce_pair()
    state = secrets.token_urlsafe(16)

    # Bind to port 0 and let the OS assign a free port atomically — eliminates the
    # TOCTOU race that would exist between a probe bind and a separate server bind.
    result: Dict[str, Any] = {}
    handler_class = _make_callback_handler(result, state)
    server = http.server.HTTPServer(("127.0.0.1", 0), handler_class)
    port = server.server_address[1]
    redirect_uri = f"http://127.0.0.1:{port}/callback"

    click.echo("Registering OAuth2 client...")
    client_id = _register_cli_client(registration_endpoint, redirect_uri)

    auth_params = {
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "scope": "openid datahub:account",
        "code_challenge": code_challenge,
        "code_challenge_method": "S256",
        "state": state,
    }
    auth_url = f"{authorization_endpoint}?{urllib.parse.urlencode(auth_params)}"

    server_thread = threading.Thread(
        target=server.serve_forever,
        kwargs={"poll_interval": 0.5},
        daemon=True,
    )
    server_thread.start()

    click.echo("Opening browser for OAuth2 login...")
    click.echo(f"If the browser does not open automatically, visit:\n  {auth_url}\n")

    try:
        webbrowser.open(auth_url)
    except Exception:
        pass  # already printed the URL above as fallback

    server_thread.join(timeout=timeout_seconds)

    if server_thread.is_alive():
        server.shutdown()
        server_thread.join(timeout=5)
        raise click.ClickException(
            f"OAuth2 login timed out after {timeout_seconds} seconds. Please try again."
        )

    if result.get("error"):
        desc = result.get("error_description") or result["error"]
        raise click.ClickException(f"OAuth2 authorization failed: {desc}")

    received_state = result.get("state")
    if received_state != state:
        raise click.ClickException(
            "OAuth2 state mismatch — the callback did not include the expected state token. "
            "This may indicate a CSRF attempt. Please try again."
        )

    code = result.get("code")
    if not code:
        raise click.ClickException(
            "OAuth2 callback received but no authorization code found. Please try again."
        )

    click.echo("✓ Authorization received. Exchanging for tokens...")

    try:
        token_resp = requests.post(
            token_endpoint,
            data={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": redirect_uri,
                "client_id": client_id,
                "code_verifier": code_verifier,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=15,
        )
    except requests.RequestException as e:
        raise click.ClickException(f"Token exchange failed: {e}") from e

    if token_resp.status_code != 200:
        raise click.ClickException(
            f"Token exchange failed: HTTP {token_resp.status_code}: {token_resp.text[:200]}"
        )

    try:
        token_data = token_resp.json()
    except ValueError as e:
        raise click.ClickException(f"Invalid token response: {e}") from e

    access_token: Optional[str] = token_data.get("access_token")
    if not access_token:
        raise click.ClickException(
            "Server returned empty access token. Please try again."
        )

    refresh_token: Optional[str] = token_data.get("refresh_token")

    return OAuthResult(
        client_id=client_id,
        access_token=access_token,
        refresh_token=refresh_token,
        token_endpoint=token_endpoint,
    )
