import logging
import urllib.parse
from datetime import datetime
from typing import Tuple

import click
import requests

logger = logging.getLogger(__name__)

CLI_TOKEN_PREFIX = "cli token "

_INSTALL_HELP = """\
The --sso flag requires Playwright and a Chromium browser.

Step 1 — Install the Python package (pick your package manager):
    pip install 'acryl-datahub[sso]'
    uv pip install 'acryl-datahub[sso]'
    pip install 'playwright>=1.40.0'

Step 2 — Download the Chromium browser binary:
    playwright install chromium\
"""


def _check_playwright_ready() -> None:
    """Verify that playwright is importable.

    Raises click.UsageError with step-by-step install instructions if not.
    If the chromium browser binary is missing, Playwright itself will raise
    a clear error at launch time telling the user to run `playwright install`.
    """
    try:
        from playwright.sync_api import sync_playwright  # noqa: F401
    except ImportError as e:
        raise click.UsageError(
            "Playwright is not installed.\n\n" + _INSTALL_HELP
        ) from e


def _warn_about_existing_cli_tokens(
    session: requests.Session,
    frontend_url: str,
    actor_urn: str,
) -> None:
    """Best-effort warning about existing CLI tokens for the current user."""
    try:
        response = session.post(
            f"{frontend_url}/api/v2/graphql",
            json={
                "query": """query listAccessTokens($input: ListAccessTokenInput!) {
                    listAccessTokens(input: $input) {
                        total
                        tokens { name }
                    }
                }""",
                "variables": {
                    "input": {
                        "start": 0,
                        "count": 100,
                        "filters": [
                            {
                                "field": "ownerUrn",
                                "values": [actor_urn],
                            }
                        ],
                    }
                },
            },
        )
        response.raise_for_status()
        data = response.json()
        if data.get("errors"):
          error_msg = data["errors"][0].get("message", str(data["errors"]))
          raise click.ClickException(
            f"Failed to create access token: {error_msg}\n"
            "Check that personal access tokens are enabled and your account has permission."
          )
        access_token = data.get("data", {}).get("createAccessToken", {}).get("accessToken")
        if not access_token:
           raise click.ClickException("Server returned empty access token. Contact your DataHub administrator.")```
        cli_token_count = sum(
            1 for t in tokens if t.get("name", "").startswith(CLI_TOKEN_PREFIX)
        )
        if cli_token_count > 0:
            click.echo(
                f"⚠ You have {cli_token_count} existing CLI token(s). "
                f"Manage them at {frontend_url}/settings/tokens"
            )
    except Exception:
        logger.debug("Failed to check existing CLI tokens", exc_info=True)


def browser_sso_login(
    frontend_url: str,
    token_duration: str,
    timeout_ms: int = 120_000,
    support: bool = False,
) -> Tuple[str, str]:
    """Open browser for SSO login, extract session, generate access token.

    Args:
        frontend_url: The DataHub frontend URL (e.g. http://localhost:9002).
        token_duration: Token validity duration (e.g. ONE_HOUR).
        timeout_ms: How long to wait for SSO login to complete, in milliseconds.
        support: If True, use /support/authenticate path for DataHub Cloud
            support team access to customer instances.

    Returns:
        Tuple of (token_name, access_token).

    Raises:
        click.ClickException: On timeout or missing session cookies.
    """
    _check_playwright_ready()

    from playwright.sync_api import sync_playwright

    auth_path = "/support/authenticate" if support else "/authenticate"
    if support:
        click.echo("Opening browser for support SSO login...")
    else:
        click.echo("Opening browser for SSO login...")
    click.echo("Complete the login in your browser.\n")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        try:
        	context = browser.new_context()
        	page = context.new_page()

        	page.goto(f"{frontend_url}{auth_path}")

        	# Wait for the actor cookie, which signals successful SSO login.
        	actor_urn = None
        	try:
            	page.wait_for_function(
                		"""() => document.cookie.split('; ').some(c => c.startsWith('actor='))""",
                		timeout=timeout_ms,
            	)
        	except Exception as e:
            	browser.close()
            	raise click.ClickException(
                		f"SSO login timed out after {timeout_ms // 1000} seconds. "
                		"Please try again."
            	) from e

        	# Extract cookies from the browser context
        	cookies = context.cookies()
        finally:
        	browser.close()

    # Build a requests.Session with the extracted cookies
    session = requests.Session()
    for cookie in cookies:
        session.cookies.set(
            cookie["name"],
            cookie["value"],
            domain=cookie.get("domain", ""),
            path=cookie.get("path", "/"),
        )

    # Extract actor URN from the actor cookie
    for cookie in cookies:
        if cookie["name"] == "actor":
            actor_urn = urllib.parse.unquote(cookie["value"])
            break

    if not actor_urn:
        raise click.ClickException(
            "SSO login succeeded but no actor cookie found. "
            "This may indicate an incompatible DataHub version."
        )

    click.echo(f"✓ Logged in as {actor_urn}")

    _warn_about_existing_cli_tokens(session, frontend_url, actor_urn)

    # Generate an access token via the frontend GraphQL API
    now = datetime.now()
    timestamp = now.astimezone().isoformat()
    token_name = f"cli token {timestamp}"

    json_payload = {
        "query": """mutation createAccessToken($input: CreateAccessTokenInput!) {
            createAccessToken(input: $input) {
              accessToken
              metadata {
                id
                actorUrn
                ownerUrn
                name
                description
              }
            }
        }""",
        "variables": {
            "input": {
                "type": "PERSONAL",
                "actorUrn": actor_urn,
                "duration": token_duration,
                "name": token_name,
            }
        },
    }

    response = session.post(f"{frontend_url}/api/v2/graphql", json=json_payload)
    response.raise_for_status()

    data = response.json()
    access_token = data.get("data", {}).get("createAccessToken", {}).get("accessToken")

    if not access_token:
        errors = data.get("errors", [])
        error_msg = errors[0]["message"] if errors else "Unknown error"
        raise click.ClickException(f"Failed to generate access token: {error_msg}")

    return token_name, access_token
