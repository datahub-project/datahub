import json
import logging
import os
import plistlib
import re
import shutil
import subprocess
import sys
import urllib.parse
from datetime import datetime
from pathlib import Path
from typing import Any, List, NamedTuple, Optional, Tuple

import click
import requests

from datahub.cli.config_utils import DATAHUB_ROOT_FOLDER

logger = logging.getLogger(__name__)

CLI_TOKEN_PREFIX = "cli token "

SSO_PROFILE_ROOT = Path(DATAHUB_ROOT_FOLDER) / "sso-browser-profiles"

SSO_SESSION_ROOT = Path(DATAHUB_ROOT_FOLDER) / "sso-sessions"

_ACTOR_COOKIE_PRESENT = (
    """() => document.cookie.split('; ').some(c => c.startsWith('actor='))"""
)

SILENT_ATTEMPT_TIMEOUT_MS = 20_000


class BrowserTarget(NamedTuple):
    """How to reach one browser through Playwright.

    ``engine`` is the Playwright browser type. ``channel`` names an installation
    already present on the machine, or is None for the build Playwright ships.

    ``engine`` also decides the on-disk profile format, which is why profiles are
    stored per engine: they are not interchangeable between engines.
    """

    engine: str
    channel: Optional[str]


BUNDLED_TARGET = BrowserTarget(engine="chromium", channel=None)

_OS_HANDLER_TARGETS: Tuple[Tuple[str, BrowserTarget], ...] = (
    ("firefox", BrowserTarget(engine="firefox", channel="moz-firefox")),
    ("edge", BrowserTarget(engine="chromium", channel="msedge")),
    ("chrome", BrowserTarget(engine="chromium", channel="chrome")),
)

_INSTALL_HELP = """\
The --sso flag requires Playwright and a browser for it to drive.

Step 1 — Install the Python package (pick your package manager):
    pip install 'acryl-datahub[sso]'
    uv pip install 'acryl-datahub[sso]'
    pip install 'playwright>=1.40.0'

Step 2 — Download a fallback browser binary:
    playwright install chromium

Login drives the browser your operating system already defaults to where it
can, so no download is usually needed for it. The bundled build above is the
fallback when that browser cannot be driven.\
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


def _os_browser_handler() -> Optional[str]:
    """Return the raw handler the operating system registered for https."""
    try:
        if sys.platform == "darwin":
            plist = (
                Path.home()
                / "Library/Preferences/com.apple.LaunchServices"
                / "com.apple.launchservices.secure.plist"
            )
            with plist.open("rb") as handle:
                handlers = plistlib.load(handle).get("LSHandlers", [])
            for handler in handlers:
                if handler.get("LSHandlerURLScheme") == "https":
                    return handler.get("LSHandlerRoleAll")
            return None

        if sys.platform.startswith("linux"):
            result = subprocess.run(
                ["xdg-settings", "get", "default-web-browser"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            return result.stdout.strip() or None

        if sys.platform == "win32":
            import winreg

            key = (
                r"Software\Microsoft\Windows\Shell\Associations"
                r"\UrlAssociations\https\UserChoice"
            )
            with winreg.OpenKey(winreg.HKEY_CURRENT_USER, key) as handle:
                return winreg.QueryValueEx(handle, "ProgId")[0]
    except Exception:
        logger.debug("Could not read the default browser", exc_info=True)
    return None


def browser_target() -> BrowserTarget:
    """Resolve which browser to drive from the operating system's own choice.

    Falls back to the bundled build, which is always present once
    `playwright install chromium` has run.
    """
    handler = (_os_browser_handler() or "").lower()
    for fingerprint, target in _OS_HANDLER_TARGETS:
        if fingerprint in handler:
            return target
    return BUNDLED_TARGET


def _describe(target: BrowserTarget) -> str:
    """Name a target the way it should read in command output."""
    return target.channel or f"bundled {target.engine}"


def _launch_targets(target: BrowserTarget) -> List[BrowserTarget]:
    """The targets to try, in order.

    Dropping the channel keeps the same engine but uses the build Playwright
    ships, covering a machine where that browser is not installed without
    silently switching engine. The bundled target is the last resort.
    """
    targets = [target]
    if target.channel:
        targets.append(BrowserTarget(engine=target.engine, channel=None))
    if BUNDLED_TARGET not in targets:
        targets.append(BUNDLED_TARGET)
    return targets


def _instance_key(frontend_url: str, support: bool) -> str:
    """The name one DataHub instance is stored under.

    Keyed by host so that instances behind different identity providers never
    share cookies. Support logins get their own key, so a support session is
    never silently reused as a normal login, or the reverse.

    The profile directory and the session file both derive from this, and they
    have to stay in lockstep for a remembered session to reach the profile it
    was saved beside.
    """
    host = urllib.parse.urlparse(frontend_url).netloc or "unknown-host"
    safe_host = re.sub(r"[^A-Za-z0-9._-]", "_", host)
    return safe_host + ("-support" if support else "")


def _profile_key(target: BrowserTarget) -> str:
    """The directory name that holds one browser's profiles.

    Keyed by channel as well as engine. Chrome and the Chromium build Playwright
    ships read the same profile format but encrypt cookies against different
    operating system keychain entries, so handing one the other's profile loses
    the saved session and can reset the profile for its real owner.
    """
    return f"{target.engine}-{target.channel}" if target.channel else target.engine


def _sso_profile_dir(frontend_url: str, support: bool, browser: str) -> Path:
    """Return the browser profile directory for one DataHub instance."""
    return SSO_PROFILE_ROOT / browser / _instance_key(frontend_url, support)


def _discard_saved_profiles(frontend_url: str, support: bool) -> None:
    """Remove every saved profile for one instance, whichever browser wrote it.

    --fresh-login has to reach the profiles of browsers this run will not open.
    Clearing only the current browser leaves the previous user signed in behind
    the old default, and switching the default back signs them in again.

    Failure is reported rather than ignored: launching on a half-deleted profile
    is how --fresh-login silently returns the identity it promised to drop.
    """
    key = _instance_key(frontend_url, support)
    if not SSO_PROFILE_ROOT.is_dir():
        return
    for browser_dir in SSO_PROFILE_ROOT.iterdir():
        profile_dir = browser_dir / key
        if not profile_dir.exists():
            continue
        try:
            shutil.rmtree(profile_dir)
        except OSError as e:
            raise click.ClickException(
                f"--fresh-login could not remove the saved profile at "
                f"{profile_dir}: {e}\n"
                "Remove it by hand, otherwise the previous login stays valid."
            ) from e


def _prepare_profile_dir(profile_dir: Path) -> None:
    """Create the profile directory.

    Permissions are tightened to 0700 because the directory holds live identity
    provider session cookies, not just the scoped DataHub token.
    """
    profile_dir.mkdir(parents=True, exist_ok=True)
    for path in (SSO_PROFILE_ROOT, profile_dir):
        try:
            path.chmod(0o700)
        except OSError:
            logger.debug("Could not tighten permissions on %s", path, exc_info=True)


def _seed_profile_dir(profile_dir: Path, seed_from: Path) -> None:
    """Copy an existing browser profile in, so even the first login is skipped.

    Point this at a copy of a profile you already sign in with, and its identity
    provider session comes along. Never point it at a profile the browser
    currently has open: browsers hold an exclusive lock and a live copy is torn.

    Only runs into an empty directory. Once a session is saved here, re-seeding
    would throw it away, which is what --fresh-login is for.
    """
    if any(profile_dir.iterdir()):
        return

    if not seed_from.is_dir():
        raise click.UsageError(f"--seed-profile is not a directory: {seed_from}")

    click.echo(f"Seeding browser profile from {seed_from} ...")
    shutil.copytree(
        seed_from,
        profile_dir,
        dirs_exist_ok=True,
        symlinks=False,
        ignore=shutil.ignore_patterns(
            "lock",
            ".parentlock",
            "parent.lock",
            "compatibility.ini",
            "SingletonLock",
            "SingletonCookie",
            "cache2",
            "startupCache",
            "shader-cache",
            "GrShaderCache",
        ),
    )


def _open_browser_context(
    playwright: Any,
    frontend_url: str,
    support: bool,
    target: BrowserTarget,
    seed_from: Optional[Path] = None,
    headless: bool = False,
) -> Any:
    """Open a browser context, preferring the user's browser and saved session.

    Walks the fallback chain, which covers a machine where the requested browser
    is not installed. Each candidate gets its own profile directory, because a
    profile belongs to one browser build and is not interchangeable.

    Raises rather than falling back to a throwaway profile: silently switching
    browser and dropping the saved session hides the cause, and the usual reason
    everything fails is another `datahub init --sso` holding the profile lock.

    The caller closes only the context; leaving sync_playwright() tears down the
    browser behind it.
    """
    tried = _launch_targets(target)
    last_error: Optional[Exception] = None
    for candidate in tried:
        engine = getattr(playwright, candidate.engine)
        launch_args = {"channel": candidate.channel} if candidate.channel else {}
        profile_dir = _sso_profile_dir(frontend_url, support, _profile_key(candidate))
        _prepare_profile_dir(profile_dir)
        if seed_from is not None and candidate.engine == target.engine:
            _seed_profile_dir(profile_dir, seed_from)
        try:
            context = engine.launch_persistent_context(
                str(profile_dir), headless=headless, **launch_args
            )
        except Exception as e:
            last_error = e
            logger.debug("Could not launch %s", _describe(candidate), exc_info=True)
            continue

        if candidate != target:
            click.echo(
                f"Note: {_describe(target)} unavailable, using {_describe(candidate)}."
            )
        return context

    raise click.ClickException(
        "Could not open a browser for SSO login. Tried: "
        f"{', '.join(_describe(t) for t in tried)}.\n"
        f"Last error: {last_error}\n"
        "If another `datahub init --sso` is running, finish it first. If one "
        "crashed and left the profile locked, retry with --fresh-login."
    )


def _session_file(frontend_url: str, support: bool) -> Path:
    """Where a remembered login is stored for one instance.

    Not keyed by browser: cookies are portable, so a session saved while driving
    one browser still works if the next run drives another.
    """
    return SSO_SESSION_ROOT / f"{_instance_key(frontend_url, support)}.json"


def _load_session(path: Path) -> list:
    """Read a remembered login, treating any damage as simply absent."""
    try:
        with path.open() as handle:
            saved = json.load(handle)
        return saved if isinstance(saved, list) else []
    except (OSError, ValueError):
        logger.debug("Could not read saved session %s", path, exc_info=True)
        return []


def _save_session(path: Path, cookies: list) -> None:
    """Store the cookies this login established, owner-readable only."""
    if not cookies:
        return
    try:
        SSO_SESSION_ROOT.mkdir(parents=True, exist_ok=True)
        SSO_SESSION_ROOT.chmod(0o700)
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as handle:
            json.dump(cookies, handle)
    except OSError:
        logger.debug("Could not save session to %s", path, exc_info=True)


def _run_login_attempt(
    context: Any,
    auth_url: str,
    timeout_ms: int,
    restore: list,
) -> Optional[Tuple[list, set, set]]:
    """Make one pass at the login.

    Returns every cookie in the context, the hosts the flow navigated through,
    and a snapshot of the cookies present before it started, or None if the
    actor cookie never appeared. Returning None rather
    than raising is what lets a silent attempt fall back to a visible one.
    """
    if restore:
        try:
            context.add_cookies(restore)
        except Exception:
            logger.debug("Could not restore session", exc_info=True)

    page = context.pages[0] if context.pages else context.new_page()

    hosts = {urllib.parse.urlparse(auth_url).hostname}

    def note_host(frame: Any) -> None:
        try:
            host = urllib.parse.urlparse(frame.url).hostname
        except Exception:
            return
        if host:
            hosts.add(host)

    before = {(c["domain"], c["name"], c["value"]) for c in context.cookies()}

    page.on("framenavigated", note_host)
    page.goto(auth_url)

    # Wait for the actor cookie, which signals successful SSO login.
    try:
        page.wait_for_function(_ACTOR_COOKIE_PRESENT, timeout=timeout_ms)
    except Exception:
        logger.debug("Actor cookie did not appear within %dms", timeout_ms)
        return None

    return context.cookies(), {h for h in hosts if h}, before


def _cookie_matches_host(cookie: dict, host: str) -> bool:
    """Would a browser send this cookie to that host?

    Ordinary cookie host matching: an exact host, or a parent domain of it.
    """
    domain = (cookie.get("domain") or "").lstrip(".")
    return bool(domain) and (host == domain or host.endswith(f".{domain}"))


def _cookie_applies_to(cookie: dict, frontend_url: str) -> bool:
    """Would a browser send this cookie to that URL?"""
    host = urllib.parse.urlparse(frontend_url).hostname or ""
    return _cookie_matches_host(cookie, host)


def _cookie_key(cookie: dict) -> Tuple[str, str, str]:
    """What makes a cookie the same cookie for storage purposes."""
    return (cookie.get("domain", ""), cookie.get("name", ""), cookie.get("path", "/"))


def _session_cookies_for(
    cookies: list, hosts: set, before: set, previous: list
) -> list:
    """The cookies worth remembering as this instance's login.

    Three signals, because each alone has failed in practice:

    - hosts the flow navigated through, which is the identity provider and the
      instance;
    - hosts whose cookies this run added or changed, in case a navigation went
      unobserved;
    - whatever was already saved, merged underneath so a run served entirely
      from the browser profile — which creates nothing new and may visit
      nobody — cannot erase a working session.
    """
    changed_hosts = {
        (c.get("domain") or "").lstrip(".")
        for c in cookies
        if (c["domain"], c["name"], c["value"]) not in before
    }
    scope = {h for h in (set(hosts) | changed_hosts) if h}

    merged = {_cookie_key(c): c for c in previous}
    merged.update(
        {
            _cookie_key(c): c
            for c in cookies
            if any(_cookie_matches_host(c, h) for h in scope)
        }
    )
    return list(merged.values())


def _actor_urn_from(cookies: list) -> Optional[str]:
    """Read the actor URN the instance wrote, for a server too old to be asked."""
    for cookie in cookies:
        if cookie["name"] == "actor":
            return urllib.parse.unquote(cookie["value"])
    return None


def _requests_session(cookies: list) -> requests.Session:
    """A requests session carrying the cookies that belong to this instance."""
    session = requests.Session()
    for cookie in cookies:
        session.cookies.set(
            cookie["name"],
            cookie["value"],
            domain=cookie.get("domain", ""),
            path=cookie.get("path", "/"),
        )
    return session


def _confirm_session(session: requests.Session, frontend_url: str) -> Optional[str]:
    """Ask the instance who these cookies are, or None if they are not signed in.

    The actor cookie alone cannot answer this. A saved session is replayed into
    the browser before the login runs, and a reused profile carries one from an
    earlier run, so the cookie is present whether or not the identity provider
    session behind it is still alive. Only the instance knows, and it is the URN
    it returns here that the access token is minted against.
    """
    try:
        response = session.post(
            f"{frontend_url}/api/v2/graphql",
            json={"query": "query { me { corpUser { urn } } }"},
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        if data.get("errors"):
            return None
        me = (data.get("data") or {}).get("me") or {}
        urn = (me.get("corpUser") or {}).get("urn")
        return urn if isinstance(urn, str) and urn else None
    except Exception:
        logger.debug("Could not confirm the session with the instance", exc_info=True)
        return None


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
        tokens = data.get("data", {}).get("listAccessTokens", {}).get("tokens", [])
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
    ticket_id: Optional[str] = None,
    fresh_login: bool = False,
    seed_profile: Optional[str] = None,
    remember_session: bool = False,
) -> Tuple[str, str]:
    """Open browser for SSO login, extract session, generate access token.

    Follows the operating system's default browser unless told otherwise, and
    reuses a per-instance profile under ~/.datahub, so a still-valid identity
    provider session skips the login form on later runs.

    Args:
        frontend_url: The DataHub frontend URL (e.g. http://localhost:9002).
        token_duration: Token validity duration (e.g. ONE_HOUR).
        timeout_ms: How long to wait for SSO login to complete, in milliseconds.
        support: If True, use /support/authenticate path for DataHub Cloud
            support team access to customer instances.
        ticket_id: Support ticket ID, appended to the support auth URL.
        fresh_login: If True, discard the saved browser profiles and the
            remembered session first. Use this to sign in as a different user.
        seed_profile: Path to an existing browser profile to copy in the first
            time this instance is used, so an identity provider session already
            in that profile skips even the first login. Must be a copy, not a
            profile the browser currently has open.
        remember_session: If True, store the cookies this login establishes and
            replay them next time. Needed when the identity provider issues an
            in-memory session, which no browser writes to disk. Writes that
            session to ~/.datahub/sso-sessions as 0600.

    Returns:
        Tuple of (token_name, access_token).

    Raises:
        click.ClickException: On timeout or missing session cookies.
    """
    _check_playwright_ready()

    from playwright.sync_api import sync_playwright

    auth_path = "/support/authenticate" if support else "/authenticate"
    auth_url = f"{frontend_url}{auth_path}"
    if support and ticket_id:
        auth_url += "?" + urllib.parse.urlencode({"ticket_id": ticket_id})

    target = browser_target()
    session_file = _session_file(frontend_url, support)
    if fresh_login:
        session_file.unlink(missing_ok=True)
        _discard_saved_profiles(frontend_url, support)

    restore = (
        _load_session(session_file) if remember_session and not fresh_login else []
    )

    attempts = ([(True, SILENT_ATTEMPT_TIMEOUT_MS)] if restore else []) + [
        (False, timeout_ms)
    ]

    actor_urn: Optional[str] = None
    session: Optional[requests.Session] = None
    with sync_playwright() as p:
        for headless, attempt_timeout in attempts:
            if headless:
                click.echo("Trying the saved session in the background...")
            else:
                which = "support SSO" if support else "SSO"
                click.echo(f"Opening browser for {which} login...")
                click.echo("Complete the login in your browser.\n")

            context = _open_browser_context(
                p,
                frontend_url,
                support,
                target,
                Path(seed_profile) if seed_profile else None,
                headless=headless,
            )
            try:
                result = _run_login_attempt(context, auth_url, attempt_timeout, restore)
                if result is None:
                    if headless:
                        click.echo("Saved session did not work, opening a browser.")
                        continue
                    raise click.ClickException(
                        f"SSO login timed out after {attempt_timeout // 1000} "
                        "seconds. Please try again."
                    )

                cookies, auth_hosts, before = result
                scoped = [c for c in cookies if _cookie_applies_to(c, frontend_url)]
                attempt_session = _requests_session(scoped)
                confirmed = _confirm_session(attempt_session, frontend_url)

                if confirmed is None and headless:
                    click.echo("The saved session has expired, opening a browser.")
                    continue

                session = attempt_session
                actor_urn = confirmed or _actor_urn_from(scoped)
                if headless:
                    click.echo("Signed in from the saved session, no browser shown.")
                if remember_session:
                    keep = _session_cookies_for(cookies, auth_hosts, before, restore)
                    _save_session(session_file, keep)
                    hosts_kept = sorted({c.get("domain", "") for c in keep})
                    click.echo(
                        f"Remembered {len(keep)} cookies for {', '.join(hosts_kept)}."
                    )
                break
            finally:
                context.close()

    if session is None or not actor_urn:
        raise click.ClickException(
            "SSO login completed but the instance did not report who signed in. "
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

    response = session.post(
        f"{frontend_url}/api/v2/graphql", json=json_payload, timeout=30
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
        raise click.ClickException(
            "Server returned empty access token. Contact your DataHub administrator."
        )

    return token_name, access_token
