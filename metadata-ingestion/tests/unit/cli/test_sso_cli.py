import json
import sys
from pathlib import Path
from typing import Any, Callable, Iterator, Optional
from unittest.mock import MagicMock, patch

import click
import pytest

from datahub.cli.sso_cli import (
    BUNDLED_TARGET,
    SILENT_ATTEMPT_TIMEOUT_MS,
    BrowserTarget,
    _cookie_applies_to,
    _launch_targets,
    _profile_key,
    _session_cookies_for,
    _sso_profile_dir,
    _warn_about_existing_cli_tokens,
    browser_sso_login,
    browser_target,
)


def mock_requests(me: Optional[str] = "urn:li:corpuser:pedro") -> MagicMock:
    """A stand-in requests module whose GraphQL calls answer plausibly.

    Login asks the instance who is signed in before it trusts any cookie, so a
    test that leaves that call unanswered exercises the expired-session path
    instead of the one it is named for. Pass me=None to be that expired session.
    """
    module = MagicMock()
    session = MagicMock()
    module.Session.return_value = session

    def post(url: str, **kwargs: Any) -> MagicMock:
        query = kwargs.get("json", {}).get("query", "")
        response = MagicMock()
        if "me {" in query:
            response.json.return_value = {"data": {"me": {"corpUser": {"urn": me}}}}
        elif "listAccessTokens" in query:
            response.json.return_value = {
                "data": {"listAccessTokens": {"total": 0, "tokens": []}}
            }
        else:
            response.json.return_value = {
                "data": {"createAccessToken": {"accessToken": "tok"}}
            }
        return response

    session.post.side_effect = post
    return module


def graphql_call(session: MagicMock, marker: str) -> Any:
    """The call whose query contains marker, so a probe cannot shift an index."""
    for call in session.post.call_args_list:
        if marker in call[1]["json"]["query"]:
            return call
    raise AssertionError(f"no GraphQL call containing {marker!r}")


@pytest.fixture(autouse=True)
def deterministic_browser() -> Iterator[None]:
    """Pin the browser under test.

    Without this the suite resolves the real OS default, so which engine gets
    driven — and which profile directory is used — would differ per machine.
    Classes that need another engine override this fixture.
    """
    with patch(
        "datahub.cli.sso_cli.browser_target",
        return_value=BrowserTarget(engine="firefox", channel="moz-firefox"),
    ):
        yield


@pytest.fixture
def mock_playwright(tmp_path: Path) -> Iterator[dict]:
    """Mock the Playwright sync API and skip auto-install."""
    mock_sync_pw = MagicMock()

    # Mock the playwright.sync_api module so the local import inside browser_sso_login works
    mock_module = MagicMock()
    mock_module.sync_playwright = mock_sync_pw
    with (
        patch.dict(
            sys.modules, {"playwright": MagicMock(), "playwright.sync_api": mock_module}
        ),
        patch("datahub.cli.sso_cli._check_playwright_ready"),
        patch("datahub.cli.sso_cli.SSO_PROFILE_ROOT", tmp_path / "profiles"),
    ):
        pw = MagicMock()
        mock_sync_pw.return_value.__enter__ = MagicMock(return_value=pw)
        mock_sync_pw.return_value.__exit__ = MagicMock(return_value=False)

        context = MagicMock()
        pw.chromium.launch_persistent_context.return_value = context
        pw.firefox.launch_persistent_context.return_value = context

        page = MagicMock()
        context.new_page.return_value = page
        context.pages = []

        browser = MagicMock()
        pw.chromium.launch.return_value = browser
        browser.new_context.return_value = context

        yield {
            "playwright": pw,
            "browser": browser,
            "context": context,
            "page": page,
        }


class TestBrowserSsoLogin:
    @pytest.mark.parametrize(
        ("support", "ticket_id", "expected_auth_url"),
        [
            (False, None, "http://localhost:9002/authenticate"),
            (
                True,
                "SUPPORT-123",
                "http://localhost:9002/support/authenticate?ticket_id=SUPPORT-123",
            ),
        ],
    )
    def test_extracts_cookies_and_generates_token(
        self,
        mock_playwright: dict,
        support: bool,
        ticket_id: str | None,
        expected_auth_url: str,
    ) -> None:
        """Happy path: SSO login succeeds, cookies extracted, token generated."""
        context = mock_playwright["context"]
        context.cookies.return_value = [
            {
                "name": "actor",
                "value": "urn%3Ali%3Acorpuser%3Ajohn.doe",
                "domain": "localhost",
                "path": "/",
            },
            {
                "name": "PLAY_SESSION",
                "value": "session-abc-123",
                "domain": "localhost",
                "path": "/",
            },
        ]

        module = mock_requests(me="urn:li:corpuser:john.doe")
        with patch("datahub.cli.sso_cli.requests", module):
            token_name, access_token = browser_sso_login(
                "http://localhost:9002",
                "ONE_HOUR",
                support=support,
                ticket_id=ticket_id,
            )

        assert access_token == "tok"
        assert "cli token" in token_name
        mock_playwright["page"].goto.assert_called_once_with(expected_auth_url)

        session = module.Session.return_value
        assert session.cookies.set.call_count == 2

        create_call = graphql_call(session, "createAccessToken")
        assert create_call[0][0] == "http://localhost:9002/api/v2/graphql"
        assert (
            create_call[1]["json"]["variables"]["input"]["actorUrn"]
            == "urn:li:corpuser:john.doe"
        )
        assert create_call[1]["json"]["variables"]["input"]["duration"] == "ONE_HOUR"

    def test_timeout_raises_error(self, mock_playwright: dict) -> None:
        """Verify timeout if login never completes."""
        page = mock_playwright["page"]
        page.wait_for_function.side_effect = Exception("Timeout 120000ms exceeded")

        with pytest.raises(Exception, match="SSO login timed out"):
            browser_sso_login("http://localhost:9002", "ONE_HOUR", timeout_ms=1000)

        mock_playwright["context"].close.assert_called_once()

    def test_an_unidentifiable_login_raises(self, mock_playwright: dict) -> None:
        """Neither the instance nor a cookie says who signed in."""
        context = mock_playwright["context"]
        context.cookies.return_value = [
            {
                "name": "PLAY_SESSION",
                "value": "session-abc-123",
                "domain": "localhost",
                "path": "/",
            },
        ]

        with (
            patch("datahub.cli.sso_cli.requests", mock_requests(me=None)),
            pytest.raises(Exception, match="did not report who signed in"),
        ):
            browser_sso_login("http://localhost:9002", "ONE_HOUR")

    def test_the_actor_cookie_stands_in_for_an_older_instance(
        self, mock_playwright: dict
    ) -> None:
        """An instance that cannot answer `me` still has to be usable."""
        mock_playwright["context"].cookies.return_value = [
            {
                "name": "actor",
                "value": "urn%3Ali%3Acorpuser%3Ajane",
                "domain": "localhost",
                "path": "/",
            },
        ]

        module = mock_requests(me=None)
        with patch("datahub.cli.sso_cli.requests", module):
            browser_sso_login("http://localhost:9002", "ONE_HOUR")

        create = graphql_call(module.Session.return_value, "createAccessToken")
        assert create[1]["json"]["variables"]["input"]["actorUrn"] == (
            "urn:li:corpuser:jane"
        )

    def test_graphql_error_raises(self, mock_playwright: dict) -> None:
        """Verify error when GraphQL mutation fails."""
        context = mock_playwright["context"]
        context.cookies.return_value = [
            {
                "name": "actor",
                "value": "urn%3Ali%3Acorpuser%3Ajane",
                "domain": "localhost",
                "path": "/",
            },
        ]

        with patch("datahub.cli.sso_cli.requests") as mock_requests:
            mock_session = MagicMock()
            mock_requests.Session.return_value = mock_session
            mock_response = MagicMock()
            mock_response.json.return_value = {
                "errors": [{"message": "Unauthorized to create token"}]
            }
            mock_session.post.return_value = mock_response

            with pytest.raises(Exception, match="Failed to create access token"):
                browser_sso_login("http://localhost:9002", "ONE_HOUR")


class TestWarnAboutExistingCliTokens:
    def test_warns_about_existing_cli_tokens(
        self, capsys: pytest.CaptureFixture
    ) -> None:
        session = MagicMock()
        response = MagicMock()
        response.json.return_value = {
            "data": {
                "listAccessTokens": {
                    "total": 4,
                    "tokens": [
                        {"name": "cli token 2026-03-01T10:00:00"},
                        {"name": "cli token 2026-03-02T10:00:00"},
                        {"name": "cli token 2026-03-03T10:00:00"},
                        {"name": "manually created token"},
                    ],
                }
            }
        }
        session.post.return_value = response

        # Should not raise — failure is silently logged
        _warn_about_existing_cli_tokens(
            session, "https://example.com", "urn:li:corpuser:alice"
        )

        captured = capsys.readouterr()
        assert "3 existing CLI token(s)" in captured.out
        assert "https://example.com/settings/tokens" in captured.out

    def test_warning_failure_does_not_block(self) -> None:
        session = MagicMock()
        session.post.side_effect = Exception("network error")

        _warn_about_existing_cli_tokens(
            session, "https://example.com", "urn:li:corpuser:alice"
        )


class TestSsoProfileDir:
    @pytest.mark.parametrize(
        ("frontend_url", "support", "expected_name"),
        [
            ("http://localhost:9002", False, "localhost_9002"),
            ("https://acme.example.com", False, "acme.example.com"),
            ("https://acme.example.com", True, "acme.example.com-support"),
            ("not-a-url", False, "unknown-host"),
        ],
    )
    def test_dir_is_keyed_by_host_and_mode(
        self, frontend_url: str, support: bool, expected_name: str
    ) -> None:
        """Separate directories stop cookies leaking between instances or modes."""
        assert _sso_profile_dir(frontend_url, support, "chromium").name == expected_name

    def test_different_hosts_never_share_a_directory(self) -> None:
        a = _sso_profile_dir("https://one.example.com", False, "chromium")
        b = _sso_profile_dir("https://two.example.com", False, "chromium")
        assert a != b

    def test_support_and_normal_never_share_a_directory(self) -> None:
        host = "https://acme.example.com"
        assert _sso_profile_dir(host, True, "chromium") != _sso_profile_dir(
            host, False, "chromium"
        )


class TestProfileKey:
    """One directory per browser build, not per engine.

    Chrome and the Chromium build Playwright ships read the same profile format
    but encrypt cookies against different keychain entries, so sharing a
    directory loses the saved session the fallback was meant to preserve.
    """

    def test_a_channel_and_the_bundled_build_are_kept_apart(self) -> None:
        chrome = _profile_key(BrowserTarget(engine="chromium", channel="chrome"))
        bundled = _profile_key(BUNDLED_TARGET)

        assert chrome != bundled

    def test_two_channels_on_one_engine_are_kept_apart(self) -> None:
        chrome = _profile_key(BrowserTarget(engine="chromium", channel="chrome"))
        edge = _profile_key(BrowserTarget(engine="chromium", channel="msedge"))

        assert chrome != edge

    def test_the_fallback_does_not_reuse_the_channel_profile(
        self, mock_playwright: dict, tmp_path: Path
    ) -> None:
        """The regression: falling back must not open Chrome's profile."""
        pw = mock_playwright["playwright"]
        pw.chromium.launch_persistent_context.side_effect = [
            Exception("chrome is not installed"),
            mock_playwright["context"],
        ]

        with (
            patch(
                "datahub.cli.sso_cli.browser_target",
                return_value=BrowserTarget(engine="chromium", channel="chrome"),
            ),
            patch("datahub.cli.sso_cli.requests", mock_requests()),
        ):
            browser_sso_login("http://localhost:9002", "ONE_HOUR")

        used = [
            call[0][0] for call in pw.chromium.launch_persistent_context.call_args_list
        ]
        assert len(set(used)) == 2, "the fallback must get its own profile directory"


class TestPersistentProfile:
    @pytest.fixture(autouse=True)
    def deterministic_browser(self) -> Iterator[None]:
        with patch("datahub.cli.sso_cli.browser_target", return_value=BUNDLED_TARGET):
            yield

    def _login(self, **kwargs: Any) -> None:
        with patch("datahub.cli.sso_cli.requests", mock_requests()):
            try:
                browser_sso_login("http://localhost:9002", "ONE_HOUR", **kwargs)
            except Exception:
                pass

    def test_uses_a_persistent_context(self, mock_playwright: dict) -> None:
        self._login()

        pw = mock_playwright["playwright"]
        pw.chromium.launch_persistent_context.assert_called_once()
        pw.chromium.launch.assert_not_called()

    def test_profile_directory_is_created_with_private_permissions(
        self, mock_playwright: dict, tmp_path: Path
    ) -> None:
        """The directory holds identity provider session cookies."""
        self._login()

        profile_dir = tmp_path / "profiles" / "chromium" / "localhost_9002"
        assert profile_dir.is_dir()
        assert profile_dir.stat().st_mode & 0o777 == 0o700

    def test_reuses_the_same_directory_across_runs(
        self, mock_playwright: dict, tmp_path: Path
    ) -> None:
        """Reuse is the whole point: run two must skip the login form."""
        self._login()
        marker = tmp_path / "profiles" / "chromium" / "localhost_9002" / "Cookies"
        marker.write_text("session")

        self._login()

        assert marker.exists(), "second run must not wipe the saved session"

    def test_fresh_login_discards_the_saved_session(
        self, mock_playwright: dict, tmp_path: Path
    ) -> None:
        self._login()
        marker = tmp_path / "profiles" / "chromium" / "localhost_9002" / "Cookies"
        marker.write_text("session")

        self._login(fresh_login=True)

        assert not marker.exists(), "fresh_login must clear the saved session"

    def test_fresh_login_discards_profiles_for_every_browser(
        self, mock_playwright: dict, tmp_path: Path
    ) -> None:
        """Change the default browser and the old profile is still a live login.

        Clearing only the browser this run drives leaves the previous user
        signed in behind the old default, which --fresh-login exists to stop.
        """
        stale = tmp_path / "profiles" / "firefox-moz-firefox" / "localhost_9002"
        stale.mkdir(parents=True)
        (stale / "cookies.sqlite").write_text("the previous user")

        self._login(fresh_login=True)

        assert not stale.exists()

    def test_fresh_login_reports_a_profile_it_cannot_remove(
        self, mock_playwright: dict, tmp_path: Path
    ) -> None:
        """Launching on a half-deleted profile silently returns the old login."""
        stale = tmp_path / "profiles" / "firefox-moz-firefox" / "localhost_9002"
        stale.mkdir(parents=True)

        with (
            patch("shutil.rmtree", side_effect=OSError("permission denied")),
            patch("datahub.cli.sso_cli.requests", mock_requests()),
            pytest.raises(click.ClickException, match="could not remove"),
        ):
            browser_sso_login("http://localhost:9002", "ONE_HOUR", fresh_login=True)

    def test_a_locked_profile_is_reported_not_worked_around(
        self, mock_playwright: dict
    ) -> None:
        """A concurrent run holds the lock. Say so rather than degrade quietly.

        TestAllBrowsersFail covers what the message has to contain.
        """
        pw = mock_playwright["playwright"]
        pw.chromium.launch_persistent_context.side_effect = Exception(
            "ProcessSingleton: profile is already in use"
        )

        with pytest.raises(click.ClickException):
            browser_sso_login("http://localhost:9002", "ONE_HOUR")

        pw.chromium.launch.assert_not_called()


class TestBrowserTarget:
    @pytest.mark.parametrize(
        ("handler", "engine", "channel"),
        [
            ("org.mozilla.firefox", "firefox", "moz-firefox"),
            ("firefox.desktop", "firefox", "moz-firefox"),
            ("com.google.chrome", "chromium", "chrome"),
            ("ChromeHTML", "chromium", "chrome"),
            ("MSEdgeHTM", "chromium", "msedge"),
        ],
    )
    def test_resolves_the_os_handler_to_engine_and_channel(
        self, handler: str, engine: str, channel: str
    ) -> None:
        """A channel means the installation the user already has."""
        with patch("datahub.cli.sso_cli._os_browser_handler", return_value=handler):
            target = browser_target()

        assert (target.engine, target.channel) == (engine, channel)

    @pytest.mark.parametrize("handler", [None, "", "com.apple.Safari"])
    def test_falls_back_to_the_bundled_build(self, handler: Optional[str]) -> None:
        """Safari and unknown handlers cannot be driven."""
        with patch("datahub.cli.sso_cli._os_browser_handler", return_value=handler):
            assert browser_target() == BUNDLED_TARGET


class TestLaunchTargets:
    def test_drops_the_channel_before_changing_engine(self) -> None:
        """Without that browser installed, stay on its engine rather than switch."""
        target = BrowserTarget(engine="firefox", channel="moz-firefox")

        assert _launch_targets(target) == [
            target,
            BrowserTarget(engine="firefox", channel=None),
            BUNDLED_TARGET,
        ]

    def test_a_chromium_channel_needs_no_extra_step(self) -> None:
        target = BrowserTarget(engine="chromium", channel="chrome")

        assert _launch_targets(target) == [target, BUNDLED_TARGET]

    def test_the_bundled_target_is_not_repeated(self) -> None:
        assert _launch_targets(BUNDLED_TARGET) == [BUNDLED_TARGET]


class TestBrowserSelection:
    def _login(self, **kwargs: Any) -> None:
        with patch("datahub.cli.sso_cli.requests", mock_requests()):
            try:
                browser_sso_login("http://localhost:9002", "ONE_HOUR", **kwargs)
            except Exception:
                pass

    def test_drives_the_engine_the_os_default_names(
        self, mock_playwright: dict
    ) -> None:
        with patch(
            "datahub.cli.sso_cli.browser_target",
            return_value=BrowserTarget(engine="firefox", channel="moz-firefox"),
        ):
            self._login()

        pw = mock_playwright["playwright"]
        launch = pw.firefox.launch_persistent_context
        launch.assert_called_once()
        assert launch.call_args[1]["channel"] == "moz-firefox"
        pw.chromium.launch_persistent_context.assert_not_called()

    def test_the_bundled_target_passes_no_channel(self, mock_playwright: dict) -> None:
        """Playwright selects its own build by receiving no channel at all."""
        with patch("datahub.cli.sso_cli.browser_target", return_value=BUNDLED_TARGET):
            self._login()

        _, kwargs = mock_playwright[
            "playwright"
        ].chromium.launch_persistent_context.call_args
        assert "channel" not in kwargs

    def test_falls_back_within_the_engine_first(self, mock_playwright: dict) -> None:
        pw = mock_playwright["playwright"]
        pw.firefox.launch_persistent_context.side_effect = [
            Exception("not installed"),
            mock_playwright["context"],
        ]

        with patch(
            "datahub.cli.sso_cli.browser_target",
            return_value=BrowserTarget(engine="firefox", channel="moz-firefox"),
        ):
            self._login()

        assert pw.firefox.launch_persistent_context.call_count == 2
        assert "channel" not in pw.firefox.launch_persistent_context.call_args[1]
        pw.chromium.launch_persistent_context.assert_not_called()

    def test_profiles_are_separated_per_engine(self) -> None:
        """A Firefox profile and a Chromium profile are not interchangeable."""
        url = "https://acme.example.com"

        assert _sso_profile_dir(url, False, "firefox") != _sso_profile_dir(
            url, False, "chromium"
        )


class TestSeedProfile:
    def _login(self, **kwargs: Any) -> None:
        with patch("datahub.cli.sso_cli.requests", mock_requests()):
            try:
                browser_sso_login("http://localhost:9002", "ONE_HOUR", **kwargs)
            except Exception:
                pass

    @pytest.fixture
    def seed(self, tmp_path: Path) -> Path:
        source = tmp_path / "real-profile"
        (source / "storage").mkdir(parents=True)
        (source / "cookies.sqlite").write_text("session-cookies")
        (source / "storage" / "ls.db").write_text("local-storage")
        (source / ".parentlock").write_text("lock")
        (source / "compatibility.ini").write_text("pinned to another build")
        (source / "cache2").mkdir()
        (source / "cache2" / "big").write_text("x" * 100)
        return source

    def test_seeds_an_empty_profile(
        self, mock_playwright: dict, seed: Path, tmp_path: Path
    ) -> None:
        """An existing session in the seed is what skips the very first login."""
        self._login(seed_profile=str(seed))

        dest = tmp_path / "profiles" / "firefox-moz-firefox" / "localhost_9002"
        assert (dest / "cookies.sqlite").read_text() == "session-cookies"
        assert (dest / "storage" / "ls.db").read_text() == "local-storage"

    @pytest.mark.parametrize("excluded", [".parentlock", "compatibility.ini", "cache2"])
    def test_locks_and_caches_are_not_copied(
        self, mock_playwright: dict, seed: Path, tmp_path: Path, excluded: str
    ) -> None:
        """A copied lock pins the profile to the browser that wrote it."""
        self._login(seed_profile=str(seed))

        dest = tmp_path / "profiles" / "firefox-moz-firefox" / "localhost_9002"
        assert not (dest / excluded).exists()

    def test_does_not_overwrite_a_saved_session(
        self, mock_playwright: dict, seed: Path, tmp_path: Path
    ) -> None:
        """Re-seeding would discard the session the previous run just saved."""
        dest = tmp_path / "profiles" / "firefox-moz-firefox" / "localhost_9002"
        dest.mkdir(parents=True)
        (dest / "cookies.sqlite").write_text("newer-session")

        self._login(seed_profile=str(seed))

        assert (dest / "cookies.sqlite").read_text() == "newer-session"

    def test_fresh_login_reseeds(
        self, mock_playwright: dict, seed: Path, tmp_path: Path
    ) -> None:
        """--fresh-login empties the directory, so the seed applies again."""
        dest = tmp_path / "profiles" / "firefox-moz-firefox" / "localhost_9002"
        dest.mkdir(parents=True)
        (dest / "cookies.sqlite").write_text("stale-session")

        self._login(seed_profile=str(seed), fresh_login=True)

        assert (dest / "cookies.sqlite").read_text() == "session-cookies"

    def test_a_seed_from_another_engine_is_refused(
        self, mock_playwright: dict, tmp_path: Path
    ) -> None:
        """A Chromium profile in a Firefox directory is not a working profile.

        The engine guard only kept seeds out of the *fallback* browsers, so a
        seed for the wrong engine was still copied into the one being driven.
        """
        chromium_seed = tmp_path / "chrome-profile"
        (chromium_seed / "Default").mkdir(parents=True)
        (chromium_seed / "Local State").write_text("{}")

        with pytest.raises(click.UsageError, match="chromium profile"):
            browser_sso_login(
                "http://localhost:9002",
                "ONE_HOUR",
                seed_profile=str(chromium_seed),
            )

        dest = tmp_path / "profiles" / "firefox-moz-firefox" / "localhost_9002"
        assert not (dest / "Local State").exists()

    def test_a_seed_of_no_known_engine_is_allowed(
        self, mock_playwright: dict, tmp_path: Path
    ) -> None:
        """Refusing an unfamiliar layout would block a directory that works."""
        seed = tmp_path / "unknown-profile"
        seed.mkdir()
        (seed / "something.dat").write_text("session")

        self._login(seed_profile=str(seed))

        dest = tmp_path / "profiles" / "firefox-moz-firefox" / "localhost_9002"
        assert (dest / "something.dat").exists()

    def test_missing_seed_is_a_usage_error(
        self, mock_playwright: dict, tmp_path: Path
    ) -> None:
        with pytest.raises(click.UsageError, match="not a directory"):
            browser_sso_login(
                "http://localhost:9002",
                "ONE_HOUR",
                seed_profile=str(tmp_path / "nope"),
            )

    def test_seed_is_not_applied_to_the_fallback_browser(
        self, mock_playwright: dict, seed: Path, tmp_path: Path
    ) -> None:
        """A Firefox profile copied into a Chromium directory would corrupt it."""
        pw = mock_playwright["playwright"]
        pw.firefox.launch_persistent_context.side_effect = Exception("no firefox")

        self._login(seed_profile=str(seed))

        chromium_dir = tmp_path / "profiles" / "chromium" / "localhost_9002"
        assert chromium_dir.is_dir(), "fallback should still prepare its own dir"
        assert not (chromium_dir / "cookies.sqlite").exists()

    def test_seed_is_not_applied_to_a_fallback_on_the_same_engine(
        self, mock_playwright: dict, seed: Path, tmp_path: Path
    ) -> None:
        """Same engine is still a different build.

        Stock Firefox and the build Playwright ships get separate directories
        precisely because a profile does not survive the crossing. Seeding the
        fallback copies a session it cannot read and reports success.
        """
        pw = mock_playwright["playwright"]
        pw.firefox.launch_persistent_context.side_effect = [
            Exception("moz-firefox is not installed"),
            mock_playwright["context"],
        ]

        self._login(seed_profile=str(seed))

        bundled = tmp_path / "profiles" / "firefox" / "localhost_9002"
        assert bundled.is_dir()
        assert not (bundled / "cookies.sqlite").exists()

    def test_an_inner_chromium_profile_is_refused(
        self, mock_playwright: dict, tmp_path: Path
    ) -> None:
        """Chromium is launched against the directory above `Default`.

        Seeding `Default` itself copies cleanly, puts the cookies a level too
        high for the browser to read, and skips no login.
        """
        inner = tmp_path / "Default"
        inner.mkdir()
        (inner / "Preferences").write_text("{}")
        (inner / "Cookies").write_text("jar")

        with (
            patch("datahub.cli.sso_cli.browser_target", return_value=BUNDLED_TARGET),
            pytest.raises(click.UsageError, match="single Chromium profile"),
        ):
            browser_sso_login(
                "http://localhost:9002", "ONE_HOUR", seed_profile=str(inner)
            )

    def test_a_chromium_user_data_root_is_accepted(
        self, mock_playwright: dict, tmp_path: Path
    ) -> None:
        root = tmp_path / "chrome-user-data"
        (root / "Default").mkdir(parents=True)
        (root / "Local State").write_text("{}")
        (root / "Default" / "Cookies").write_text("jar")

        with (
            patch("datahub.cli.sso_cli.browser_target", return_value=BUNDLED_TARGET),
            patch("datahub.cli.sso_cli.requests", mock_requests()),
        ):
            browser_sso_login(
                "http://localhost:9002", "ONE_HOUR", seed_profile=str(root)
            )

        dest = tmp_path / "profiles" / "chromium" / "localhost_9002"
        assert (dest / "Default" / "Cookies").read_text() == "jar"


class TestAllBrowsersFail:
    def test_raises_instead_of_silently_switching_browser(
        self, mock_playwright: dict
    ) -> None:
        """Every candidate failing usually means a concurrent run holds the lock.

        Falling back to a throwaway profile would hand back a different browser
        with no saved session and no explanation, so say what was tried instead.
        """
        pw = mock_playwright["playwright"]
        missing = Exception("Executable doesn't exist at /ms-playwright/firefox")
        pw.firefox.launch_persistent_context.side_effect = missing
        pw.chromium.launch_persistent_context.side_effect = missing

        with (
            patch(
                "datahub.cli.sso_cli.browser_target",
                return_value=BrowserTarget(engine="firefox", channel="moz-firefox"),
            ),
            pytest.raises(click.ClickException) as err,
        ):
            browser_sso_login("http://localhost:9002", "ONE_HOUR")

        message = str(err.value)
        assert "moz-firefox" in message
        assert "bundled firefox" in message
        assert "bundled chromium" in message
        assert "Executable doesn't exist" in message
        # Only the builds Playwright has to download, and only those tried.
        assert "playwright install chromium firefox" in message
        pw.chromium.launch.assert_not_called()

    def test_a_driveable_default_needs_no_download(self) -> None:
        """Chrome, Edge and Firefox are driven where they already are.

        Telling every user to run `playwright install chromium` asks for a
        150MB download that only a machine defaulting to Safari — or to
        something Playwright cannot drive — actually needs.
        """
        for handler in ("com.google.chrome", "MSEdgeHTM", "org.mozilla.firefox"):
            with patch("datahub.cli.sso_cli._os_browser_handler", return_value=handler):
                assert browser_target().channel is not None

    def test_no_install_advice_when_no_browser_was_reported_missing(
        self, mock_playwright: dict
    ) -> None:
        """`playwright install` fixes a browser Playwright does not have.

        A build that is present but will not start needs its own error read.
        Advising an install there sends the user to a command that reports
        nothing to do, and buries the reason underneath it.
        """
        pw = mock_playwright["playwright"]
        broken = Exception("Failed to launch: libnss3.so: cannot open shared object")
        pw.firefox.launch_persistent_context.side_effect = broken
        pw.chromium.launch_persistent_context.side_effect = broken

        with pytest.raises(click.ClickException) as err:
            browser_sso_login("http://localhost:9002", "ONE_HOUR")

        message = str(err.value)
        assert "playwright install" not in message
        assert "libnss3.so" in message

    def test_install_advice_names_only_the_browsers_reported_missing(
        self, mock_playwright: dict
    ) -> None:
        pw = mock_playwright["playwright"]
        pw.firefox.launch_persistent_context.side_effect = Exception(
            "Failed to launch: libnss3.so: cannot open shared object"
        )
        pw.chromium.launch_persistent_context.side_effect = Exception(
            "Executable doesn't exist at /ms-playwright/chromium-1234/chrome"
        )

        with pytest.raises(click.ClickException) as err:
            browser_sso_login("http://localhost:9002", "ONE_HOUR")

        # Not "chromium firefox": firefox is installed, it just would not start.
        assert str(err.value).rstrip().endswith("playwright install chromium")

    def test_a_profile_problem_stops_the_chain(self, mock_playwright: dict) -> None:
        """Every candidate would hit the same lock, so trying them all is noise.

        Worse, it ends on bundled Chromium with an empty profile: a browser the
        user did not pick, with no saved session and no reason given.
        """
        pw = mock_playwright["playwright"]
        pw.firefox.launch_persistent_context.side_effect = Exception(
            "ProcessSingleton: the profile is already in use"
        )

        with (
            patch(
                "datahub.cli.sso_cli.browser_target",
                return_value=BrowserTarget(engine="firefox", channel="moz-firefox"),
            ),
            pytest.raises(click.ClickException, match="already in use"),
        ):
            browser_sso_login("http://localhost:9002", "ONE_HOUR")

        assert pw.firefox.launch_persistent_context.call_count == 1
        pw.chromium.launch_persistent_context.assert_not_called()


class TestCookieScoping:
    @pytest.mark.parametrize(
        ("domain", "expected"),
        [
            ("dev01.acryl.io", True),
            (".dev01.acryl.io", True),
            (".acryl.io", True),
            # Host-only: acryl.io set it for itself, not for its subdomains.
            ("acryl.io", False),
            ("taxact.acryl.io", False),
            ("acryl.okta.com", False),
            ("localhost", False),
            ("", False),
        ],
    )
    def test_only_cookies_this_host_would_receive(
        self, domain: str, expected: bool
    ) -> None:
        assert (
            _cookie_applies_to({"domain": domain}, "https://dev01.acryl.io") is expected
        )

    @pytest.mark.parametrize(
        ("path", "expected"),
        [
            ("/", True),
            ("/api", True),
            ("/api/v2/graphql", True),
            ("/api/v3", False),
            ("/authenticate", False),
        ],
    )
    def test_only_cookies_the_api_path_would_receive(
        self, path: str, expected: bool
    ) -> None:
        """The actor URN is read from this set, so a scoped cookie must not join."""
        cookie = {"domain": "dev01.acryl.io", "path": path}

        assert _cookie_applies_to(cookie, "https://dev01.acryl.io") is expected

    def test_a_secure_cookie_is_not_read_over_plain_http(self) -> None:
        cookie = {"domain": "localhost", "path": "/", "secure": True}

        assert not _cookie_applies_to(cookie, "http://localhost:9002")
        assert _cookie_applies_to(cookie, "https://localhost:9002")

    def test_actor_is_not_taken_from_another_tenant(
        self, mock_playwright: dict
    ) -> None:
        """A seeded profile holds actor cookies for every tenant it has visited.

        Reading the wrong one would report the wrong user and mint a token
        against their URN.
        """
        mock_playwright["context"].cookies.return_value = [
            {
                "name": "actor",
                "value": "urn%3Ali%3Acorpuser%3Asomeone.else",
                "domain": "taxact.acryl.io",
                "path": "/",
            },
            {
                "name": "actor",
                "value": "urn%3Ali%3Acorpuser%3Apedro",
                "domain": "dev01.acryl.io",
                "path": "/",
            },
        ]

        module = mock_requests(me=None)
        with patch("datahub.cli.sso_cli.requests", module):
            browser_sso_login("https://dev01.acryl.io", "ONE_HOUR")

        session = module.Session.return_value
        create_call = graphql_call(session, "createAccessToken")
        assert (
            create_call[1]["json"]["variables"]["input"]["actorUrn"]
            == "urn:li:corpuser:pedro"
        )
        assert session.cookies.set.call_count == 1


class TestRememberSession:
    """Covers carrying an in-memory identity provider session across runs.

    A provider that issues a session cookie with no expiry ends the session when
    the browser exits, so the login repeats no matter how well the profile is
    reused. Storing those cookies ourselves is the only way across.
    """

    def _login(self, **kwargs: Any) -> None:
        with patch("datahub.cli.sso_cli.requests", mock_requests()):
            try:
                browser_sso_login("https://dev01.acryl.io", "ONE_HOUR", **kwargs)
            except Exception:
                pass

    @pytest.fixture(autouse=True)
    def _sessions_in_tmp(self, tmp_path: Path) -> Iterator[Path]:
        with patch("datahub.cli.sso_cli.SSO_SESSION_ROOT", tmp_path / "sessions"):
            yield tmp_path / "sessions"

    def _cookies(self, mock_playwright: dict, after: list) -> None:
        mock_playwright["context"].cookies.return_value = after

    @pytest.fixture(autouse=True)
    def _redirects_via_okta(self, mock_playwright: dict) -> None:
        """A MagicMock never fires framenavigated, so stand in for the redirect.

        Host collection is what decides which cookies are saved, so a test that
        skipped this would never exercise the identity provider hop at all.
        """

        def fire(event: str, callback: Callable[[Any], None]) -> None:
            if event == "framenavigated":
                frame = MagicMock()
                frame.url = "https://acryl.okta.com/oauth2/v1/authorize"
                frame.parent_frame = None
                callback(frame)

        mock_playwright["page"].on.side_effect = fire

    def test_saves_cookies_for_every_host_the_flow_touched(
        self, mock_playwright: dict, _sessions_in_tmp: Path
    ) -> None:
        """The instance and the identity provider both hold part of the login."""
        established = [
            {"domain": "acryl.okta.com", "name": "sid", "value": "s", "path": "/"},
            {"domain": "dev01.acryl.io", "name": "actor", "value": "u", "path": "/"},
        ]
        self._cookies(mock_playwright, established)

        self._login(remember_session=True)

        saved = json.loads((_sessions_in_tmp / "dev01.acryl.io.json").read_text())
        assert {c["name"] for c in saved} == {"sid", "actor"}

    def test_background_cookies_are_not_saved(
        self, mock_playwright: dict, _sessions_in_tmp: Path
    ) -> None:
        """A seeded profile holds thousands of unrelated cookies."""
        noise = {"domain": ".google.com", "name": "SID", "value": "x", "path": "/"}
        fresh = {"domain": "acryl.okta.com", "name": "sid", "value": "s", "path": "/"}
        self._cookies(mock_playwright, [noise, fresh])

        self._login(remember_session=True)

        saved = json.loads((_sessions_in_tmp / "dev01.acryl.io.json").read_text())
        assert [c["name"] for c in saved] == ["sid"]

    def test_a_subframe_does_not_widen_what_is_saved(
        self, mock_playwright: dict, _sessions_in_tmp: Path
    ) -> None:
        """A tracker in an iframe is not part of the login.

        Counting every frame host put its whole domain in scope, so an unrelated
        cookie the login never touched was written to disk in plaintext.
        """

        def fire(event: str, callback: Callable[[Any], None]) -> None:
            if event != "framenavigated":
                return
            for url, parent in (
                ("https://acryl.okta.com/oauth2/v1/authorize", None),
                ("https://ads.example.com/pixel", MagicMock()),
            ):
                frame = MagicMock()
                frame.url = url
                frame.parent_frame = parent
                callback(frame)

        mock_playwright["page"].on.side_effect = fire
        tracker = {
            "domain": "ads.example.com",
            "name": "uid",
            "value": "t",
            "path": "/",
        }
        okta = {"domain": "acryl.okta.com", "name": "sid", "value": "s", "path": "/"}
        # Both look unchanged, so only the navigation signal can pull them in.
        mock_playwright["context"].cookies.return_value = [tracker, okta]

        self._login(remember_session=True)

        saved = json.loads((_sessions_in_tmp / "dev01.acryl.io.json").read_text())
        assert [c["name"] for c in saved] == ["sid"]

    def test_a_run_that_creates_nothing_does_not_clobber_the_session(
        self, mock_playwright: dict, _sessions_in_tmp: Path
    ) -> None:
        """Regression: a run served by the browser profile creates almost nothing.

        Saving only that run's new cookies replaced a good saved session with a
        single analytics cookie, and the next run had nothing to restore.
        """
        _sessions_in_tmp.mkdir(parents=True)
        path = _sessions_in_tmp / "dev01.acryl.io.json"
        path.write_text(
            json.dumps(
                [{"domain": "acryl.okta.com", "name": "sid", "value": "s", "path": "/"}]
            )
        )
        self._cookies(
            mock_playwright,
            [
                {"domain": "acryl.okta.com", "name": "sid", "value": "s", "path": "/"},
                {"domain": "dev01.acryl.io", "name": "bid", "value": "b", "path": "/"},
            ],
        )

        self._login(remember_session=True)

        saved = json.loads(path.read_text())
        assert "sid" in {c["name"] for c in saved}, "must not lose the provider session"

    def test_session_file_is_owner_only(
        self, mock_playwright: dict, _sessions_in_tmp: Path
    ) -> None:
        """It holds a live identity provider session."""
        self._cookies(
            mock_playwright,
            [{"domain": "acryl.okta.com", "name": "sid", "value": "s", "path": "/"}],
        )

        self._login(remember_session=True)

        path = _sessions_in_tmp / "dev01.acryl.io.json"
        assert path.stat().st_mode & 0o777 == 0o600

    def test_an_existing_session_file_is_tightened_too(
        self, mock_playwright: dict, _sessions_in_tmp: Path
    ) -> None:
        """O_CREAT applies its mode on create only, so a re-save kept 0644."""
        _sessions_in_tmp.mkdir(parents=True)
        path = _sessions_in_tmp / "dev01.acryl.io.json"
        path.write_text("[]")
        path.chmod(0o644)
        self._cookies(
            mock_playwright,
            [{"domain": "acryl.okta.com", "name": "sid", "value": "s", "path": "/"}],
        )

        self._login(remember_session=True)

        assert path.stat().st_mode & 0o777 == 0o600

    def test_a_failed_write_is_not_reported_as_remembered(
        self,
        mock_playwright: dict,
        _sessions_in_tmp: Path,
        capsys: pytest.CaptureFixture,
    ) -> None:
        """Claiming a session was stored sends the user back for no reason."""
        self._cookies(
            mock_playwright,
            [{"domain": "acryl.okta.com", "name": "sid", "value": "s", "path": "/"}],
        )

        with patch("datahub.cli.sso_cli.os.open", side_effect=OSError("disk full")):
            self._login(remember_session=True)

        out = capsys.readouterr().out
        assert "Remembered" not in out
        assert "Could not remember this session" in out

    def test_restores_on_the_next_run(
        self, mock_playwright: dict, _sessions_in_tmp: Path
    ) -> None:
        saved = [{"domain": "acryl.okta.com", "name": "sid", "value": "s", "path": "/"}]
        _sessions_in_tmp.mkdir(parents=True)
        (_sessions_in_tmp / "dev01.acryl.io.json").write_text(json.dumps(saved))

        self._login(remember_session=True)

        mock_playwright["context"].add_cookies.assert_called_once_with(saved)

    def test_nothing_is_written_without_the_flag(
        self, mock_playwright: dict, _sessions_in_tmp: Path
    ) -> None:
        """Persisting a provider session must stay opt-in."""
        self._cookies(
            mock_playwright,
            [{"domain": "acryl.okta.com", "name": "sid", "value": "s", "path": "/"}],
        )

        self._login()

        assert not _sessions_in_tmp.exists()
        mock_playwright["context"].add_cookies.assert_not_called()

    def test_fresh_login_forgets_the_saved_session(
        self, mock_playwright: dict, _sessions_in_tmp: Path
    ) -> None:
        _sessions_in_tmp.mkdir(parents=True)
        path = _sessions_in_tmp / "dev01.acryl.io.json"
        path.write_text(json.dumps([{"domain": "x", "name": "sid", "value": "old"}]))

        self._login(remember_session=True, fresh_login=True)

        mock_playwright["context"].add_cookies.assert_not_called()

    def test_a_corrupt_session_file_is_ignored(
        self, mock_playwright: dict, _sessions_in_tmp: Path
    ) -> None:
        _sessions_in_tmp.mkdir(parents=True)
        (_sessions_in_tmp / "dev01.acryl.io.json").write_text("{not json")

        self._login(remember_session=True)

        mock_playwright["context"].add_cookies.assert_not_called()


class TestSilentLogin:
    """A valid saved session only needs a redirect chain, so show no window."""

    @pytest.fixture(autouse=True)
    def _sessions_in_tmp(self, tmp_path: Path) -> Iterator[Path]:
        root = tmp_path / "sessions"
        with patch("datahub.cli.sso_cli.SSO_SESSION_ROOT", root):
            yield root

    def _save(self, root: Path) -> list:
        root.mkdir(parents=True, exist_ok=True)
        saved = [{"domain": "acryl.okta.com", "name": "sid", "value": "s", "path": "/"}]
        (root / "dev01.acryl.io.json").write_text(json.dumps(saved))
        return saved

    def _login(
        self, me: Optional[str] = "urn:li:corpuser:pedro", **kwargs: Any
    ) -> None:
        with patch("datahub.cli.sso_cli.requests", mock_requests(me=me)):
            try:
                browser_sso_login("https://dev01.acryl.io", "ONE_HOUR", **kwargs)
            except Exception:
                pass

    def _headless_flags(self, mock_playwright: dict) -> list:
        return [
            call[1]["headless"]
            for call in mock_playwright[
                "playwright"
            ].firefox.launch_persistent_context.call_args_list
        ]

    def test_no_window_when_the_saved_session_works(
        self, mock_playwright: dict, _sessions_in_tmp: Path
    ) -> None:
        saved = self._save(_sessions_in_tmp)
        mock_playwright["context"].cookies.return_value = [
            {
                "domain": "dev01.acryl.io",
                "name": "actor",
                "value": "urn%3Ali%3Acorpuser%3Apedro",
                "path": "/",
            },
        ]

        module = mock_requests()
        with patch("datahub.cli.sso_cli.requests", module):
            browser_sso_login(
                "https://dev01.acryl.io", "ONE_HOUR", remember_session=True
            )

        assert self._headless_flags(mock_playwright) == [True], (
            "a working session must not open a second, visible browser"
        )
        mock_playwright["context"].add_cookies.assert_called_once_with(saved)
        graphql_call(module.Session.return_value, "createAccessToken")

    def test_an_expired_saved_session_still_opens_a_browser(
        self, mock_playwright: dict, _sessions_in_tmp: Path
    ) -> None:
        """The regression: the replayed actor cookie is not proof of a login.

        --remember-session saves the instance's own actor cookie and replays it
        before the flow runs, so the wait for that cookie passes the instant the
        page loads whether or not the provider session behind it is still alive.
        Only the instance can settle it, and here it says nobody is signed in.
        """
        self._save(_sessions_in_tmp)
        mock_playwright["context"].cookies.return_value = [
            {
                "domain": "dev01.acryl.io",
                "name": "actor",
                "value": "urn%3Ali%3Acorpuser%3Astale",
                "path": "/",
            },
        ]

        self._login(me=None, remember_session=True)

        assert self._headless_flags(mock_playwright) == [True, False], (
            "an expired session must hand over to a visible login"
        )

    def test_falls_back_to_a_visible_browser(
        self, mock_playwright: dict, _sessions_in_tmp: Path
    ) -> None:
        """An expired session has to hand over to a human."""
        self._save(_sessions_in_tmp)
        mock_playwright["page"].wait_for_function.side_effect = [
            Exception("Timeout"),
            None,
        ]

        self._login(remember_session=True)

        assert self._headless_flags(mock_playwright) == [True, False]

    def test_a_crashing_silent_attempt_still_opens_a_browser(
        self, mock_playwright: dict, _sessions_in_tmp: Path
    ) -> None:
        """Not every silent failure is a timeout.

        An identity provider redirecting to a custom scheme, a TLS error, a
        blocked headless launch — each raises rather than returning, and each
        used to kill the run instead of handing over to a visible login.
        """
        self._save(_sessions_in_tmp)
        mock_playwright["page"].goto.side_effect = [
            Exception("net::ERR_ABORTED"),
            None,
        ]

        self._login(remember_session=True)

        assert self._headless_flags(mock_playwright) == [True, False]

    def test_the_visible_attempt_does_not_swallow_its_error(
        self, mock_playwright: dict
    ) -> None:
        """There is nothing behind it, so failing quietly would just hang."""
        mock_playwright["page"].goto.side_effect = Exception("net::ERR_ABORTED")

        with (
            patch("datahub.cli.sso_cli.requests", mock_requests()),
            pytest.raises(Exception, match="ERR_ABORTED"),
        ):
            browser_sso_login("https://dev01.acryl.io", "ONE_HOUR")

    def test_no_silent_attempt_without_a_saved_session(
        self, mock_playwright: dict
    ) -> None:
        """There is nothing to prove silently, so do not add the delay."""
        self._login(remember_session=True)

        assert self._headless_flags(mock_playwright) == [False]

    def test_fresh_login_always_shows_the_browser(
        self, mock_playwright: dict, _sessions_in_tmp: Path
    ) -> None:
        self._save(_sessions_in_tmp)

        self._login(remember_session=True, fresh_login=True)

        assert self._headless_flags(mock_playwright) == [False]

    def test_silent_attempt_is_short(
        self, mock_playwright: dict, _sessions_in_tmp: Path
    ) -> None:
        """Its timeout is added to the wait before a real login can start."""
        self._save(_sessions_in_tmp)
        mock_playwright["page"].wait_for_function.side_effect = [
            Exception("Timeout"),
            None,
        ]

        self._login(remember_session=True, timeout_ms=120_000)

        timeouts = [
            call[1]["timeout"]
            for call in mock_playwright["page"].wait_for_function.call_args_list
        ]
        assert timeouts == [SILENT_ATTEMPT_TIMEOUT_MS, 120_000]
        assert SILENT_ATTEMPT_TIMEOUT_MS < 120_000


class TestSessionCookieSelection:
    """The three signals in _session_cookies_for, isolated from the browser."""

    OKTA = {"domain": "acryl.okta.com", "name": "sid", "value": "s", "path": "/"}
    INSTANCE = {"domain": "dev01.acryl.io", "name": "bid", "value": "b", "path": "/"}

    def test_hosts_the_flow_visited_are_in_scope(self) -> None:
        keep = _session_cookies_for(
            [self.OKTA, self.INSTANCE],
            hosts={"dev01.acryl.io", "acryl.okta.com"},
            before=set(),
            previous=[],
        )
        assert {c["name"] for c in keep} == {"sid", "bid"}

    def test_a_changed_cookie_pulls_its_host_into_scope(self) -> None:
        """Covers a navigation going unobserved: the cookie still betrays it."""
        keep = _session_cookies_for(
            [self.OKTA],
            hosts={"dev01.acryl.io"},
            before=set(),
            previous=[],
        )
        assert [c["name"] for c in keep] == ["sid"]

    def test_previous_survives_a_run_that_changes_nothing(self) -> None:
        """The regression: a run served from the profile must not erase the session."""
        unchanged = {(self.OKTA["domain"], self.OKTA["name"], self.OKTA["value"])}
        keep = _session_cookies_for(
            [self.OKTA],
            hosts={"dev01.acryl.io"},
            before=unchanged,
            previous=[self.OKTA],
        )
        assert [c["name"] for c in keep] == ["sid"]

    def test_out_of_scope_cookies_stay_out(self) -> None:
        noise = {"domain": ".google.com", "name": "SID", "value": "x", "path": "/"}
        unchanged = {(noise["domain"], noise["name"], noise["value"])}
        keep = _session_cookies_for(
            [noise], hosts={"dev01.acryl.io"}, before=unchanged, previous=[]
        )
        assert keep == []

    def test_a_newer_value_replaces_the_saved_one(self) -> None:
        refreshed = {**self.OKTA, "value": "rotated"}
        keep = _session_cookies_for(
            [refreshed],
            hosts={"acryl.okta.com"},
            before=set(),
            previous=[self.OKTA],
        )
        assert [c["value"] for c in keep] == ["rotated"]
