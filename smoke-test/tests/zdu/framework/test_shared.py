"""Unit tests for ``phases/_shared.py`` — helper utilities shared across phases."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pytest

from tests.utilities.domains import Domain
from tests.zdu.framework.constants import TOKEN_SERVICE_KEYS
from tests.zdu.framework.phases._shared import old_image_window, read_token_passthrough

pytestmark = pytest.mark.domain(Domain.PLATFORM)


class TestReadTokenPassthrough:
    """R4 / D1 — five phases used to duplicate this pattern. Consolidate the
    expected behavior here.
    """

    def test_returns_env_when_all_keys_present(self) -> None:
        docker = MagicMock()
        docker.get_service_env.return_value = {
            "DATAHUB_TOKEN_SERVICE_SIGNING_KEY": "sigkey",
            "DATAHUB_TOKEN_SERVICE_SALT": "salt",
        }
        out = read_token_passthrough(docker, "gms-svc", purpose="test-call-site")
        assert out == {
            "DATAHUB_TOKEN_SERVICE_SIGNING_KEY": "sigkey",
            "DATAHUB_TOKEN_SERVICE_SALT": "salt",
        }
        # Underlying call used the canonical key list.
        docker.get_service_env.assert_called_once_with(
            "gms-svc", list(TOKEN_SERVICE_KEYS)
        )

    def test_returns_empty_dict_when_get_service_env_returns_empty(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        docker = MagicMock()
        docker.get_service_env.return_value = {}
        with caplog.at_level(logging.WARNING):
            out = read_token_passthrough(docker, "gms-svc", purpose="some-phase")
        assert out == {}
        # Warning must surface the call site (``purpose``) so a triager can
        # tell which phase missed the secrets without grepping each one.
        assert any(
            "some-phase" in r.message and "gms-svc" in r.message for r in caplog.records
        )

    def test_warns_on_partial_keys_but_still_returns_what_was_captured(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        # Only SIGNING_KEY captured; SALT missing.
        docker = MagicMock()
        docker.get_service_env.return_value = {
            "DATAHUB_TOKEN_SERVICE_SIGNING_KEY": "sigkey",
        }
        with caplog.at_level(logging.WARNING):
            out = read_token_passthrough(docker, "gms-svc", purpose="partial-test")
        # Returns whatever was captured — caller decides whether to abort.
        assert out == {"DATAHUB_TOKEN_SERVICE_SIGNING_KEY": "sigkey"}
        # Warning identifies the missing key by name.
        assert any(
            "partial-test" in r.message and "DATAHUB_TOKEN_SERVICE_SALT" in r.message
            for r in caplog.records
        )

    def test_passes_purpose_into_log_message(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        # Distinct purposes from different phases must each appear in their
        # respective warnings — proves the centralized helper hasn't dropped
        # the call-site context.
        docker = MagicMock()
        docker.get_service_env.return_value = {}
        with caplog.at_level(logging.WARNING):
            read_token_passthrough(docker, "gms-svc", purpose="rolling_restart")
            read_token_passthrough(docker, "gms-svc", purpose="cleanup")
        purposes_logged = {r.message for r in caplog.records}
        assert any("rolling_restart" in m for m in purposes_logged)
        assert any("cleanup" in m for m in purposes_logged)


def _docker_with(services: list[str]) -> MagicMock:
    d = MagicMock()
    d.get_service_env.return_value = {
        "DATAHUB_TOKEN_SERVICE_SIGNING_KEY": "sigkey",
        "DATAHUB_TOKEN_SERVICE_SALT": "salt",
    }
    d.get_all_service_images.return_value = {s: "img" for s in services}
    return d


def _recreated(docker: MagicMock) -> list[str]:
    return [c.kwargs["service"] for c in docker.recreate_service.call_args_list]


class TestOldImageWindowCoversTheWritePath:
    """The window exists to keep the sweep fixture un-migrated.

    Swapping GMS alone achieves that only where the consumers run inside the
    GMS process. On a split topology they are separate containers, and a
    GMS-only swap leaves them on NEW with the mutator chain armed — draining
    the fixture before the sweep's first batch, which reads as a sweep timing
    failure rather than as a topology bug.
    """

    def test_swaps_consumers_as_well_as_gms(self) -> None:
        docker = _docker_with(["gms", "mae", "mce"])
        with old_image_window(
            docker,
            gms_service="gms",
            consumer_services=("mae", "mce"),
            old_image_tag="zdu-old",
            new_image_tag="zdu-new",
            build_images_root="nonexistent",
            log_prefix="[t]",
        ) as swapped:
            assert swapped is True
            # Every service is on OLD for the duration of the block.
            assert _recreated(docker) == ["gms", "mae", "mce"]

        # ...and every one is restored, or later phases run on the wrong image.
        assert _recreated(docker) == ["gms", "mae", "mce", "gms", "mae", "mce"]
        tags = [
            c.kwargs["compose_env"]["DATAHUB_VERSION"]
            for c in docker.recreate_service.call_args_list
        ]
        assert tags == ["zdu-old"] * 3 + ["zdu-new"] * 3

    def test_skips_consumers_the_profile_does_not_run(self) -> None:
        # Embedded profile: GMS hosts the consumers, and recreating a service
        # Compose does not know about would fail the whole window.
        docker = _docker_with(["gms"])
        with old_image_window(
            docker,
            gms_service="gms",
            consumer_services=("mae", "mce"),
            old_image_tag="zdu-old",
            new_image_tag="zdu-new",
            build_images_root="nonexistent",
            log_prefix="[t]",
        ):
            pass
        assert _recreated(docker) == ["gms", "gms"]

    def test_restores_even_when_the_block_raises(self) -> None:
        docker = _docker_with(["gms", "mae"])
        with pytest.raises(RuntimeError):
            with old_image_window(
                docker,
                gms_service="gms",
                consumer_services=("mae",),
                old_image_tag="zdu-old",
                new_image_tag="zdu-new",
                build_images_root="nonexistent",
                log_prefix="[t]",
            ):
                raise RuntimeError("sweep blew up")
        assert _recreated(docker) == ["gms", "mae", "gms", "mae"]

    def test_no_swap_when_both_tags_are_identical(self) -> None:
        docker = _docker_with(["gms", "mae"])
        with old_image_window(
            docker,
            gms_service="gms",
            consumer_services=("mae",),
            old_image_tag="debug",
            new_image_tag="debug",
            build_images_root="nonexistent",
            log_prefix="[t]",
        ) as swapped:
            assert swapped is False
        docker.recreate_service.assert_not_called()
