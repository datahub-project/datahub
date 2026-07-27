"""Unit tests for BuildImagesPhase."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest

from tests.zdu.framework.config import ZDUTestConfig
from tests.zdu.framework.context import TestContext
from tests.zdu.framework.phases.build_images import BuildImagesPhase


@pytest.fixture
def phase(tmp_path: Path) -> BuildImagesPhase:
    return BuildImagesPhase(
        repo_root=tmp_path,
        old_ref="master",
        services_to_build=(
            ":metadata-service:war:docker",
            ":datahub-upgrade:docker",
        ),
        gradle_cmd=["./gradlew"],
    )


class TestSkipsWhenDisabled:
    def test_skips_when_build_images_false(self, phase: BuildImagesPhase) -> None:
        cfg = ZDUTestConfig(build_images=False)
        ctx = TestContext()
        result = phase.run(ctx, cfg)
        assert result.status == "skipped"
        assert ctx.image_build is None


class TestShaResolution:
    def test_resolves_short_sha_from_git_ref(self, phase: BuildImagesPhase) -> None:
        # _ensure_worktree returns the short SHA at the worktree's HEAD.
        with patch.object(phase, "_ensure_worktree") as mock_ensure:
            mock_ensure.side_effect = ["abc12345", "def09876"]
            with (
                patch.object(
                    phase,
                    "_run_git",
                    return_value="def0987654321def0987654321def09876543",
                ),
                patch.object(phase, "_first_missing_image", return_value=None),
            ):
                cfg = ZDUTestConfig(build_images=True)
                ctx = TestContext()
                result = phase.run(ctx, cfg)
        assert result.status == "passed"
        assert ctx.image_build is not None
        assert ctx.image_build.old_sha == "abc12345"
        assert ctx.image_build.new_sha == "def09876"


class TestCacheHit:
    def test_cache_hit_skips_build_and_returns_passed(
        self, phase: BuildImagesPhase
    ) -> None:
        # All images already exist locally with the computed tags.
        cfg = ZDUTestConfig(build_images=True)
        ctx = TestContext()
        with (
            patch.object(
                phase, "_ensure_worktree", side_effect=["abc12345", "def09876"]
            ),
            patch.object(
                phase,
                "_run_git",
                return_value="def0987654321def0987654321def09876543",
            ),
            patch.object(phase, "_first_missing_image", return_value=None),
            patch.object(phase, "_build_old") as mock_old,
            patch.object(phase, "_build_new") as mock_new,
        ):
            result = phase.run(ctx, cfg)
        assert result.status == "passed"
        assert ctx.image_build is not None
        assert ctx.image_build.cache_hit is True
        assert ctx.image_build.old_image_tag == "zdu-old-abc12345"
        assert ctx.image_build.new_image_tag == "zdu-new-def09876"
        mock_old.assert_not_called()
        mock_new.assert_not_called()


class TestCacheMissBuildsBothSides:
    def test_cache_miss_builds_both_sides(self, phase: BuildImagesPhase) -> None:
        cfg = ZDUTestConfig(build_images=True)
        ctx = TestContext()
        with (
            patch.object(
                phase, "_ensure_worktree", side_effect=["abc12345", "def09876"]
            ),
            patch.object(
                phase,
                "_run_git",
                return_value="def0987654321def0987654321def09876543",
            ),
            patch.object(
                phase,
                "_first_missing_image",
                return_value="acryldata/datahub-gms:zdu-old-abc12345",
            ),
            patch.object(phase, "_build_old") as mock_old,
            patch.object(phase, "_build_new") as mock_new,
        ):
            result = phase.run(ctx, cfg)
        assert result.status == "passed"
        assert ctx.image_build is not None
        assert ctx.image_build.cache_hit is False
        # Signatures are now (image_tag,) — no SHA argument.
        mock_old.assert_called_once_with("zdu-old-abc12345")
        mock_new.assert_called_once_with("zdu-new-def09876")


class TestCacheMissLogIsDiagnostic:
    """G14 — Cache MISS log must name the missing image.

    Without this, every miss prints identically and you can't tell whether
    OLD vs NEW or which of the 4 service images (GMS, upgrade, MAE, MCE) got
    evicted — the running stack keeps GMS warm but the other three get pruned
    independently, so naming the missing ref makes future misses self-explain.
    """

    def test_cache_miss_log_names_missing_image(
        self, phase: BuildImagesPhase, caplog
    ) -> None:
        cfg = ZDUTestConfig(build_images=True)
        ctx = TestContext()
        missing_ref = "acryldata/datahub-upgrade:zdu-old-abc12345"
        with (
            patch.object(
                phase, "_ensure_worktree", side_effect=["abc12345", "def09876"]
            ),
            patch.object(
                phase,
                "_run_git",
                return_value="def0987654321def0987654321def09876543",
            ),
            patch.object(phase, "_first_missing_image", return_value=missing_ref),
            patch.object(phase, "_build_old"),
            patch.object(phase, "_build_new"),
        ):
            with caplog.at_level(
                "INFO", logger="tests.zdu.framework.phases.build_images"
            ):
                phase.run(ctx, cfg)
        # Find the cache-MISS log line and assert it names the missing ref.
        miss_lines = [r.message for r in caplog.records if "cache MISS" in r.message]
        assert len(miss_lines) == 1, (
            f"expected exactly one cache MISS line, got {miss_lines!r}"
        )
        assert missing_ref in miss_lines[0], (
            f"cache MISS log must name the missing image; got {miss_lines[0]!r}"
        )

    def test_first_missing_image_returns_first_missing_ref(
        self, phase: BuildImagesPhase
    ) -> None:
        # All exist → None.
        with patch.object(phase, "_image_exists", return_value=True):
            assert (
                phase._first_missing_image("zdu-old-abc12345", "zdu-new-def09876")
                is None
            )

        # First service's OLD ref missing → that ref returned.
        first_repo = next(iter(phase._services_to_build))
        from tests.zdu.framework.phases.build_images import _SERVICE_TO_DOCKER_REPO

        first_repo_name = _SERVICE_TO_DOCKER_REPO[first_repo]
        expected = f"{first_repo_name}:zdu-old-abc12345"

        def fake_exists(ref: str) -> bool:
            return ref != expected

        with patch.object(phase, "_image_exists", side_effect=fake_exists):
            assert (
                phase._first_missing_image("zdu-old-abc12345", "zdu-new-def09876")
                == expected
            )


class TestMutatesConfigTags:
    def test_run_sets_config_image_tags(self, phase: BuildImagesPhase) -> None:
        cfg = ZDUTestConfig(build_images=True)
        ctx = TestContext()
        with (
            patch.object(
                phase, "_ensure_worktree", side_effect=["abc12345", "def09876"]
            ),
            patch.object(
                phase,
                "_run_git",
                return_value="def0987654321def0987654321def09876543",
            ),
            patch.object(phase, "_first_missing_image", return_value=None),
        ):
            phase.run(ctx, cfg)
        assert cfg.old_image_tag == "zdu-old-abc12345"
        assert cfg.new_image_tag == "zdu-new-def09876"


class TestWorktreeLifecycle:
    def test_build_old_uses_persistent_worktree(
        self, phase: BuildImagesPhase, tmp_path: Path
    ) -> None:
        with (
            patch.object(phase, "_worktree_paths") as mock_paths,
            patch.object(phase, "_run_gradle") as mock_gradle,
            patch.object(phase, "_verify_image_tagged"),
        ):
            old_wt = tmp_path / "zdu-images" / "old"
            new_wt = tmp_path / "zdu-images" / "new"
            mock_paths.return_value = (old_wt, new_wt)
            mock_gradle.return_value = MagicMock(returncode=0)
            phase._build_old("zdu-old-abc12345")

        # Gradle invoked once for each service in services_to_build (2).
        assert mock_gradle.call_count == 2
        # Verify gradle ran from the OLD worktree path.
        for gradle_call in mock_gradle.call_args_list:
            assert (
                gradle_call
                == call(
                    [gradle_call.args[0][0], gradle_call.args[0][1]],
                    cwd=old_wt,
                )
                or gradle_call.kwargs.get("cwd") == old_wt
                or gradle_call.args[1] == old_wt
            )

    def test_build_old_does_not_remove_worktree(
        self, phase: BuildImagesPhase, tmp_path: Path
    ) -> None:
        with (
            patch.object(phase, "_worktree_paths") as mock_paths,
            patch.object(phase, "_run_git") as mock_git,
            patch.object(phase, "_run_gradle") as mock_gradle,
            patch.object(phase, "_verify_image_tagged"),
        ):
            old_wt = tmp_path / "zdu-images" / "old"
            new_wt = tmp_path / "zdu-images" / "new"
            mock_paths.return_value = (old_wt, new_wt)
            mock_gradle.return_value = MagicMock(returncode=0)
            phase._build_old("zdu-old-abc12345")

        # Persistent worktrees are NEVER removed.
        git_calls = [c.args[0] for c in mock_git.call_args_list]
        assert not any("worktree" in args and "remove" in args for args in git_calls), (
            "worktree remove must not be called — worktrees are persistent"
        )


class TestRaisesOnGradleFailure:
    def test_gradle_nonzero_returncode_raises(self, phase: BuildImagesPhase) -> None:
        with (
            patch.object(phase, "_worktree_paths") as mock_paths,
            patch.object(phase, "_run_gradle") as mock_gradle,
        ):
            mock_paths.return_value = (
                Path("/tmp/fake/old"),
                Path("/tmp/fake/new"),
            )
            mock_gradle.return_value = MagicMock(returncode=1, stderr="boom")
            with pytest.raises(RuntimeError, match="gradle"):
                phase._build_old("zdu-old-abc12345")


class TestEnsureWorktree:
    def test_existing_worktree_synced_to_ref(
        self, phase: BuildImagesPhase, tmp_path: Path
    ) -> None:
        """When the worktree dir + .git already exist, fetch+reset instead of worktree add."""
        worktree_path = tmp_path / "old"
        worktree_path.mkdir(parents=True)
        (worktree_path / ".git").write_text("gitdir: ../../.git/worktrees/old")

        git_calls: list[list[str]] = []

        def fake_run_git(
            args: list[str], cwd: Path | None = None, check: bool = True
        ) -> str:
            git_calls.append(args)
            if args[0] == "rev-parse":
                return "abc1234567890abc1234567890abc12345678"
            return ""

        with patch.object(phase, "_run_git", side_effect=fake_run_git):
            short_sha = phase._ensure_worktree(worktree_path, "master")

        # Must call fetch and reset --hard, NOT worktree add.
        cmd_names = [tuple(c[:2]) for c in git_calls]
        assert ("fetch", "origin") in cmd_names, "expected 'git fetch origin'"
        assert ("reset", "--hard") in cmd_names, "expected 'git reset --hard'"
        assert not any(c[0] == "worktree" and c[1] == "add" for c in git_calls), (
            "worktree add must NOT be called for existing worktree"
        )
        assert short_sha == "abc12345"

    def test_dirty_worktree_refuses_to_clobber(
        self, phase: BuildImagesPhase, tmp_path: Path
    ) -> None:
        """Existing worktree with uncommitted changes → raise instead of
        silently `git reset --hard`ing them away."""
        worktree_path = tmp_path / "old"
        worktree_path.mkdir(parents=True)
        (worktree_path / ".git").write_text("gitdir: ../../.git/worktrees/old")

        def fake_run_git(
            args: list[str], cwd: Path | None = None, check: bool = True
        ) -> str:
            if args == ["status", "--porcelain"]:
                return " M smoke-test/tests/zdu/foo.py\n?? unstaged.txt\n"
            return ""

        with patch.object(phase, "_run_git", side_effect=fake_run_git):
            with pytest.raises(RuntimeError, match="uncommitted changes"):
                phase._ensure_worktree(worktree_path, "master")

    def test_clean_worktree_proceeds_to_reset(
        self, phase: BuildImagesPhase, tmp_path: Path
    ) -> None:
        """Clean worktree (empty `git status --porcelain`) → fetch + reset normally."""
        worktree_path = tmp_path / "old"
        worktree_path.mkdir(parents=True)
        (worktree_path / ".git").write_text("gitdir: ../../.git/worktrees/old")

        git_calls: list[list[str]] = []

        def fake_run_git(
            args: list[str], cwd: Path | None = None, check: bool = True
        ) -> str:
            git_calls.append(args)
            if args == ["status", "--porcelain"]:
                return ""  # clean
            if args[0] == "rev-parse":
                return "abc1234567890abc1234567890abc12345678"
            return ""

        with patch.object(phase, "_run_git", side_effect=fake_run_git):
            short_sha = phase._ensure_worktree(worktree_path, "master")

        # status check, then fetch + reset.
        cmd_names = [tuple(c[:2]) for c in git_calls]
        assert ("status", "--porcelain") in cmd_names
        assert ("fetch", "origin") in cmd_names
        assert ("reset", "--hard") in cmd_names
        assert short_sha == "abc12345"

    def test_new_worktree_uses_resolved_sha_not_head_string(
        self, phase: BuildImagesPhase, tmp_path: Path
    ) -> None:
        """HEAD is resolved to a full SHA before _ensure_worktree is called,
        so the NEW worktree is pinned to a specific commit, not the moving
        'HEAD' pointer."""
        cfg = ZDUTestConfig(build_images=True)
        ctx = TestContext()

        full_sha = "def0987654321def0987654321def09876543"
        ensure_calls: list[tuple[Path, str]] = []

        def fake_ensure(path: Path, ref: str) -> str:
            ensure_calls.append((path, ref))
            return ref[:8]

        with (
            patch.object(phase, "_run_git", return_value=full_sha),
            patch.object(phase, "_ensure_worktree", side_effect=fake_ensure),
            patch.object(phase, "_first_missing_image", return_value=None),
        ):
            phase.run(ctx, cfg)

        # The NEW worktree must receive the full resolved SHA, not "HEAD".
        assert len(ensure_calls) == 2
        _old_path, old_ref = ensure_calls[0]
        _new_path, new_ref = ensure_calls[1]
        assert old_ref == "master"
        assert new_ref == full_sha, (
            f"NEW worktree must be pinned to resolved SHA {full_sha!r}, got {new_ref!r}"
        )


class TestGradleTagPropertyName:
    """Regression: docker tag override is -Ptag, not -PdockerVersion.

    The DataHub Docker tasks use ${versionTag} from versioning-global.gradle,
    which only honors -Ptag=... . Passing -PdockerVersion silently no-ops and
    the produced images get the default versionTag (e.g. v1.5.0rc1-SNAPSHOT)
    instead of the zdu-old-{sha}/zdu-new-{sha} we asked for.
    """

    def test_build_old_passes_ptag_flag(
        self, phase: BuildImagesPhase, tmp_path: Path
    ) -> None:
        with (
            patch.object(phase, "_worktree_paths") as mock_paths,
            patch.object(phase, "_run_gradle") as mock_gradle,
            patch.object(phase, "_verify_image_tagged"),
        ):
            mock_paths.return_value = (tmp_path / "old", tmp_path / "new")
            mock_gradle.return_value = MagicMock(returncode=0)
            phase._build_old("zdu-old-abc12345")

        # Every gradle call must include exactly -Ptag=<tag>, never -PdockerVersion.
        for gradle_call in mock_gradle.call_args_list:
            args = gradle_call.args[0]
            assert "-Ptag=zdu-old-abc12345" in args, (
                f"gradle call must use -Ptag, got: {args}"
            )
            assert not any(a.startswith("-PdockerVersion=") for a in args), (
                f"gradle call must NOT use -PdockerVersion (silently ignored "
                f"by the docker task — produces wrong image tag): {args}"
            )

    def test_build_new_passes_ptag_flag(
        self, phase: BuildImagesPhase, tmp_path: Path
    ) -> None:
        with (
            patch.object(phase, "_worktree_paths") as mock_paths,
            patch.object(phase, "_run_gradle") as mock_gradle,
            patch.object(phase, "_verify_image_tagged"),
        ):
            mock_paths.return_value = (tmp_path / "old", tmp_path / "new")
            mock_gradle.return_value = MagicMock(returncode=0)
            phase._build_new("zdu-new-def09876")

        for gradle_call in mock_gradle.call_args_list:
            args = gradle_call.args[0]
            assert "-Ptag=zdu-new-def09876" in args, (
                f"gradle call must use -Ptag, got: {args}"
            )
            assert not any(a.startswith("-PdockerVersion=") for a in args), args


class TestPostBuildImageVerification:
    """Trust-but-verify: gradle rc==0 is not enough — confirm the image
    is actually tagged in the daemon. Catches the silent -PdockerVersion
    failure mode where gradle succeeds but tags as the default versionTag.
    """

    def test_raises_when_image_not_in_daemon_after_build(
        self, phase: BuildImagesPhase, tmp_path: Path
    ) -> None:
        with (
            patch.object(phase, "_worktree_paths") as mock_paths,
            patch.object(phase, "_run_gradle") as mock_gradle,
            patch.object(phase, "_image_exists", return_value=False),
        ):
            mock_paths.return_value = (tmp_path / "old", tmp_path / "new")
            mock_gradle.return_value = MagicMock(returncode=0)
            with pytest.raises(RuntimeError, match="not in the local daemon"):
                phase._build_old("zdu-old-abc12345")

    def test_passes_when_image_present_after_build(
        self, phase: BuildImagesPhase, tmp_path: Path
    ) -> None:
        with (
            patch.object(phase, "_worktree_paths") as mock_paths,
            patch.object(phase, "_run_gradle") as mock_gradle,
            patch.object(phase, "_image_exists", return_value=True),
        ):
            mock_paths.return_value = (tmp_path / "old", tmp_path / "new")
            mock_gradle.return_value = MagicMock(returncode=0)
            phase._build_old("zdu-old-abc12345")  # no raise
