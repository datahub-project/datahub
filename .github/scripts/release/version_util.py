"""Parse, classify, and sort DataHub Cloud release tags.

Tag forms (version is always ``major.minor.hotfix``):

  * RC       — ``v{major}.{minor}.{hotfix}rc{n}`` plus ``VERSION_SUFFIX``
  * Official — ``v{major}.{minor}.{hotfix}`` plus ``VERSION_SUFFIX``
  * Custom   — any other tag (including extra version components)
"""

import json
import logging
import re
import subprocess
import sys
from typing import Iterable, NamedTuple, Optional

from release_variables import SLACK_RELEASE_CHANNEL_PREFIX, VERSION_SUFFIX

logger = logging.getLogger(__name__)

_SUFFIX = re.escape(VERSION_SUFFIX)
RC_PATTERN = re.compile(rf"^v(\d+)\.(\d+)\.(\d+)rc(\d+){_SUFFIX}$")
OFFICIAL_PATTERN = re.compile(rf"^v(\d+)\.(\d+)\.(\d+){_SUFFIX}$")

# Bare version, no 'v' prefix or suffix: 1.0.0
VERSION_STR_PATTERN = re.compile(r"\d+\.\d+\.\d+")


class Version(NamedTuple):
    """A ``major.minor.hotfix`` version; sorts in natural tuple order."""

    major: int
    minor: int
    hotfix: int


class RcVersion(NamedTuple):
    """An RC version: its base :class:`Version` plus the RC number."""

    version: Version
    rc: int


def is_official(tag: str) -> bool:
    """True for final release tags matching the official pattern."""
    return OFFICIAL_PATTERN.match(tag) is not None


def is_rc(tag: str) -> bool:
    """True for release-candidate tags matching the RC pattern."""
    return RC_PATTERN.match(tag) is not None


def is_custom(tag: str) -> bool:
    """True for tags that are neither RC nor official (e.g. ``v0.3.18.1-cx42-06``)."""
    return not is_official(tag) and not is_rc(tag)


def parse_official(tag: str) -> Optional[Version]:
    """Parse an official tag into a :class:`Version`, or ``None`` if it isn't one."""
    m = OFFICIAL_PATTERN.match(tag)
    if not m:
        return None
    return Version(int(m.group(1)), int(m.group(2)), int(m.group(3)))


def parse_version_str(version: str) -> Optional[Version]:
    """Parse a bare ``major.minor.hotfix`` string (e.g. "1.0.0"), or ``None``."""
    if not VERSION_STR_PATTERN.fullmatch(version):
        logger.warning("Version string %r is not in MAJOR.MINOR.HOTFIX form.", version)
        return None
    major, minor, hotfix = (int(part) for part in version.split("."))
    return Version(major, minor, hotfix)


def release_channel(version_or_tag: str) -> Optional[str]:
    """Return the Slack release channel for a version line, or ``None`` if unparseable.

    Accepts a bare version, an official tag, or an RC tag. All patches/RCs of a
    line map to the same channel.
    """
    rc = parse_rc(version_or_tag)
    version = parse_official(version_or_tag) or (rc.version if rc is not None else None)
    if version is None:
        bare = version_or_tag.removeprefix("v")
        if VERSION_STR_PATTERN.fullmatch(bare):
            version = parse_version_str(bare)
    if version is None:
        return None
    return f"{SLACK_RELEASE_CHANNEL_PREFIX}{version.major}_{version.minor}_0"


def release_channel_from_ref(ref: str) -> Optional[str]:
    """Return the Slack release channel for a git ref, or ``None`` if it isn't one.

    Like :func:`release_channel`, but first strips a leading ``releases/`` or
    ``hotfixes/`` branch prefix so release/hotfix branches map to their line's
    channel. Non-release refs return ``None``.
    """
    ref = ref.removeprefix("releases/").removeprefix("hotfixes/")
    return release_channel(ref)


def parse_rc(tag: str) -> Optional[RcVersion]:
    """Parse an RC tag into an :class:`RcVersion`, or ``None`` if it isn't one."""
    m = RC_PATTERN.match(tag)
    if not m:
        return None
    version = Version(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    return RcVersion(version, int(m.group(4)))


def official_tags_sorted(tags: Iterable[str]) -> list[str]:
    """Return only the official tags, sorted ascending by version.

    Custom and RC tags are skipped.
    """
    official = [t for t in tags if is_official(t)]
    return sorted(official, key=lambda t: parse_official(t))  # type: ignore[arg-type, return-value]


def last_official_before(tags: Iterable[str], before: Version) -> Optional[str]:
    """Highest official tag whose version sorts strictly before ``before``.

    Custom and RC tags are skipped. Returns ``None`` when no official tag
    precedes ``before``.
    """
    candidates = [
        t for t in tags if is_official(t) and parse_official(t) < before  # type: ignore[operator]
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda t: parse_official(t))  # type: ignore[arg-type, return-value]


def fetch_release_tags(repo: str) -> list[str]:
    """Return all release tag names for *repo*, falling back to ``gh release list``."""
    try:
        result = subprocess.run(
            [
                "gh",
                "api",
                f"repos/{repo}/releases",
                "--paginate",
                "--jq",
                ".[].tag_name",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        return [line for line in result.stdout.splitlines() if line]
    except subprocess.CalledProcessError as exc:
        print(
            f"Warning: 'gh api' failed ({exc}), falling back to gh release list",
            file=sys.stderr,
        )

    result = subprocess.run(
        [
            "gh",
            "release",
            "list",
            "--repo",
            repo,
            "--json",
            "tagName",
            "--limit",
            "1000",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    releases = json.loads(result.stdout)
    return [r["tagName"] for r in releases]


def git_tag_exists(repo: str, tag: str) -> bool:
    """Return True if a git tag ref exists (even without an associated release)."""
    result = subprocess.run(
        ["gh", "api", f"repos/{repo}/git/ref/tags/{tag}"],
        capture_output=True,
    )
    return result.returncode == 0
