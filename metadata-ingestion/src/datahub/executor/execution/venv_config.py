"""Venv configuration and the cache key derived from it.

Everything here is a pure function of a `VenvConfig`: which kind of venv it
asks for, what its cache entry is called, and whether that entry may be
reused at all. Nothing here touches the filesystem -- entry lifecycle lives
in venv_cache, and installing lives in runner.
"""

import dataclasses
import hashlib
import os
import pathlib
from datetime import datetime, timezone
from typing import Annotated, Any, Mapping, Optional, Union

import pydantic
from expandvars import (
    ExpandvarsException,
    UnboundVariable,
    expand as _expandvars_expand,
)
from packaging.requirements import InvalidRequirement, Requirement

from datahub.executor.common.env_config import get_venv_cache_enabled
from datahub.executor.execution.venv_cache import EntryLock
from datahub.executor.execution.venv_utils import (
    VENV_NO_DATAHUB,
    VENV_VERSION_BUNDLED,
    VENV_VERSION_LATEST,
    VENV_VERSION_NATIVE,
    VenvKind,
    classify_version,
)


def pydantic_parse_json(v: Any) -> Any:
    """Accept a JSON string where a list or dict is expected.

    These fields arrive from the execution request as JSON text.
    """
    import json

    if isinstance(v, str):
        return json.loads(v)
    return v


def _expand_pip_req(req: str) -> str:
    """Expand ${VAR:-default} templates in a pip requirement string.

    Only expands entries that contain ${, so plain pip specs and URLs with bare
    $ mid-string (e.g. ?sig=$TOKEN) are passed through unchanged.

    Uses nounset semantics — matching config_loader.py's pattern — so that
    ${VAR} with no default raises a RuntimeError rather than silently expanding
    to an empty string and producing a blank pip requirement that uv skips.
    """
    if "${" not in req:
        return req
    try:
        return _expandvars_expand(req, nounset=True)
    except UnboundVariable as e:
        raise RuntimeError(
            f"pip requirement {req!r} references unset environment variable {e}. "
            "Set the variable or add a default (e.g. ${VAR:-fallback})."
        ) from e
    except ExpandvarsException as e:
        raise RuntimeError(
            f"pip requirement {req!r} has invalid environment variable syntax: {e}"
        ) from e


def _pages_wheel_url(base_url: str) -> str:
    """Build the wheel download URL for a DataHub Pages dev build, with cache-busting timestamp."""
    now = datetime.now(tz=timezone.utc)
    return f"{base_url}/artifacts/wheels/acryl_datahub-0.0.0.dev1-py3-none-any.whl?ts={now.timestamp()}"


@dataclasses.dataclass(frozen=True)
class CacheName:
    """The cache identity of one venv: what to call it and how to treat it."""

    name: str
    cacheable: bool
    moving: bool


class VenvConfig(pydantic.BaseModel):
    version: str = VENV_VERSION_LATEST
    main_plugin: Union[str, None] = None
    extra_pip_requirements: Annotated[
        list[str], pydantic.BeforeValidator(pydantic_parse_json)
    ] = []
    extra_pip_plugins: Annotated[
        list[str], pydantic.BeforeValidator(pydantic_parse_json)
    ] = []
    extra_env_vars: Annotated[dict, pydantic.BeforeValidator(pydantic_parse_json)] = {}
    requirements_file: Union[pathlib.Path, None] = None

    def cache_name(self, expanded_pip_reqs: list[str]) -> "CacheName":
        """This config's cache identity: name, whether it may be cached, and
        whether the thing it names can move under that name."""
        return _name_dynamic_venv(self, expanded_pip_reqs)

    @property
    def kind(self) -> VenvKind:
        """What this config asks for: pinned, latest, dev build, bundled…

        One classification for the whole module. Asking `version` directly is
        what let two different dev-build tests coexist.
        """
        return classify_version(self.version)

    def set_main_plugin(self, plugin: str) -> None:
        self.main_plugin = plugin

    def resolve_pip_requirements(self) -> list[str]:
        """Expand env-var templates in extra_pip_requirements."""
        return [_expand_pip_req(r) for r in self.extra_pip_requirements]

    def get_stable_venv_name(
        self, expanded_pip_reqs: Union[list[str], None] = None
    ) -> Union[str, None]:
        if self.requirements_file is not None:
            suffix = hashlib.sha256()
            suffix.update(self.requirements_file.read_bytes())
            return f"req-{suffix.digest().hex()[:16]}"

        if self.main_plugin is None:
            return None
        if (
            self.version == VENV_VERSION_LATEST
            or self.version == VENV_VERSION_NATIVE
            or self.version == VENV_VERSION_BUNDLED
            or self.version == VENV_NO_DATAHUB
            or self.kind is VenvKind.DEV_BUILD
        ):
            return None

        # Generate a stable name for the venv.
        # Hash the expanded values so that changing DATAHUB_INTEGRATIONS_PACKAGE_SPEC
        # (or any other env-var template) forces a new venv rather than reusing a
        # cached one with the old spec. Callers may pass pre-expanded reqs (from
        # resolve_pip_requirements()) so that hash time and install time use the same
        # os.environ snapshot.
        suffix = hashlib.sha256()
        suffix.update(self.version.encode("utf-8"))
        reqs_for_hash = (
            expanded_pip_reqs
            if expanded_pip_reqs is not None
            else self.resolve_pip_requirements()
        )
        suffix.update(str(reqs_for_hash).encode("utf-8"))
        suffix.update(str(self.extra_pip_plugins).encode("utf-8"))

        return f"{self.main_plugin}-{suffix.digest().hex()[:16]}"

    def get_acryl_datahub_requirement_line(self) -> str:
        plugins = ""
        plugins_list = filter(None, [self.main_plugin, *self.extra_pip_plugins])
        if plugins_list:
            plugins = f"[{','.join(plugins_list)}]"

        if self.version == VENV_VERSION_LATEST:
            return f"acryl-datahub{plugins}"
        elif self.version == VENV_NO_DATAHUB:
            return "# acryl-datahub is explicitly not requested."
        elif self.kind is VenvKind.DEV_BUILD:
            url = (
                self.version
                if self.version.endswith(".whl")
                else _pages_wheel_url(self.version)
            )
            return f"acryl-datahub{plugins} @ {url}"
        else:
            return f"acryl-datahub{plugins}=={self.version}"


@dataclasses.dataclass
class VenvReference:
    venv_loc: pathlib.Path
    venv_config: VenvConfig
    # Held for the task's life when this venv came from the cache, so eviction
    # cannot delete it mid-run. None for ephemeral venvs and whenever the cache
    # is off or unusable. finalize_task_output releases it.
    lock: Optional["EntryLock"] = None

    def command(self, cmd: str) -> str:
        return str(self.venv_loc / "bin" / cmd)

    def extra_envs(self) -> dict[str, str]:
        return {
            **self.venv_config.extra_env_vars,
            # TODO: Do we need to add this?
            # "VIRTUAL_ENV": str(self.venv_loc),
        }


def _node_local_stable_name(
    venv_config: VenvConfig, expanded_pip_reqs: list[str]
) -> Optional[str]:
    """A cache name for the versions get_stable_venv_name() refuses.

    `latest` moves, so its entry expires after
    DATAHUB_VENV_CACHE_LATEST_TTL_HOURS. Without that bound a long-lived pod
    would serve one resolution of `latest` for as long as it ran.

    A dev-build wheel URL is the opposite case: the pipeline publishes a
    per-deployment address naming exactly one immutable build, so it is a
    content address and a new commit is a new key. A hand-written branch
    alias would move instead, which _is_fresh_hit covers by treating every
    URL version as moving.

    Only the VENV is reused. The PACKAGE cache stays bypassed for dev builds
    (UV_NO_CACHE=1 in setup_venv) and must: every dev wheel ships the same
    name and version, so a cache keyed on those would hand one commit's build
    to another.
    """
    version = venv_config.version
    if venv_config.main_plugin is None:
        return None
    is_dev_build = classify_version(version) is VenvKind.DEV_BUILD
    if version != VENV_VERSION_LATEST and not is_dev_build:
        return None
    suffix = hashlib.sha256()
    # The version STRING, not the URL the install resolves to: _pages_wheel_url
    # appends a cache-busting timestamp, so hashing the resolved URL would make
    # every run a miss and quietly restore the behaviour this removes.
    suffix.update(version.encode("utf-8"))
    # The caller's already-expanded list, never a fresh
    # resolve_pip_requirements(): the hash and the install must see one
    # os.environ snapshot, or a template that changed between the two reads
    # names the entry after requirements nobody installs.
    suffix.update(str(expanded_pip_reqs).encode("utf-8"))
    suffix.update(str(venv_config.extra_pip_plugins).encode("utf-8"))
    tag = "dev" if is_dev_build else VENV_VERSION_LATEST
    return f"{venv_config.main_plugin}-{tag}-{suffix.digest().hex()[:16]}"


def _extra_env_vars_cache_suffix(extra_env_vars: Mapping[str, object]) -> str:
    """Short digest distinguishing cache entries that differ only in extra_env_vars.

    extra_env_vars is per-recipe (package index URLs, private-index
    credentials) and IS merged into the environment the venv is built under,
    but get_stable_venv_name() does not hash it. Without this suffix two
    recipes differing only there share one entry, and one of them silently
    gets a venv built against the other's index.

    16 hex characters, not 8: a collision reintroduces exactly that bug, and
    32 bits collides at a few tens of thousands of distinct environments.

    Fields are LENGTH-PREFIXED, not delimited. Under `key=value\\n` framing a
    value containing a newline impersonates an extra pair, and these values
    routinely hold multi-line content -- service-account JSON, PEM keys -- so
    such a collision needs no malice. Hashing rather than using the value
    keeps a credential out of the directory name.
    """
    digest = hashlib.sha256()
    for key, value in sorted(extra_env_vars.items()):
        for field in (key.encode("utf-8"), str(value).encode("utf-8")):
            digest.update(f"{len(field)}:".encode("ascii"))
            digest.update(field)
    return digest.hexdigest()[:16]


def _name_dynamic_venv(
    venv_config: VenvConfig, expanded_pip_reqs: list[str]
) -> CacheName:
    """Pick the venv's name and whether it is cacheable.

    A pinned version always gets a stable name. Two more join it while the
    cache is on:

      - `latest`, a moving target, whose staleness is bounded by
        DATAHUB_VENV_CACHE_LATEST_TTL_HOURS rather than by how long the pod
        happens to live. It is also the default for every recipe, so excluding it
        would leave the cache almost never hit -- and sharing one entry makes
        a probe and the ingestion run it predicts install the same version,
        which resolving twice does not.
      - A dev-build wheel URL, which unlike `latest` names one immutable
        build. Excluded until now over storage, which evict_stale_entries now
        bounds by both entry count and age. It is the only
        version a probe can run before the `recipe probe` command ships, so
        leaving it uncacheable made every probe re-download its wheel --
        about 4.4s of a 7-9s probe, every time.

    Everything else still gets a random, per-run name.

    The kill switch is consulted BEFORE _node_local_stable_name, not after:
    with the cache disabled, both must fall back to today's ephemeral random
    name exactly, not keep a stable cache-shaped name that just happens to
    live under tmp_dir. A pinned version keeps its stable name either way --
    that's today's behaviour too.
    """
    cache_enabled = get_venv_cache_enabled()
    stable_name = venv_config.get_stable_venv_name(expanded_pip_reqs=expanded_pip_reqs)
    if stable_name is None and cache_enabled:
        stable_name = _node_local_stable_name(venv_config, expanded_pip_reqs)
    cacheable = stable_name is not None and cache_enabled
    if cacheable and venv_config.extra_env_vars:
        # An empty dict -- the overwhelmingly common case -- must leave the
        # name byte-identical to today, so this only applies when there is
        # something to distinguish.
        assert stable_name is not None
        suffix = _extra_env_vars_cache_suffix(venv_config.extra_env_vars)
        stable_name = f"{stable_name}-{suffix}"
    venv_name = stable_name or f"eph-{hashlib.sha256(os.urandom(32)).hexdigest()[:16]}"
    return CacheName(
        name=venv_name,
        cacheable=cacheable,
        moving=_resolves_to_moving_target(venv_config, expanded_pip_reqs),
    )


def _is_pinned_requirement(req: str) -> bool:
    """Whether this requirement names one immutable artifact.

    Only an exact-version pin does. A direct URL does NOT: the artifact
    behind an address can be republished, which is the same reason
    _node_local_stable_name treats a dev-build wheel URL as a moving target.

    `==` alone is not enough to call it exact. PEP 440 prefix matching uses
    the same operator, so `pkg==1.2.*` resolves to 1.2.3 today and 1.2.9
    tomorrow while the cache key -- built from the requirement STRING --
    never changes. `===` has no prefix form: it is arbitrary equality, a
    literal string comparison, so it really is immutable.

    An unparseable requirement counts as unpinned. Being wrong that way costs
    a periodic rebuild; being wrong the other way freezes the entry for the
    pod's life.
    """
    try:
        parsed = Requirement(req)
    except InvalidRequirement:
        return False
    if parsed.url:
        return False
    return any(
        spec.operator == "==="
        or (spec.operator == "==" and not spec.version.endswith(".*"))
        for spec in parsed.specifier
    )


def _requirements_file_is_pinned(path: pathlib.Path) -> bool:
    """Whether every line of a requirements file names an immutable artifact.

    Unreadable counts as unpinned, for the same reason an unparseable
    requirement does.
    """
    try:
        lines = path.read_text().splitlines()
    except OSError:
        return False
    for raw in lines:
        line = raw.split("#", 1)[0].strip()
        if not line or line.startswith("-"):
            # Blank, comment, or a pip flag (-r, --index-url). A nested -r
            # is not followed, so treat any flag line as unpinnable.
            if line.startswith("-"):
                return False
            continue
        if not _is_pinned_requirement(line):
            return False
    return True


def _resolves_to_moving_target(
    venv_config: VenvConfig, expanded_pip_reqs: list[str]
) -> bool:
    """Whether this venv's contents can differ tomorrow under the same key.

    The cache key is built from requirement STRINGS, not from what they
    resolve to, so "immutable" has to be judged on the strings.

    `version` alone is not enough, and that was the gap: a pinned CLI
    version with `extra_pip_requirements: ["some-lib"]` produced a stable
    name and no TTL, so the pod served day-one's resolution of that
    dependency for its whole life -- and a daily schedule keeps the
    last-used marker fresh, so the age-based eviction never fired either.
    Before this cache existed, a stable-named venv still lived under the
    per-execution directory and was rebuilt every run, so nothing was frozen.

    The common case -- no extra requirements -- is unchanged: the answer
    still comes down to `version`.
    """
    version = venv_config.version
    if classify_version(version) in (VenvKind.LATEST, VenvKind.DEV_BUILD):
        return True
    if any(not _is_pinned_requirement(req) for req in expanded_pip_reqs):
        return True
    if venv_config.requirements_file is not None:
        return not _requirements_file_is_pinned(venv_config.requirements_file)
    return False
