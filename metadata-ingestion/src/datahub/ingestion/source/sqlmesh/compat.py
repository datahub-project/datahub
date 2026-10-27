import contextlib
import logging
import os
import threading
from typing import Any, Callable, Dict, Iterator, Optional

logger = logging.getLogger(__name__)

try:
    from sqlmesh import Context as SqlmeshContext
except ImportError:
    # sqlmesh is an optional dependency; callers must check `SqlmeshContext is None` before use
    SqlmeshContext = None  # type: ignore[assignment,misc]

# Type aliases for annotations that reference sqlmesh's own classes. sqlmesh is
# an optional extra, so these fall back to `Any` when it isn't installed. The
# connector controls its own deps, so we resolve them with a guarded runtime
# import (the repo convention) rather than a `TYPE_CHECKING` block — the
# annotations are only ever consumed by static type checkers and IDEs.
try:
    from sqlmesh.core.model import Model as SqlmeshModel
    from sqlmesh.core.snapshot import Snapshot
except ImportError:
    SqlmeshModel = Any  # type: ignore[assignment,misc]
    Snapshot = Any  # type: ignore[assignment,misc]

# The Context type used purely for annotations. Uses the same guarded-import
# idiom as SqlmeshModel/Snapshot above (rather than a ternary) so mypy treats it
# as a type alias — a `X if cond else Any` expression is a runtime value and is
# rejected as "not valid as a type" wherever it annotates a parameter.
try:
    from sqlmesh import Context as SqlmeshContextType
except ImportError:
    SqlmeshContextType = Any  # type: ignore[assignment,misc]

# SQLMesh uses ProcessPoolExecutor internally to parse SQL models. Serialise
# context initialisation to avoid racing over worker process spawning, and reuse
# the same lock to serialise the process-pool patch install below so a second
# source can't build a Context while the patch is half-installed. The lock is
# held only for the patch install and SqlmeshContext.__init__ (~sub-second).
_sqlmesh_context_load_lock = threading.Lock()

# Idempotency sentinel for _install_process_pool_patch(): the patch is a global
# module-state mutation, so it must run at most once per process. Only published
# under _sqlmesh_context_load_lock after the module assignments succeed.
_process_pool_patched = False


def _install_process_pool_patch() -> None:
    """Replace SQLMesh's process-pool factory with a synchronous in-process one.

    SQLMesh's ``ProcessPoolExecutor(mp_context=fork)`` deadlocks when the DataHub
    async sink thread pool is already running — the child process inherits locks
    held by other threads (allocator arena, stdio buffer, libcurl connection
    cache) but no thread alive in the child to release them. Repro is reliable on
    macOS (libdispatch + malloc_zone hold non-atfork locks); on Linux glibc's
    pthread_atfork handlers reset most of these so the same scenario "usually"
    works. We patch unconditionally because the remaining locks (logging, numpy
    C-ext init, requests session pool) can still strand a fork on Linux under
    contention, and the parallel-parse speedup is small in practice.

    Deferred to connector init (rather than run at import time) so importing this
    module has no global side effects. It only has to be in place before the
    first ``SqlmeshContext`` is constructed: ``sqlmesh.core.loader`` /
    ``.model.cache`` call ``create_process_pool_executor`` by name *at call time*,
    so replacing the module attribute now — even after those modules were
    imported — redirects their call sites. Idempotent via ``_process_pool_patched``.

    These are private SQLMesh internals, so a version bump can rename them.
    sqlmesh is installed whenever this runs (the connector guards on
    ``SqlmeshContext is None`` first), so a failure here means an API rename
    rather than a missing package — log loudly and carry on without the patch.

    The whole check/install/publish runs under ``_sqlmesh_context_load_lock`` —
    the same lock that guards ``SqlmeshContext.__init__`` — so a concurrent source
    can neither run a second install nor construct a Context while the patch is
    only half-applied. The sentinel is published only after both module
    assignments succeed, so a partially-installed state is never observable.
    """
    global _process_pool_patched
    if SqlmeshContext is None:
        return
    with _sqlmesh_context_load_lock:
        if _process_pool_patched:
            return
        try:
            import sqlmesh.core.loader as _loader_mod
            import sqlmesh.core.model.cache as _cache_mod
            from sqlmesh.utils.process import SynchronousPoolExecutor

            def _sync_pool(
                initializer: Optional[Callable[..., object]] = None,
                initargs: tuple = (),
                **_ignored: object,
            ) -> SynchronousPoolExecutor:
                # create_process_pool_executor's real contract is (initializer,
                # initargs); a sqlmesh version may also pass max_workers /
                # mp_context, which we deliberately drop — running synchronously
                # in-process is the whole point. Naming the two we use keeps that
                # contract visible instead of hiding it behind **kwargs.
                return SynchronousPoolExecutor(
                    initializer=initializer,  # type: ignore[arg-type]
                    initargs=initargs,
                )

            # Patch every module that captured create_process_pool_executor by
            # name at import time. Hitting the factory in sqlmesh.utils.process is
            # not enough — call sites that did `from ... import
            # create_process_pool_executor` have their own binding.
            _loader_mod.create_process_pool_executor = _sync_pool  # type: ignore[attr-defined]
            _cache_mod.create_process_pool_executor = _sync_pool  # type: ignore[attr-defined]
            _process_pool_patched = True
        except ImportError:
            logger.warning(
                "Could not patch SQLMesh's process-pool factory (private API "
                "moved in this sqlmesh version). Model parsing will fork worker "
                "processes, which can hang when the DataHub async sink is active.",
                exc_info=True,
            )


# Exact substring of the ConfigError raised by RemoteCloudSchedulerConfig when
# no Tobiko Cloud token is available. We match on this so the shim never
# swallows any other kind of scheduler failure.
_TOBIKO_CLOUD_NO_CREDS_ERR_FRAGMENT = (
    "Cloud scheduler requires a cloud state connection"
)

# Sentinels for the EnterpriseConfig compat patches below.
_TOBIKO_SNOWFLAKE_APP_PATCH_SENTINEL = "_datahub_snowflake_app_literal_relaxed"
_TOBIKO_CONVERT_PATCH_SENTINEL = "_datahub_convert_config_type_isinstance_patched"


def _install_enterprise_config_compat_patches() -> None:
    """When tobikodata is installed, the project's ``config.py`` may return an
    ``EnterpriseConfig`` carrying a Snowflake connection with
    ``application="Tobiko_TobikoCloud"``. Loading that through plain
    ``sqlmesh.Context`` trips two distinct failures seen on enterprise Tobiko
    Cloud projects:

    - The OSS ``SnowflakeConnectionConfig`` declares
      ``application: Literal["Tobiko_SQLMesh"]``, which pydantic rejects the
      enterprise value against.
    - ``sqlmesh.core.config.loader`` uses strict ``type(config) != Config``
      checks in three places; an ``EnterpriseConfig`` subclass fails the
      check and gets re-instantiated as plain ``Config(extra="forbid")``,
      dropping enterprise-only fields like ``allow_prod_deploy``.

    Tobiko's own ``tcloud`` sidesteps both by going through ``EnterpriseContext``
    rather than ``sqlmesh.Context``; we can't import ``EnterpriseContext``
    without confirming its path, so we apply two targeted runtime patches
    instead. Both gated on ``tobikodata`` being importable so an OSS-only
    install is untouched. Idempotent via sentinel attrs.
    """
    try:
        import tobikodata  # noqa: F401
    except ImportError:
        return

    # Patch 1: relax SnowflakeConnectionConfig.application Literal so the
    # enterprise value "Tobiko_TobikoCloud" validates. The field is only used
    # as a client-identifier string passed to Snowflake's connector — there's
    # no semantic value in pinning it to a single Literal.
    try:
        from sqlmesh.core.config.connection import SnowflakeConnectionConfig

        if not getattr(
            SnowflakeConnectionConfig, _TOBIKO_SNOWFLAKE_APP_PATCH_SENTINEL, False
        ):
            field = SnowflakeConnectionConfig.model_fields.get("application")
            if field is not None:
                field.annotation = str
                SnowflakeConnectionConfig.model_rebuild(force=True)
                setattr(
                    SnowflakeConnectionConfig,
                    _TOBIKO_SNOWFLAKE_APP_PATCH_SENTINEL,
                    True,
                )
    except ImportError:
        # tobikodata is installed (checked above), so sqlmesh is too — an
        # ImportError here means the private API moved. Log loudly and carry on
        # without the patch, matching the file's stated policy; a Tobiko Cloud
        # Snowflake project may then fail Context init on the application Literal.
        logger.warning(
            "Could not apply the SnowflakeConnectionConfig.application compat "
            "patch (private sqlmesh API moved in this version). Loading a Tobiko "
            "Cloud Snowflake project may fail on the 'application' Literal.",
            exc_info=True,
        )

    # Patch 2: short-circuit convert_config_type when the object is already an
    # instance of the target type. The OSS loader otherwise re-instantiates
    # any non-exact-type config through ``config_type.parse_obj(obj.dict())``,
    # which drops enterprise-only fields and chokes on extra="forbid". A
    # single replacement at this seam covers all three strict-type call sites
    # in sqlmesh/core/config/loader.py (lines 55, 188, 246).
    try:
        import sqlmesh.core.config.loader as _loader_mod

        if not getattr(
            _loader_mod.convert_config_type, _TOBIKO_CONVERT_PATCH_SENTINEL, False
        ):
            _original_convert = _loader_mod.convert_config_type

            def _convert_config_type_isinstance(config_obj, config_type):  # type: ignore[no-untyped-def]
                if isinstance(config_obj, config_type):
                    return config_obj
                return _original_convert(config_obj, config_type)

            _convert_config_type_isinstance._datahub_convert_config_type_isinstance_patched = True  # type: ignore[attr-defined]
            _loader_mod.convert_config_type = _convert_config_type_isinstance
    except ImportError:
        # As above: sqlmesh is present, so this is a moved private API. Log it
        # rather than silently dropping the patch — an EnterpriseConfig project
        # may then lose enterprise-only fields on load.
        logger.warning(
            "Could not apply the convert_config_type compat patch (private "
            "sqlmesh API moved in this version). EnterpriseConfig projects may "
            "drop enterprise-only fields when loaded.",
            exc_info=True,
        )


# Sentinel attribute used to make _install_tobiko_local_state_fallback_shim()
# idempotent across multiple ingest runs in the same process.
_TOBIKO_SHIM_SENTINEL_ATTR = "_datahub_local_state_shim_installed"

# Invoked with a human-readable reason when the local-state fallback actually
# activates, so the running source can put it on its ingestion report. The
# patch is installed once per process but a process may run several
# ingestions, so the callback is read at call time, not captured at install.
_tobiko_local_state_fallback_callback: Optional[Callable[[str], None]] = None


def _install_tobiko_local_state_fallback_shim(
    on_fallback: Optional[Callable[[str], None]] = None,
) -> None:
    """When tobikodata is installed but no Tobiko Cloud token is configured,
    let SQLMesh's Context init succeed against an EnterpriseConfig project by
    swapping the cloud state sync for an in-memory DuckDB stub.

    The shim only catches the very specific ConfigError raised by
    ``RemoteCloudSchedulerConfig.get_cloud_connection()`` when creds are
    absent; any other scheduler failure surfaces. No-op when tobikodata isn't
    installed (OSS-only projects don't have a cloud scheduler to patch).

    ``on_fallback`` is called if and when the fallback is actually used, so the
    caller can surface it on the ingestion report — the degraded mode silently
    drops every state-derived signal.
    """
    global _tobiko_local_state_fallback_callback
    _tobiko_local_state_fallback_callback = on_fallback

    try:
        from tobikodata.sqlmesh_enterprise.config.scheduler import (  # type: ignore[import-not-found]
            RemoteCloudSchedulerConfig,
        )
    except ImportError:
        return

    if getattr(RemoteCloudSchedulerConfig, _TOBIKO_SHIM_SENTINEL_ATTR, False):
        return

    from sqlmesh.core.config.connection import DuckDBConnectionConfig
    from sqlmesh.core.state_sync import EngineAdapterStateSync
    from sqlmesh.utils.errors import ConfigError

    _original_create = RemoteCloudSchedulerConfig.create_state_sync
    _original_fingerprint = RemoteCloudSchedulerConfig.state_sync_fingerprint

    def _create_state_sync_with_fallback(self, context):  # type: ignore[no-untyped-def]
        try:
            return _original_create(self, context)
        except ConfigError as e:
            if _TOBIKO_CLOUD_NO_CREDS_ERR_FRAGMENT not in str(e):
                raise
            reason = (
                "Tobiko Cloud state store unreachable (no token configured). "
                "Falling back to an in-memory DuckDB state so the SQLMesh "
                "Context can initialise from project files. Snapshot history "
                "and environment promotions read from cloud state are "
                "unavailable in this mode. Set tobiko_cloud_token / "
                "tobiko_cloud_token_file to read from the real cloud state."
            )
            logger.info(reason)
            if _tobiko_local_state_fallback_callback is not None:
                _tobiko_local_state_fallback_callback(reason)
            engine_adapter = DuckDBConnectionConfig().create_engine_adapter()
            schema = context.config.get_state_schema(context.gateway)
            return EngineAdapterStateSync(
                engine_adapter,
                schema=schema,
                cache_dir=context.cache_dir,
                console=context.console,
            )

    def _state_sync_fingerprint_with_fallback(self, context):  # type: ignore[no-untyped-def]
        try:
            return _original_fingerprint(self, context)
        except ConfigError as e:
            if _TOBIKO_CLOUD_NO_CREDS_ERR_FRAGMENT not in str(e):
                raise
            return "datahub-tobiko-local-state-fallback"

    RemoteCloudSchedulerConfig.create_state_sync = _create_state_sync_with_fallback
    RemoteCloudSchedulerConfig.state_sync_fingerprint = (
        _state_sync_fingerprint_with_fallback
    )
    setattr(RemoteCloudSchedulerConfig, _TOBIKO_SHIM_SENTINEL_ATTR, True)


def _tobiko_state_connection_env_keys(gateway: str) -> Dict[str, str]:
    """Return the SQLMesh env-var keys that override a gateway's state
    connection. Matches what tcloud's installer.py does verbatim — this is
    the only injection channel tobikodata exposes."""
    prefix = f"SQLMESH__GATEWAYS__{gateway.upper()}__STATE_CONNECTION"
    return {
        "TYPE": f"{prefix}__TYPE",
        "URL": f"{prefix}__URL",
        "TOKEN": f"{prefix}__TOKEN",
    }


@contextlib.contextmanager
def _scoped_tobiko_cloud_env(
    token: Optional[str], gateway: Optional[str], url: Optional[str]
) -> Iterator[None]:
    """Scope SQLMESH__GATEWAYS__<gw>__STATE_CONNECTION__{TYPE,URL,TOKEN} env
    vars to a single block, restoring previous values on exit.

    tobikodata exposes no programmatic injection API for cloud creds — even
    tcloud itself sets these env vars (see
    tcloud/installer.py:_configure_state_connection). We mirror tcloud's
    pattern: TYPE and URL are always injected when tobiko_cloud_url is
    configured; TOKEN is injected only when a static token is explicitly set.
    Without TOKEN, tobikodata falls back to SSO auth via ~/.tcloud/auth.yaml,
    matching the normal tcloud SSO flow.

    No-op when gateway is not configured or neither url nor token is set
    (OSS SQLMesh projects that don't use Tobiko Cloud).
    """
    if gateway is None or (url is None and token is None):
        yield
        return

    keys = _tobiko_state_connection_env_keys(gateway)
    tracked = [keys["TYPE"], keys["TOKEN"], keys["URL"], "SQLMESH__DEFAULT_GATEWAY"]
    saved: Dict[str, Optional[str]] = {k: os.environ.get(k) for k in tracked}

    os.environ[keys["TYPE"]] = "cloud"
    if url:
        os.environ[keys["URL"]] = url
    if token:
        os.environ[keys["TOKEN"]] = token
    else:
        # No static token → SSO fallback via ~/.tcloud/auth.yaml. Clear any
        # ambient TOKEN so a stale value from the environment can't silently
        # authenticate this run (the previous value is restored on exit; the
        # key is already tracked in `saved`).
        os.environ.pop(keys["TOKEN"], None)
    os.environ["SQLMESH__DEFAULT_GATEWAY"] = gateway
    try:
        yield
    finally:
        for k, original in saved.items():
            if original is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = original
