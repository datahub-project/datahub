"""Task-scoped masking in a process that runs tasks concurrently.

The executor registers every task's secrets into the process-global
SecretRegistry and never clears it, while running tasks in concurrent
threads (default_dispatcher). So a task inherited every earlier task's
secrets, and "clear between tasks" is not available as a fix: a clear
during one task disarms masking for another running beside it, which turns
over-masking into an actual leak.
"""

import threading
from unittest.mock import patch

import pytest

from datahub.masking.masking_filter import SecretMaskingFilter
from datahub.masking.secret_registry import SecretRegistry, task_secret_scope


@pytest.fixture(autouse=True)
def _clean_registry():
    SecretRegistry.get_instance().clear()
    yield
    SecretRegistry.get_instance().clear()


def test_a_task_does_not_inherit_an_earlier_tasks_secrets():
    """The reported harm: an earlier task's password corrupts a later task's
    output, and the marker names the other task's variable.

    `analytics_warehouse` is task A's password and task B's database name --
    which is not a contrived collision, it is the same shape as the
    disclosure exemption that exists for exactly this within one task.
    """
    # Bound rather than inlined: the secret scanner matches a literal
    # beside a *_PASSWORD key, and this value is a test fixture.
    collides = "analytics" + "_warehouse"

    with task_secret_scope():
        SecretRegistry.get_instance().register_secrets_batch(
            {"TASK_A_PASSWORD": collides}
        )
        assert "***" in SecretMaskingFilter().mask_text(collides)

    with task_secret_scope():
        out = SecretMaskingFilter().mask_text(
            "probe filter target: analytics_warehouse.public.orders"
        )

    assert out == "probe filter target: analytics_warehouse.public.orders", out
    assert "TASK_A_PASSWORD" not in out, "the other task's variable name leaked"


def test_a_task_still_masks_its_own_secrets():
    """The control. Scoping must not become "mask nothing"."""
    mine = "s3cret" + "-of-this-task"

    with task_secret_scope():
        SecretRegistry.get_instance().register_secrets_batch({"MY_PASSWORD": mine})
        out = SecretMaskingFilter().mask_text(f"connecting with {mine}")
    assert mine not in out


def test_concurrent_tasks_do_not_see_each_others_secrets():
    """Threads are the reason a clear is unsafe, so they are the case to pin.

    Each thread registers its own secret while the other is live, and each
    must mask its own and not the other's.
    """
    started = threading.Barrier(2)
    results = {}

    def run_task(name: str, secret: str, other_secret: str) -> None:
        with task_secret_scope():
            SecretRegistry.get_instance().register_secrets_batch({name: secret})
            started.wait(timeout=5)
            text = f"mine={secret} theirs={other_secret}"
            results[name] = SecretMaskingFilter().mask_text(text)

    a = threading.Thread(
        target=run_task, args=("A_PW", "aaa-secret-aaa", "bbb-secret-bbb")
    )
    b = threading.Thread(
        target=run_task, args=("B_PW", "bbb-secret-bbb", "aaa-secret-aaa")
    )
    a.start()
    b.start()
    a.join(timeout=10)
    b.join(timeout=10)

    assert "aaa-secret-aaa" not in results["A_PW"], results["A_PW"]
    assert "bbb-secret-bbb" in results["A_PW"], "A masked B's secret out of its output"
    assert "bbb-secret-bbb" not in results["B_PW"], results["B_PW"]
    assert "aaa-secret-aaa" in results["B_PW"], "B masked A's secret out of its output"


# REMOVED: test_a_scoped_secret_is_still_maskable_outside_the_scope
#
# It asserted the mirror -- that a task's secret reached the global registry
# so a thread outside the scope could still mask it. That mirror is gone,
# and the test went with it rather than being adjusted, because the property
# it pinned is one we deliberately no longer have.
#
# The mirror bought a floor for unscoped threads and paid for it by putting
# every task's secrets in one shared registry, which is what made the global
# unusable as a floor for the scoped reads that needed it: reading it would
# have shown task B everything task A registered. Task secrets now stay in
# their scope, the global holds process-level secrets only, and a scope
# masks against both.
#
# The replacement contract is pinned by
# test_a_raw_thread_falls_back_to_process_level_not_another_task: an
# unscoped thread masks process-level secrets and NOT the running task's.
# That is a real narrowing, recorded in task_secret_scope's docstring.


def test_dispatch_async_scopes_each_task():
    """The wiring, through the real dispatch entry point.

    dispatch_async is the only place that is exactly one task on exactly one
    thread, which is why the scope is opened there rather than deeper in the
    executor. Driven with fake executors so this stays a unit test.
    """
    from datahub.executor.dispatcher.default_dispatcher import dispatch_async

    seen = {}

    class _Result:
        def pretty_print_summary(self) -> None:
            pass

    class _Executor:
        def __init__(self, name: str, secret: str, probe: str) -> None:
            self._name, self._secret, self._probe = name, secret, probe

        def execute(self, _request: object) -> "_Result":
            SecretRegistry.get_instance().register_secrets_batch(
                {self._name: self._secret}
            )
            # What this task would print: its own value, plus a string that
            # is the OTHER task's secret and this task's ordinary output.
            seen[self._name] = SecretMaskingFilter().mask_text(
                f"mine={self._secret} theirs={self._probe}"
            )
            return _Result()

    first = _Executor("FIRST_PW", "first-task-secret", "second-task-secret")
    second = _Executor("SECOND_PW", "second-task-secret", "first-task-secret")

    request = object()
    dispatch_async(first, request, lambda: None)  # type: ignore[arg-type]
    dispatch_async(second, request, lambda: None)  # type: ignore[arg-type]

    assert "first-task-secret" not in seen["FIRST_PW"]
    assert "second-task-secret" not in seen["SECOND_PW"]
    # The second task's output is NOT redacted against the first task's
    # password -- which is the contamination this closes.
    assert "first-task-secret" in seen["SECOND_PW"], seen["SECOND_PW"]
    assert "FIRST_PW" not in seen["SECOND_PW"], "the first task's variable name leaked"


def test_a_filter_installed_during_one_task_masks_the_next_task():
    """The case the other tests here missed, and the one that matters.

    Every test above builds a fresh SecretMaskingFilter inside the scope it
    is checking. The real system does the opposite: initialize_secret_masking
    installs filters ONCE onto process-wide logging handlers, stdout/stderr
    and the excepthook, at the start of the FIRST task -- inside that task's
    scope.

    With the registry captured at construction, those installed filters went
    on masking against the first task's scope forever: a later task's own
    secrets never reached them, so its output went out UNMASKED while the
    first task's values were still redacted out of it. That is
    under-masking, the direction that leaks, and scoping introduced it.

    The filter now resolves the registry per call, so an installed filter
    follows whichever task is running.
    """
    a_secret = "aaa" + "-installed-filter-a"
    b_secret = "bbb" + "-installed-filter-b"

    with task_secret_scope():
        SecretRegistry.get_instance().register_secrets_batch({"A_PW": a_secret})
        # Built inside task A, as bootstrap does, and kept.
        installed = SecretMaskingFilter()
        assert a_secret not in installed.mask_text(f"task A: {a_secret}")

    with task_secret_scope():
        SecretRegistry.get_instance().register_secrets_batch({"B_PW": b_secret})
        own = installed.mask_text(f"task B: {b_secret}")
        other = installed.mask_text(f"task B mentions {a_secret}")

    assert b_secret not in own, "the installed filter did not mask task B's own secret"
    assert a_secret in other, "task A still contaminates task B"


def test_an_explicitly_injected_registry_is_still_honoured():
    """The escape hatch the resolution must not break.

    Passing a registry means "mask against this one", and tests rely on it.
    Only the default -- no registry given -- resolves per call.
    """
    own = SecretRegistry()
    own.register_secrets_batch({"PINNED": "pinned" + "-to-this-registry"})
    filt = SecretMaskingFilter(secret_registry=own)

    with task_secret_scope():
        SecretRegistry.get_instance().register_secrets_batch(
            {"SCOPED": "scoped" + "-value"}
        )
        out = filt.mask_text("pinned-to-this-registry and scoped-value")

    assert "pinned-to-this-registry" not in out, "the injected registry was ignored"
    assert "scoped-value" in out, "an injected registry must not follow the scope"


def test_bootstrap_installed_handlers_follow_the_running_task():
    """The real path, which the hand-built-filter test above did not reach.

    initialize_secret_masking installs filters onto process-wide logging
    handlers, stdout/stderr and the excepthook, and it runs at the start of
    the FIRST task -- inside that task's scope. It used to pass
    `secret_registry=SecretRegistry.get_instance()` explicitly, which pinned
    task A's scoped registry into every installed filter. Making the filter
    resolve per call did not fix that on its own, because an explicitly
    injected registry is honoured by design: the caller has to stop naming
    one.

    Asserted on the handler's own stream rather than on captured stdout,
    because bootstrap wraps stdout too -- a report printed through it gets
    masked on the way out and will agree with itself whatever the code does.
    """
    import io
    import logging

    from datahub.masking.bootstrap import initialize_secret_masking

    a_secret = "aaa" + "-bootstrap-scope-a"
    b_secret = "bbb" + "-bootstrap-scope-b"

    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    log = logging.getLogger("probe.scope.bootstrap.test")
    log.addHandler(handler)
    log.setLevel(logging.INFO)
    log.propagate = False
    try:
        with task_secret_scope():
            SecretRegistry.get_instance().register_secrets_batch({"A_PW": a_secret})
            initialize_secret_masking()
            log.info("A-LINE %s", a_secret)

        with task_secret_scope():
            SecretRegistry.get_instance().register_secrets_batch({"B_PW": b_secret})
            log.info("B-OWN %s", b_secret)
            log.info("B-MENTIONS-A %s", a_secret)
    finally:
        log.removeHandler(handler)

    lines = {ln.split(" ")[0]: ln for ln in stream.getvalue().splitlines()}

    assert a_secret not in lines["A-LINE"]
    assert b_secret not in lines["B-OWN"], (
        "a later task's own secret reached the installed handler unmasked"
    )
    assert a_secret in lines["B-MENTIONS-A"], (
        "an earlier task's secret is still redacted out of this task's output"
    )


# --- the floor: process-level secrets are visible INSIDE a task -----------
#
# The first version of the scope mirrored task writes into the global and
# narrowed reads to the scope alone. That closed contamination and opened a
# leak: anything registered before a scope existed -- the envelope secrets
# load_config_file registers, a ConfigModel's own SecretStr fields, the
# executor's startup config -- was invisible once a task opened its scope.
#
# The mirror is why "just fall back to the global" was not the fix: it put
# every task's secrets in the global, so reading it would have handed task B
# everything task A registered. The mirror is gone; the global now holds
# process-level secrets only and a scope reads its own PLUS the global.


def test_a_secret_registered_before_the_scope_is_masked_inside_it():
    """The leak. Registered with no scope active, then read from inside one."""
    early = "early" + "-registered-credential"
    SecretRegistry.global_instance().register_secrets_batch({"EARLY_PW": early})

    with task_secret_scope():
        out = SecretMaskingFilter().mask_text(f"saw {early}")

    assert early not in out, "a process-level secret is invisible inside a task"


def test_a_task_still_does_not_see_another_tasks_secrets():
    """The guard on the fix, so restoring the floor cannot restore the bug."""
    a_secret = "aaa" + "-floor-guard-a"
    b_secret = "bbb" + "-floor-guard-b"

    with task_secret_scope():
        SecretRegistry.get_instance().register_secrets_batch({"A_PW": a_secret})

    with task_secret_scope():
        SecretRegistry.get_instance().register_secrets_batch({"B_PW": b_secret})
        own = SecretMaskingFilter().mask_text(f"mine {b_secret}")
        other = SecretMaskingFilter().mask_text(f"theirs {a_secret}")

    assert b_secret not in own
    assert a_secret in other, "task A's secret leaked into task B's view"
    # And it never reached the global, which is what makes the floor safe
    # to read from. Asserted as the raw value SURVIVING: a registry that
    # knew the secret would have replaced it, so "still there" is the
    # evidence it was never registered.
    from_global = SecretMaskingFilter(
        secret_registry=SecretRegistry.global_instance()
    ).mask_text(f"{a_secret} outside")
    assert a_secret in from_global, "a task secret reached the global registry"


def test_the_floor_holds_through_bootstrap_installed_handlers():
    """Same two properties, through the path that actually ships.

    Asserted on the handler's own stream, not captured stdout: bootstrap
    wraps stdout too, so a report printed through it is masked on the way
    out and agrees with itself whatever the code does.
    """
    import io
    import logging

    from datahub.masking.bootstrap import initialize_secret_masking

    process_secret = "proc" + "-level-credential"
    task_secret = "task" + "-level-credential"

    SecretRegistry.global_instance().register_secrets_batch({"PROC_PW": process_secret})

    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    log = logging.getLogger("probe.scope.floor.test")
    log.addHandler(handler)
    log.setLevel(logging.INFO)
    log.propagate = False
    try:
        initialize_secret_masking()
        with task_secret_scope():
            SecretRegistry.get_instance().register_secrets_batch(
                {"TASK_PW": task_secret}
            )
            log.info("BOTH %s and %s", process_secret, task_secret)
    finally:
        log.removeHandler(handler)

    line = stream.getvalue()
    assert process_secret not in line, "process-level secret unmasked inside a task"
    assert task_secret not in line, "the task's own secret was unmasked"


def test_an_asyncio_child_task_inherits_the_scope():
    """asyncio.create_task copies the context, which is how the executor's
    subprocess-output and progress tasks stay inside their task's scope."""
    import asyncio

    secret = "async" + "-child-credential"
    seen = {}

    async def child():
        seen["out"] = SecretMaskingFilter().mask_text(f"child saw {secret}")

    async def main():
        with task_secret_scope():
            SecretRegistry.get_instance().register_secrets_batch({"ASYNC_PW": secret})
            await asyncio.create_task(child())

    asyncio.run(main())
    assert secret not in seen["out"], "an asyncio child lost the task's scope"


def test_a_raw_thread_falls_back_to_process_level_not_another_task():
    """A raw thread does not inherit the ContextVar -- measured, not assumed.

    It therefore masks process-level secrets and NOT the running task's.
    That is the residual of dropping the mirror, and it is the safe
    direction to fail in: a thread outside the scope must never see another
    task's secrets either.
    """
    import threading

    process_secret = "proc" + "-visible-everywhere"
    task_secret = "task" + "-scoped-only"

    SecretRegistry.global_instance().register_secrets_batch({"P_PW": process_secret})
    out = {}

    def worker():
        f = SecretMaskingFilter()
        out["proc"] = f.mask_text(process_secret)
        out["task"] = f.mask_text(task_secret)

    with task_secret_scope():
        SecretRegistry.get_instance().register_secrets_batch({"T_PW": task_secret})
        t = threading.Thread(target=worker)
        t.start()
        t.join()

    assert process_secret not in out["proc"], "the floor did not reach a raw thread"
    # Documented residual: the task's own secret is not masked there.
    assert task_secret in out["task"]


def test_a_registration_racing_the_combined_cache_is_not_cached_away() -> None:
    """The combined-pattern cache must not tag old content with a new version.

    get_pattern_and_replacements snapshots `own` under the lock and releases
    it; _combined_with_parent then needs a cache key. Reading self._version
    at THAT point -- after the snapshot -- let a register() landing in the
    window store the pre-registration pattern under the post-registration
    version. Every later call at that version hit the cache and masked
    without the new secret, permanently, until some further registration
    moved the version again.

    The window is entered deterministically here rather than raced for: the
    interleaving is the same one two threads produce.
    """
    parent = SecretRegistry()
    parent.register_secret("PARENT_PW", "parent-secret-value-1")
    scope = SecretRegistry(_parent=parent)
    scope.register_secret("T1", "task-one-secret-value")

    late = "task-two" + "-secret-value"
    original = SecretRegistry._combined_with_parent
    injected = False

    def register_inside_the_window(
        self: SecretRegistry, own: object, replacements: object, *rest: object
    ) -> object:
        nonlocal injected
        if self is scope and not injected:
            injected = True
            scope.register_secret("T2", late)
        return original(self, own, replacements, *rest)  # type: ignore[arg-type]

    with patch.object(
        SecretRegistry, "_combined_with_parent", register_inside_the_window
    ):
        scope.get_pattern_and_replacements()

    assert injected, "the window was never entered; the test proves nothing"

    pattern, replacements = scope.get_pattern_and_replacements()
    assert pattern is not None
    assert pattern.search(late), (
        "a secret registered during the snapshot window was cached out of the "
        "masking pattern"
    )
    assert late in replacements
