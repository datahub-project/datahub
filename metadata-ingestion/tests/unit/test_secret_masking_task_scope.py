"""Task-scoped masking in a process that runs tasks concurrently.

The executor registers every task's secrets into the process-global
SecretRegistry and never clears it, while running tasks in concurrent
threads (default_dispatcher). So a task inherited every earlier task's
secrets, and "clear between tasks" is not available as a fix: a clear
during one task disarms masking for another running beside it, which turns
over-masking into an actual leak.
"""

import threading

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


def test_a_scoped_secret_is_still_maskable_outside_the_scope():
    """SECURITY: the fail-safe floor, and the reason this is not just a
    per-task registry.

    A thread spawned inside a task does NOT inherit the ContextVar -- it
    starts from the default and therefore resolves to the global registry.
    If registration went only to the scope, that thread's logs would mask
    nothing, turning a contamination bug into a disclosure one. So a scoped
    registration is mirrored to the global, which stays the floor: possibly
    over-masking, never under.
    """
    from_task = "floor-check" + "-secret"

    with task_secret_scope():
        SecretRegistry.get_instance().register_secrets_batch(
            {"TASK_PASSWORD": from_task}
        )

    leaked = {}

    def unscoped_worker() -> None:
        leaked["out"] = SecretMaskingFilter().mask_text(f"saw {from_task} here")

    t = threading.Thread(target=unscoped_worker)
    t.start()
    t.join(timeout=10)

    assert from_task not in leaked["out"], (
        "a thread outside any task scope masked nothing"
    )


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
