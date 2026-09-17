# Copyright 2021 Acryl Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import threading
import traceback
import uuid
from typing import Callable

from datahub.executor.dispatcher.dispatcher import Dispatcher
from datahub.executor.execution.executor import Executor
from datahub.executor.request.execution_request import ExecutionRequest
from datahub.executor.request.signal_request import SignalRequest
from datahub.masking.secret_registry import task_secret_scope

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def dispatch_async(
    executor: Executor, request: ExecutionRequest, close_callback: Callable[[], None]
) -> None:
    try:
        # SECURITY: one masking scope per task, opened here because this is
        # the only place that is exactly one task on exactly one thread.
        #
        # Secrets were registered into a process-global registry that is
        # never cleared, so every task inherited every earlier task's. The
        # visible harm is a later task's own output being redacted against an
        # unrelated task's password -- and the marker names that task's
        # variable, so on a shared executor one tenant's recipe leaks into
        # another's output.
        #
        # Clearing between tasks is not available as a fix: dispatch runs
        # tasks concurrently, so a clear during one disarms masking for
        # another running beside it. Scoping needs no coordination between
        # tasks. Registration still reaches the global registry, which stays
        # the fail-safe floor -- see task_secret_scope.
        #
        # The summary print AND the failure traceback are inside the scope.
        # An earlier version of this said so while leaving the `except`
        # outside the `with`, which is worse than not scoping at all: the
        # scope's finally resets the ContextVar before the log line runs, so
        # the failure path -- the one that renders a traceback, the leakiest
        # channel here -- was masked against the global registry while the
        # comment claimed it was masked against this task's.
        with task_secret_scope():
            try:
                res = executor.execute(request)
                res.pretty_print_summary()
            except Exception:
                logger.error(
                    f"Failed dispatch for {request.exec_id}: "
                    f"{traceback.format_exc(limit=3)}"
                )
    finally:
        close_callback()


def dispatch_signal_async(
    executor: Executor, request: SignalRequest, close_callback: Callable[[], None]
) -> None:
    try:
        executor.signal(request)
    except Exception:
        logger.error(
            f"Failed signal dispatch for {request.exec_id}: {traceback.format_exc(limit=3)}"
        )
    finally:
        close_callback()


# An abstract base class representing a dispatcher capable of dispatching Execution Requests
class DefaultDispatcher(Dispatcher):
    def __init__(self, executors: list[Executor]):
        self.executors: list[Executor] = executors
        self.threads: dict[uuid.UUID, threading.Thread] = {}

    def dispatch(self, request: ExecutionRequest) -> None:
        # Determine which executor should handle the request.
        thread_id = uuid.uuid4()
        for executor in self.executors:
            if executor.get_id() == request.executor_id:
                # Simply execute the task on a new thread.
                thread = threading.Thread(
                    target=dispatch_async,
                    args=(executor, request, lambda: self.threads.pop(thread_id, None)),
                )
                self.threads[thread_id] = thread
                thread.start()
                logger.debug(f"Started thread {thread} for {request.exec_id}")
                return

        raise Exception(
            f"Failed to find executor {request.executor_id} for Execution Request with execution id {request.exec_id}"
        )

    def dispatch_signal(self, request: SignalRequest) -> None:
        # Determine which executor should handle the signal.
        thread_id = uuid.uuid4()
        for executor in self.executors:
            if executor.get_id() == request.executor_id:
                # Simply execute the task on a new thread.
                thread = threading.Thread(
                    target=dispatch_signal_async,
                    args=(executor, request, lambda: self.threads.pop(thread_id, None)),
                )
                self.threads[thread_id] = thread
                thread.start()
                logger.debug(f"Started signal thread {thread} for {request.exec_id}")
                return

        raise Exception(
            f"Failed to find executor {request.executor_id} for Signal Request with execution id {request.exec_id}."
        )

    def shutdown(self):
        for t in self.threads.values():
            t.join()
        self.threads.clear()
