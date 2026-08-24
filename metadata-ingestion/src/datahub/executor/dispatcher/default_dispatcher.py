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

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def dispatch_async(
    executor: Executor, request: ExecutionRequest, close_callback: Callable[[], None]
) -> None:
    try:
        res = executor.execute(request)
        res.pretty_print_summary()
    except Exception:
        logger.error(
            f"Failed dispatch for {request.exec_id}: {traceback.format_exc(limit=3)}"
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
