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

import asyncio
import logging
import traceback
from datetime import datetime, timezone
from typing import Optional

from datahub.executor.common.config import ConfigModel
from datahub.executor.context.execution_context import ExecutionContext
from datahub.executor.context.executor_context import ExecutorContext
from datahub.executor.execution.executor import Executor
from datahub.executor.execution.task import Task, TaskConfig
from datahub.executor.execution.task_registry import TaskRegistry
from datahub.executor.request.execution_request import ExecutionRequest
from datahub.executor.request.signal_request import SignalRequest
from datahub.executor.result.execution_result import ExecutionResult, Type
from datahub.executor.secret.secret_store_registry import SecretStoreRegistry
from datahub.secret.secret_store import SecretStore, SecretStoreConfig

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

"""
Sets up the Task Executor
"""


class DefaultExecutorConfig(ConfigModel):
    id: str
    task_configs: list[TaskConfig]
    secret_stores: list[SecretStoreConfig] = []

    executor_instance_id: Optional[str] = None
    executor_version: Optional[str] = None


"""
Executes a Task Execution Request
"""


class DefaultExecutor(Executor):
    id: str

    # Tasks
    task_registry: TaskRegistry = TaskRegistry()
    task_instances: dict[str, Task] = {}

    # Execution event loops + futures.
    task_event_loops: dict[str, asyncio.AbstractEventLoop] = {}
    task_futures: dict[str, asyncio.Task] = {}

    # Secret Stores
    secret_stores: list[SecretStore] = []

    def __init__(self, config: DefaultExecutorConfig) -> None:
        # Register the tasks
        self.id = config.id
        self.secret_stores = self._create_secret_stores(config)
        for task_config in config.task_configs:
            self.register_task(task_config)
        self._config = config

    def register_task(self, task: TaskConfig) -> None:
        # Store a registry of command -> task.
        self.task_registry.register_lazy(task.name, task.type)

        # Create and initialize the task instances
        task_class = self.task_registry.get(task.name)
        try:
            executor_context = ExecutorContext(self.id, self.secret_stores)
            task_instance = task_class.create(task.configs, executor_context)
            self.task_instances[task.name] = task_instance
        except Exception:
            # `from err`/`from None` both alter the rendered traceback (the message
            # already embeds format_exc), which reaches users and tests.
            raise Exception(  # noqa: B904
                f"Failed to create instance of task with name {task.name}: {traceback.format_exc(limit=3)}"
            )

    # Run a list of tasks in sequence
    def execute(self, request: ExecutionRequest) -> ExecutionResult:
        # 1. Create execution context
        executor_context = ExecutorContext(
            executor_id=self.id, secret_stores=self.secret_stores
        )

        # 2. Execute the task in the request
        return self.execute_task(request, executor_context)

    def execute_task(
        self, request: ExecutionRequest, executor_context: ExecutorContext
    ) -> ExecutionResult:
        # ASSUMPTION: Each time execute_task is called, it is called from a different thread.

        # 1. Build task execution context
        execution_context = ExecutionContext(request)
        execution_id = execution_context.get_execution_id()
        execution_report = execution_context.get_report()
        execution_report.report_info(
            f"Starting execution for task with name={request.name}"
        )

        if self._config.executor_instance_id is not None:
            identity_message = (
                f"Task claimed by executor instance {self._config.executor_instance_id}"
            )
            if self._config.executor_version is not None:
                identity_message = (
                    f"{identity_message}; version = {self._config.executor_version}"
                )
            execution_report.report_info(identity_message)

        assert execution_id not in self.task_futures, (
            "Already running task with same execution ID"
        )

        # 2. Retrieve an instance of the task
        task_instance = self.task_instances.get(request.name)
        execution_result = ExecutionResult(execution_context)
        if task_instance is None:
            execution_report.report_info(
                f"Failed to find task with name={request.name}"
            )
            return execution_result

        # 3. Execute task - task is expected to throw TaskError on failure.
        try:
            # 3.1. Setup event loop for task execution.
            task_event_loop = asyncio.new_event_loop()
            self.task_event_loops[execution_id] = task_event_loop
            asyncio.set_event_loop(task_event_loop)

            # 3.2. Execute task via the thread's event loop. Store the executing task such that it can be cancelled.
            # TODO: Have an async version of this method.
            task_future = task_event_loop.create_task(
                task_instance.execute(request.args, execution_context)
            )
            task_future.set_name(f"dispatch-{execution_id}")
            logger.debug(f"Task for {execution_id} created")
            self.task_futures[execution_id] = task_future
            task_event_loop.run_until_complete(task_future)
            logger.debug(f"Task for {execution_id} completed")
        except Exception:
            execution_report.report_info(
                f"Caught exception EXECUTING task_id={execution_context.get_execution_id()}, name={request.name}, stacktrace={traceback.format_exc(limit=20)}"
            )
            return execution_result
        except asyncio.exceptions.CancelledError:
            execution_report.report_info(
                f"Execution cancelled while EXECUTING task_id={execution_context.get_execution_id()}, name={request.name}, stacktrace={traceback.format_exc(limit=3)}"
            )
            execution_result.set_result_type(Type.CANCELLED)
            return execution_result
        finally:
            if task_event_loop:
                task_event_loop.close()
            if execution_id in self.task_futures:
                del self.task_futures[execution_id]
            if execution_id in self.task_event_loops:
                del self.task_event_loops[execution_id]

            logger.debug(f"Cleaned up task for {execution_id}")

        execution_report.report_info(
            f"Finished execution for task with name={request.name}"
        )
        execution_result.set_result_type(Type.SUCCESS)
        execution_result.end_time = datetime.now(timezone.utc)
        return execution_result

    def signal(self, request: SignalRequest) -> None:
        if request.signal == "KILL":
            task_future = self.task_futures.get(request.exec_id)
            if task_future is None:
                logger.error(f"Received KILL for missing task ID {request.exec_id}")
            else:
                event_loop = self.task_event_loops[request.exec_id]
                # Cancel the task if not complete.
                if not task_future.done():
                    logger.debug(f"Trying to cancel {task_future}")
                    event_loop.call_soon_threadsafe(task_future.cancel)

    def shutdown(self) -> None:
        for task_name, task_instance in self.task_instances.items():
            try:
                task_instance.close()
            except Exception:
                logger.warning(f"Failed to shutdown task with name {task_name}")

    def get_id(self) -> str:
        return self.id

    def _create_secret_stores(self, config: DefaultExecutorConfig) -> list[SecretStore]:
        secret_stores = []
        for secret_store_config in config.secret_stores:
            secret_stores.append(self._create_secret_store(secret_store_config))
        return secret_stores

    def _create_secret_store(self, config: SecretStoreConfig) -> SecretStore:
        # Create a secret store registry
        secret_store_registry: SecretStoreRegistry = SecretStoreRegistry()

        # Get the secret store configs
        secret_store_type: str = config.type

        # Fetch the correct secret store, or register a new one
        if not secret_store_registry.is_enabled(secret_store_type):
            # Custom Secret Store found. Register it.
            secret_store_registry.register_lazy(secret_store_type, secret_store_type)

        # Instantiate the secret store class
        secret_store_class = secret_store_registry.get(secret_store_type)

        # Create & return new instance of secret store
        return secret_store_class.create(config.config)
