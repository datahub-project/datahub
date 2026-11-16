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
import traceback
from dataclasses import dataclass
from threading import RLock, Thread
from typing import Dict, Optional

from datahub_actions.pipeline.pipeline import Pipeline

logger = logging.getLogger(__name__)


@dataclass
class PipelineSpec:
    # The pipeline name
    name: str

    # The pipeline
    pipeline: Pipeline

    # The thread which is executing the pipeline.
    thread: Thread

    # Set when the pipeline ended with an exception rather than its source finishing.
    failed: bool = False


# Run a pipeline in blocking fashion. Returns False if it ended with an exception.
def run_pipeline(pipeline: Pipeline) -> bool:
    try:
        pipeline.run()
    except Exception:
        # Not only PipelineException: a source that cannot reach its backend raises its
        # own error types, and those must stop the pipeline too rather than kill the
        # thread with the source left open.
        logger.error(
            f"Caught exception while running pipeline with name {pipeline.name}: {traceback.format_exc(limit=3)}"
        )
        try:
            pipeline.stop()
        except Exception:
            # The caller records this as a failed pipeline. Swallowing the stop error
            # here is what lets that assignment run; re-raising skipped it.
            logger.error(
                f"Failed to stop pipeline with name {pipeline.name}: {traceback.format_exc(limit=3)}"
            )
        try:
            pipeline.close()
        except Exception:
            logger.error(
                f"Failed to close pipeline with name {pipeline.name}: {traceback.format_exc(limit=3)}"
            )
        return False
    finally:
        logger.debug(f"Thread for pipeline with name {pipeline.name} has stopped.")
    return True


# A manager of multiple Action Pipelines.
# This class manages 1 thread per pipeline registered.
class PipelineManager:
    # A catalog of all the currently executing Action Pipelines.
    pipeline_registry: Dict[str, PipelineSpec] = {}

    def __init__(self) -> None:
        self._lock = RLock()
        self._shutting_down = False

    # Start a new Action Pipeline.
    def start_pipeline(self, name: str, pipeline: Pipeline) -> None:
        logger.debug(f"Attempting to start pipeline with name {name}...")
        with self._lock:
            if self._shutting_down:
                raise Exception(
                    f"Pipeline manager is shutting down; not starting pipeline {name}."
                )
            if name in self.pipeline_registry:
                raise Exception(f"Pipeline with name {name} is already running.")

            def run_and_record() -> None:
                try:
                    spec.failed = not run_pipeline(pipeline)
                except Exception:
                    spec.failed = True
                    logger.exception(
                        f"Pipeline with name {pipeline.name} failed without recording it"
                    )

            thread = Thread(target=run_and_record)
            spec = PipelineSpec(name, pipeline, thread)
            # Register and start under the same lock as stop_all. Otherwise a shutdown
            # can delete an unstarted spec (ident is None, so join is skipped) and
            # thread.start() still runs the worker.
            self.pipeline_registry[name] = spec
            try:
                thread.start()
            except Exception:
                del self.pipeline_registry[name]
                raise
            logger.debug(f"Started pipeline with name {name}.")

    def has_running_pipelines(self) -> bool:
        return any(spec.thread.is_alive() for spec in self.pipeline_registry.values())

    def has_failed_pipelines(self) -> bool:
        return any(spec.failed for spec in self.pipeline_registry.values())

    # Stop a running Action Pipeline.
    def stop_pipeline(self, name: str) -> None:
        logger.debug(f"Attempting to stop pipeline with name {name}...")
        if name in self.pipeline_registry:
            # Signal first and join, then close. Closing before the worker leaves
            # events() drops failed-event logging and can use a closed Kafka consumer.
            try:
                pipeline_spec = self.pipeline_registry[name]
                stop_error: Optional[BaseException] = None
                try:
                    pipeline_spec.pipeline.stop()
                    if pipeline_spec.thread.ident is not None:
                        # Skipped when the pipeline was registered but its thread had not
                        # started yet: join() on an unstarted thread raises RuntimeError.
                        pipeline_spec.thread.join()  # Wait for the pipeline thread to terminate.
                except Exception as e:
                    stop_error = e
                try:
                    pipeline_spec.pipeline.close()
                except Exception as e:
                    logger.error(
                        f"Failed to close pipeline with name {name}: {traceback.format_exc(limit=3)}"
                    )
                    if stop_error is None:
                        stop_error = e
                if stop_error is not None:
                    raise stop_error
                logger.info(f"Actions Pipeline with name '{name}' has been stopped.")
                try:
                    pipeline_spec.pipeline.stats().pretty_print_summary(
                        name
                    )  # Print the pipeline's statistics.
                except Exception:
                    # Reporting is best-effort; a pipeline that never reached mark_start()
                    # has no stats to print, and that must not look like a stop failure.
                    logger.warning(
                        f"Could not print run summary for pipeline {name}",
                        exc_info=True,
                    )
                del self.pipeline_registry[name]
            except Exception as e:
                # Failed to stop a pipeline - this is a critical issue, we should avoid starting another action of the same type
                # until this pipeline is confirmed killed.
                logger.error(
                    f"Caught exception while attempting to stop pipeline with name {name}: {traceback.format_exc(limit=3)}"
                )
                raise Exception(
                    f"Caught exception while attempting to stop pipeline with name {name}."
                ) from e
        else:
            raise Exception(f"No pipeline with name {name} found.")

    # Stop all running pipelines.
    def stop_all(self) -> None:
        logger.debug("Attempting to stop all running pipelines...")
        with self._lock:
            self._shutting_down = True
            # Stop each running pipeline. Copied so start_pipeline cannot insert
            # a name this loop will miss, and cannot start once the flag is set.
            names = list(self.pipeline_registry.keys()).copy()
        for name in names:
            self.stop_pipeline(name)
        logger.debug("Successfully stop all running pipelines.")
