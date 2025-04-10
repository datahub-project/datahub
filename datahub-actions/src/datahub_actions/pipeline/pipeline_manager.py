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
from threading import Thread
from typing import Dict

from datahub_actions.pipeline.pipeline import Pipeline, PipelineException

logger = logging.getLogger(__name__)


@dataclass
class PipelineSpec:
    # The pipeline name
    name: str

    # The pipeline
    pipeline: Pipeline

    # The thread which is executing the pipeline.
    thread: Thread


# Run a pipeline in blocking fashion
# TODO: Exit process on failure of single pipeline.
def run_pipeline(pipeline: Pipeline) -> None:
    try:
        pipeline.run()
    except PipelineException:
        logger.error(
            f"Caught exception while running pipeline with name {pipeline.name}: {traceback.format_exc(limit=3)}"
        )
        pipeline.stop()
    logger.debug(f"Thread for pipeline with name {pipeline.name} has stopped.")


# A manager of multiple Action Pipelines.
# This class manages 1 thread per pipeline registered.
class PipelineManager:
    # A catalog of all the currently executing Action Pipelines.
    pipeline_registry: Dict[str, PipelineSpec] = {}

    def __init__(self) -> None:
        pass

    # Start a new Action Pipeline.
    def start_pipeline(self, name: str, pipeline: Pipeline) -> None:
        logger.debug(f"Attempting to start pipeline with name {name}...")
        if name not in self.pipeline_registry:
            thread = Thread(target=run_pipeline, args=([pipeline]))
            thread.start()
            spec = PipelineSpec(name, pipeline, thread)
            self.pipeline_registry[name] = spec
            logger.debug(f"Started pipeline with name {name}.")
        else:
            raise Exception(f"Pipeline with name {name} is already running.")

    # Stop a running Action Pipeline.
    def stop_pipeline(self, name: str) -> None:
        logger.debug(f"Attempting to stop pipeline with name {name}...")
        if name in self.pipeline_registry:
            # First, stop the pipeline.
            try:
                pipeline_spec = self.pipeline_registry[name]
                pipeline_spec.pipeline.stop()
                pipeline_spec.thread.join()  # Wait for the pipeline thread to terminate.
                logger.info(f"Actions Pipeline with name '{name}' has been stopped.")
                pipeline_spec.pipeline.stats().pretty_print_summary(
                    name
                )  # Print the pipeline's statistics.
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
        # Stop each running pipeline.
        names = list(self.pipeline_registry.keys()).copy()
        for name in names:
            self.stop_pipeline(name)
        logger.debug("Successfully stop all running pipelines.")
