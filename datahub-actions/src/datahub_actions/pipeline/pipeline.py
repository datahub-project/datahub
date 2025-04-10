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
import os
from typing import List, Optional

from datahub_actions.action.action import Action
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.pipeline.pipeline_config import FailureMode, PipelineConfig
from datahub_actions.pipeline.pipeline_stats import PipelineStats
from datahub_actions.pipeline.pipeline_util import (
    create_action,
    create_action_context,
    create_event_source,
    create_filter_transformer,
    create_transformer,
    normalize_directory_name,
)
from datahub_actions.source.event_source import EventSource
from datahub_actions.transform.transformer import Transformer

logger = logging.getLogger(__name__)


# Defaults for the location where failed events will be written.
DEFAULT_RETRY_COUNT = 0  # Do not retry unless instructed.
DEFAULT_FAILED_EVENTS_DIR = "/tmp/logs/datahub/actions"
DEFAULT_FAILED_EVENTS_FILE_NAME = "failed_events.log"  # Not currently configurable.
DEFAULT_FAILURE_MODE = FailureMode.CONTINUE


class PipelineException(Exception):
    """
    An exception thrown when a Pipeline encounters and unrecoverable situation.
    Mainly a placeholder for now.
    """

    pass


class Pipeline:
    """
    A Pipeline is responsible for coordinating execution of a single DataHub Action.

    This responsibility includes:

        - sourcing events from an Event Source
        - executing a configurable chain of Transformers
        - invoking an Action with the final Event
        - acknowledging the processing of an Event with the Event Source

    Additionally, a Pipeline supports the following notable capabilities:

        - Configurable retries of event processing in cases of component failure
        - Configurable dead letter queue
        - Capturing basic statistics about each Pipeline component
        - At-will start and stop of an individual pipeline

    """

    name: str
    source: EventSource
    transforms: List[Transformer] = []
    action: Action

    # Whether the Pipeline has been requested to shut down
    _shutdown: bool = False

    # Pipeline statistics
    _stats: PipelineStats = PipelineStats()

    # Options
    _retry_count: int = DEFAULT_RETRY_COUNT  # Number of times a single event should be retried in case of processing error.
    _failure_mode: FailureMode = DEFAULT_FAILURE_MODE
    _failed_events_dir: str = DEFAULT_FAILED_EVENTS_DIR  # The top-level path where failed events will be logged.

    def __init__(
        self,
        name: str,
        source: EventSource,
        transforms: List[Transformer],
        action: Action,
        retry_count: Optional[int],
        failure_mode: Optional[FailureMode],
        failed_events_dir: Optional[str],
    ) -> None:
        self.name = name
        self.source = source
        self.transforms = transforms
        self.action = action

        if retry_count is not None:
            self._retry_count = retry_count
        if failure_mode is not None:
            self._failure_mode = failure_mode
        if failed_events_dir is not None:
            self._failed_events_dir = failed_events_dir
        self._init_failed_events_dir()

    @classmethod
    def create(cls, config_dict: dict) -> "Pipeline":
        # Bind config
        config = PipelineConfig.parse_obj(config_dict)

        if not config.enabled:
            raise Exception(
                "Pipeline is disabled, but create method was called unexpectedly."
            )

        # Create Context
        ctx = create_action_context(config.name, config.datahub)

        # Create Event Source
        event_source = create_event_source(config.source, ctx)

        # Create Transforms
        transforms = []
        if config.filter is not None:
            transforms.append(create_filter_transformer(config.filter, ctx))

        if config.transform is not None:
            for transform_config in config.transform:
                transforms.append(create_transformer(transform_config, ctx))

        # Create Action
        action = create_action(config.action, ctx)

        # Finally, create Pipeline.
        return cls(
            config.name,
            event_source,
            transforms,
            action,
            config.options.retry_count if config.options else None,
            config.options.failure_mode if config.options else None,
            config.options.failed_events_dir if config.options else None,
        )

    async def start(self) -> None:
        """
        Start the action pipeline asynchronously. This method is non-blocking.
        """
        self.run()

    def run(self) -> None:
        """
        Run the action pipeline synchronously. This method is blocking.
        Raises an instance of PipelineException if an unrecoverable pipeline failure occurs.
        """
        self._stats.mark_start()

        # First, source the events.
        enveloped_events = self.source.events()
        for enveloped_event in enveloped_events:
            # Then, process the event.
            retval = self._process_event(enveloped_event)

            # For legacy users w/o selective ack support, convert
            # None to True, i.e. always commit.
            if retval is None:
                retval = True

            # Finally, ack the event.
            self._ack_event(enveloped_event, retval)

    def stop(self) -> None:
        """
        Stops a running action pipeline.
        """
        logger.debug(f"Preparing to stop Actions Pipeline with name {self.name}")
        self._shutdown = True
        self._failed_events_fd.close()
        self.source.close()
        self.action.close()

    def stats(self) -> PipelineStats:
        """
        Returns basic statistics about the Pipeline run.
        """
        return self._stats

    def _process_event(self, enveloped_event: EventEnvelope) -> Optional[bool]:
        # Attempt to process the incoming event, with retry.
        curr_attempt = 1
        max_attempts = self._retry_count + 1
        retval = None
        while curr_attempt <= max_attempts:
            try:
                # First, transform the event.
                transformed_event = self._execute_transformers(enveloped_event)

                # Then, invoke the action if the event is non-null.
                if transformed_event is not None:
                    retval = self._execute_action(transformed_event)

                # Short circuit - processing has succeeded.
                return retval
            except Exception:
                logger.exception(
                    f"Caught exception while attempting to process event. Attempt {curr_attempt}/{max_attempts} event type: {enveloped_event.event_type}, pipeline name: {self.name}"
                )
                curr_attempt = curr_attempt + 1

        logger.error(
            f"Failed to process event after {self._retry_count} retries. event type: {enveloped_event.event_type}, pipeline name: {self.name}. Handling failure..."
        )

        # Increment failed event count.
        self._stats.increment_failed_event_count()

        # Finally, handle the failure
        self._handle_failure(enveloped_event)

        return retval

    def _execute_transformers(
        self, enveloped_event: EventEnvelope
    ) -> Optional[EventEnvelope]:
        curr_event = enveloped_event
        # Iterate through all transformers, sequentially apply them to the result of the previous.
        for transformer in self.transforms:
            # Increment stats
            self._stats.increment_transformer_processed_count(transformer)

            # Transform the event
            transformed_event = self._execute_transformer(curr_event, transformer)

            # Process result
            if transformed_event is None:
                # If the transformer has filtered the event, short circuit.
                self._stats.increment_transformer_filtered_count(transformer)
                return None
            # Otherwise, set the result to the transformed event.
            curr_event = transformed_event

        # Return the final transformed event.
        return curr_event

    def _execute_transformer(
        self, enveloped_event: EventEnvelope, transformer: Transformer
    ) -> Optional[EventEnvelope]:
        try:
            return transformer.transform(enveloped_event)
        except Exception as e:
            self._stats.increment_transformer_exception_count(transformer)
            raise PipelineException(
                f"Caught exception while executing Transformer with name {type(transformer).__name__}"
            ) from e

    def _execute_action(self, enveloped_event: EventEnvelope) -> Optional[bool]:
        try:
            retval = self.action.act(enveloped_event)
            self._stats.increment_action_success_count()
            return retval
        except Exception as e:
            self._stats.increment_action_exception_count()
            raise PipelineException(
                f"Caught exception while executing Action with type {type(self.action).__name__}"
            ) from e

    def _ack_event(self, enveloped_event: EventEnvelope, processed: bool) -> None:
        try:
            self.source.ack(enveloped_event, processed)
            self._stats.increment_success_count()
        except Exception:
            self._stats.increment_failed_ack_count()
            logger.exception(
                f"Caught exception while attempting to ack successfully processed event. event type: {enveloped_event.event_type}, pipeline name: {self.name}",
            )
            logger.debug(f"Failed to ack event: {enveloped_event}")

    def _handle_failure(self, enveloped_event: EventEnvelope) -> None:
        # First, always save the failed event to a file. Useful for investigation.
        self._append_failed_event_to_file(enveloped_event)
        if self._failure_mode == FailureMode.THROW:
            raise PipelineException("Failed to process event after maximum retries.")
        elif self._failure_mode == FailureMode.CONTINUE:
            # Simply return, nothing left to do.
            pass

    def _append_failed_event_to_file(self, enveloped_event: EventEnvelope) -> None:
        # First, convert the event to JSON.
        try:
            json = enveloped_event.as_json()
            # Then append to failed events file.
            self._failed_events_fd.write(json + "\n")
            self._failed_events_fd.flush()
        except Exception as e:
            # This is a serious issue, as if we do not handle it can mean losing an event altogether.
            # Raise an exception to ensure this issue is reported to the operator.
            raise PipelineException(
                f"Failed to log failed event to file! {enveloped_event}"
            ) from e

    def _init_failed_events_dir(self) -> None:
        # create a directory for failed events from this actions pipeine.
        failed_events_dir = os.path.join(
            self._failed_events_dir, normalize_directory_name(self.name)
        )
        try:
            os.makedirs(failed_events_dir, exist_ok=True)

            failed_events_file_name = os.path.join(
                failed_events_dir, DEFAULT_FAILED_EVENTS_FILE_NAME
            )
            self._failed_events_fd = open(failed_events_file_name, "a")
        except Exception as e:
            logger.debug(e)
            raise PipelineException(
                f"Caught exception while attempting to create failed events log file at path {failed_events_dir}. Please check your file system permissions."
            ) from e
