import bz2
import functools
import json
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Dict, Optional, Type

import pydantic

from datahub.configuration.common import ConfigModel
from datahub.metadata.schema_classes import (
    DatahubIngestionCheckpointClass,
    IngestionCheckpointStateClass,
)

logger: logging.Logger = logging.getLogger(__name__)


class CheckpointStateBase(ConfigModel):
    """
    Base class for ingestion checkpoint state.
    NOTE: We use the pydantic based ConfigModel as base here so that
    we can leverage built-in functionality for including/excluding fields in the
    serialization along with potential validation for specific sources.
    """

    version: str = pydantic.Field(default="1.0")
    serde: str = pydantic.Field(default="utf-8")

    def to_bytes(
        self,
        compressor: Callable[[bytes], bytes] = functools.partial(
            bz2.compress, compresslevel=9
        ),
        max_allowed_state_size: int = 2 ** 22,  # 4MB
    ) -> bytes:
        """
        NOTE: Binary compression cannot be turned on yet as the current MCPs encode the GeneralizedAspect
        payload using Json encoding which does not support bytes type data. For V1, we go with the utf-8 encoding.
        This also means that double serialization also is not possible to encode version and serde separate from the
        binary state payload. Binary content-type needs to be supported for encoding the GenericAspect to do this.
        """

        json_str_self = self.json(exclude={"version", "serde"})
        encoded_bytes = json_str_self.encode("utf-8")
        if len(encoded_bytes) > max_allowed_state_size:
            raise ValueError(
                f"The state size has exceeded the max_allowed_state_size of {max_allowed_state_size}"
            )

        return encoded_bytes

    @staticmethod
    def from_bytes_to_dict(
        data_bytes: bytes, decompressor: Callable[[bytes], bytes] = bz2.decompress
    ) -> Dict[str, Any]:
        """Helper method for sub-classes to use."""
        # uncompressed_data: bytes = decompressor(data_bytes)
        # json_str = uncompressed_data.decode('utf-8')
        json_str = data_bytes.decode("utf-8")
        return json.loads(json_str)


@dataclass
class Checkpoint:
    """
    Ingestion Run Checkpoint class. This is a more convenient abstraction for use in the python ingestion code,
    providing a strongly typed state object vs the opaque blob in the PDL, and the config persisted as the first-class
    ConfigModel object.
    """

    job_name: str
    pipeline_name: str
    platform_instance_id: str
    run_id: str
    config: ConfigModel
    state: CheckpointStateBase

    @classmethod
    def create_from_checkpoint_aspect(
        cls,
        job_name: str,
        checkpoint_aspect: Optional[DatahubIngestionCheckpointClass],
        config_class: Type[ConfigModel],
        state_class: Type[CheckpointStateBase],
    ) -> Optional["Checkpoint"]:
        if checkpoint_aspect is None:
            return None
        try:
            # Construct the config
            config_as_dict = json.loads(checkpoint_aspect.config)
            config_obj = config_class.parse_obj(config_as_dict)

            # Construct the state
            state_as_dict = (
                CheckpointStateBase.from_bytes_to_dict(checkpoint_aspect.state.payload)
                if checkpoint_aspect.state.payload is not None
                else {}
            )
            state_as_dict["version"] = checkpoint_aspect.state.formatVersion
            state_as_dict["serde"] = checkpoint_aspect.state.serde
            state_obj = state_class.parse_obj(state_as_dict)
        except Exception as e:
            logger.error(
                "Failed to construct checkpoint class from checkpoint aspect.", e
            )
        else:
            # Construct the deserialized Checkpoint object from the raw aspect.
            checkpoint = cls(
                job_name=job_name,
                pipeline_name=checkpoint_aspect.pipelineName,
                platform_instance_id=checkpoint_aspect.platformInstanceId,
                run_id=checkpoint_aspect.runId,
                config=config_obj,
                state=state_obj,
            )
            logger.info(
                f"Successfully constructed last checkpoint state for job {job_name}"
            )
            return checkpoint
        return None

    def to_checkpoint_aspect(
        self, max_allowed_state_size: int
    ) -> Optional[DatahubIngestionCheckpointClass]:
        try:
            checkpoint_state = IngestionCheckpointStateClass(
                formatVersion=self.state.version,
                serde=self.state.serde,
                payload=self.state.to_bytes(
                    max_allowed_state_size=max_allowed_state_size
                ),
            )
            checkpoint_aspect = DatahubIngestionCheckpointClass(
                timestampMillis=int(datetime.utcnow().timestamp() * 1000),
                pipelineName=self.pipeline_name,
                platformInstanceId=self.platform_instance_id,
                runId=self.run_id,
                config=self.config.json(),
                state=checkpoint_state,
            )
            return checkpoint_aspect
        except Exception as e:
            logger.error(
                "Failed to construct the checkpoint aspect from checkpoint object", e
            )

        return None
