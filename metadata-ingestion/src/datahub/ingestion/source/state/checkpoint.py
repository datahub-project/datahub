import base64
import bz2
import contextlib
import functools
import json
import logging
import pickle
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Generic, Optional, Type, TypeVar

import pydantic

from datahub.configuration.common import ConfigModel
from datahub.metadata.schema_classes import (
    DatahubIngestionCheckpointClass,
    IngestionCheckpointStateClass,
)

logger: logging.Logger = logging.getLogger(__name__)

DEFAULT_MAX_STATE_SIZE = 2**22  # 4MB


class CheckpointStateBase(ConfigModel):
    """
    Base class for ingestion checkpoint state.
    NOTE: We use the pydantic based ConfigModel as base here so that
    we can leverage built-in functionality for including/excluding fields in the
    serialization along with potential validation for specific sources.
    """

    version: str = pydantic.Field(default="1.0")
    serde: str = pydantic.Field(default="base85-bz2-json")

    def to_bytes(
        self,
        compressor: Callable[[bytes], bytes] = functools.partial(
            bz2.compress, compresslevel=9
        ),
        max_allowed_state_size: int = DEFAULT_MAX_STATE_SIZE,
    ) -> bytes:
        """
        NOTE: Binary compression cannot be turned on yet as the current MCPs encode the GeneralizedAspect
        payload using Json encoding which does not support bytes type data. For V1, we go with the utf-8 encoding.
        This also means that double serialization also is not possible to encode version and serde separate from the
        binary state payload. Binary content-type needs to be supported for encoding the GenericAspect to do this.
        """

        if self.serde == "utf-8":
            encoded_bytes = CheckpointStateBase._to_bytes_utf8(self)
        elif self.serde == "base85":
            # The original base85 implementation used pickle, which would cause
            # issues with deserialization if we ever changed the state class definition.
            raise ValueError(
                "Cannot write base85 encoded bytes. Use base85-bz2-json instead."
            )
        elif self.serde == "base85-bz2-json":
            encoded_bytes = CheckpointStateBase._to_bytes_base85_json(self, compressor)
        else:
            raise ValueError(f"Unknown serde: {self.serde}")

        if len(encoded_bytes) > max_allowed_state_size:
            raise ValueError(
                f"The state size has exceeded the max_allowed_state_size of {max_allowed_state_size}"
            )

        return encoded_bytes

    @staticmethod
    def _to_bytes_utf8(model: ConfigModel) -> bytes:
        return model.json(exclude={"version", "serde"}).encode("utf-8")

    @staticmethod
    def _to_bytes_base85_json(
        model: ConfigModel, compressor: Callable[[bytes], bytes]
    ) -> bytes:
        return base64.b85encode(compressor(CheckpointStateBase._to_bytes_utf8(model)))

    def prepare_for_commit(self) -> None:
        """
        Perform any pre-commit steps, such as deduplication, custom-compression across data etc.
        """
        pass


StateType = TypeVar("StateType", bound=CheckpointStateBase)


@dataclass
class Checkpoint(Generic[StateType]):
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
    state: StateType

    @classmethod
    def create_from_checkpoint_aspect(
        cls,
        job_name: str,
        checkpoint_aspect: Optional[DatahubIngestionCheckpointClass],
        config_class: Type[ConfigModel],
        state_class: Type[StateType],
    ) -> Optional["Checkpoint"]:
        if checkpoint_aspect is None:
            return None
        try:
            # Construct the config
            config_as_dict = json.loads(checkpoint_aspect.config)
            config_obj = config_class.parse_obj(config_as_dict)
        except Exception as e:
            # Failure to load config is probably okay...config structure has changed.
            logger.warning(
                "Failed to construct checkpoint's config from checkpoint aspect. %s", e
            )
        else:
            try:
                if checkpoint_aspect.state.serde == "utf-8":
                    state_obj = Checkpoint._from_utf8_bytes(
                        checkpoint_aspect, state_class
                    )
                elif checkpoint_aspect.state.serde == "base85":
                    state_obj = Checkpoint._from_base85_bytes(
                        checkpoint_aspect,
                        functools.partial(bz2.decompress),
                        state_class,
                    )
                elif checkpoint_aspect.state.serde == "base85-bz2-json":
                    state_obj = Checkpoint._from_base85_json_bytes(
                        checkpoint_aspect,
                        functools.partial(bz2.decompress),
                        state_class,
                    )
                else:
                    raise ValueError(f"Unknown serde: {checkpoint_aspect.state.serde}")
            except Exception as e:
                logger.error(
                    "Failed to construct checkpoint class from checkpoint aspect.", e
                )
                raise e
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

    @staticmethod
    def _from_utf8_bytes(
        checkpoint_aspect: DatahubIngestionCheckpointClass,
        state_class: Type[StateType],
    ) -> StateType:
        state_as_dict = (
            json.loads(checkpoint_aspect.state.payload.decode("utf-8"))
            if checkpoint_aspect.state.payload is not None
            else {}
        )
        state_as_dict["version"] = checkpoint_aspect.state.formatVersion
        state_as_dict["serde"] = checkpoint_aspect.state.serde
        return state_class.parse_obj(state_as_dict)

    @staticmethod
    def _from_base85_bytes(
        checkpoint_aspect: DatahubIngestionCheckpointClass,
        decompressor: Callable[[bytes], bytes],
        state_class: Type[StateType],
    ) -> StateType:
        state: StateType = pickle.loads(
            decompressor(base64.b85decode(checkpoint_aspect.state.payload))  # type: ignore
        )

        with contextlib.suppress(Exception):
            # When loading from pickle, the pydantic validators don't run.
            # By re-serializing and re-parsing, we ensure that the state is valid.
            # However, we also suppress any exceptions to make sure this doesn't blow up.
            state = state_class.parse_obj(state.dict())

        # Because the base85 method is deprecated in favor of base85-bz2-json,
        # we will automatically switch the serde.
        state.serde = "base85-bz2-json"

        return state

    @staticmethod
    def _from_base85_json_bytes(
        checkpoint_aspect: DatahubIngestionCheckpointClass,
        decompressor: Callable[[bytes], bytes],
        state_class: Type[StateType],
    ) -> StateType:
        state_uncompressed = decompressor(
            base64.b85decode(checkpoint_aspect.state.payload)
            if checkpoint_aspect.state.payload is not None
            else b"{}"
        )
        state_as_dict = json.loads(state_uncompressed.decode("utf-8"))
        state_as_dict["version"] = checkpoint_aspect.state.formatVersion
        state_as_dict["serde"] = checkpoint_aspect.state.serde
        return state_class.parse_obj(state_as_dict)

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

    def prepare_for_commit(self) -> None:
        self.state.prepare_for_commit()
