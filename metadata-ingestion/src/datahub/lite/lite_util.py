import logging
import time
from typing import Dict, Iterable, List, Optional, Union

from datahub.configuration.common import ConfigModel, DynamicTypedConfig
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext, RecordEnvelope
from datahub.ingestion.api.sink import NoopWriteCallback, Sink
from datahub.ingestion.sink.sink_registry import sink_registry
from datahub.lite.lite_local import (
    Browseable,
    DataHubLiteLocal,
    Searchable,
    SearchFlavor,
)
from datahub.lite.lite_registry import lite_registry
from datahub.metadata.schema_classes import MetadataChangeEventClass, _Aspect

logger = logging.getLogger(__name__)


class LiteLocalConfig(ConfigModel):
    type: str
    config: dict = {}
    forward_to: Optional[DynamicTypedConfig] = None


class DataHubLiteWrapper(DataHubLiteLocal):
    def __init__(self, lite_impl: DataHubLiteLocal, forward_to: Sink):
        self.lite = lite_impl
        self.forward_to = forward_to

    def write(
        self,
        record: Union[
            MetadataChangeEventClass,
            MetadataChangeProposalWrapper,
        ],
    ) -> None:
        self.lite.write(record)
        record_envelope = RecordEnvelope(record=record, metadata={})
        self.forward_to.write_record_async(
            record_envelope=record_envelope, write_callback=NoopWriteCallback()
        )

    def close(self) -> None:
        self.lite.close()
        self.forward_to.close()

    def location(self) -> str:
        return self.lite.location()

    def destroy(self) -> None:
        return self.lite.destroy()

    def list_ids(self) -> Iterable[str]:
        yield from self.lite.list_ids()

    def get(
        self,
        id: str,
        aspects: Optional[List[str]],
        typed: bool = False,
        as_of: Optional[int] = None,
        details: Optional[bool] = False,
    ) -> Optional[Dict[str, Union[str, dict, _Aspect]]]:
        return self.get(id, aspects, typed, as_of, details)

    def search(
        self,
        query: str,
        flavor: SearchFlavor,
        aspects: List[str] = [],
        snippet: bool = True,
    ) -> Iterable[Searchable]:
        yield from self.lite.search(query, flavor, aspects, snippet)

    def ls(self, path: str) -> List[Browseable]:
        return self.lite.ls(path)

    def get_all_entities(
        self, typed: bool = False
    ) -> Iterable[Dict[str, Union[dict, _Aspect]]]:
        yield from self.lite.get_all_entities(typed)

    def get_all_aspects(self) -> Iterable[MetadataChangeProposalWrapper]:
        yield from self.lite.get_all_aspects()

    def reindex(self) -> None:
        self.lite.reindex()


def get_datahub_lite(config_dict: dict, read_only: bool = False) -> "DataHubLiteLocal":
    lite_local_config = LiteLocalConfig.parse_obj(config_dict)

    lite_type = lite_local_config.type
    try:
        lite_class = lite_registry.get(lite_type)
    except KeyError:
        raise Exception(
            f"Failed to find a registered lite implementation for {lite_type}. Valid values are {[k for k in lite_registry.mapping.keys()]}"
        )

    lite_specific_config = lite_class.get_config_class().parse_obj(
        lite_local_config.config
    )
    lite = lite_class(lite_specific_config)
    # we only set up forwarding if forwarding config is present and read_only is set to False
    if lite_local_config.forward_to and not read_only:
        forward_sink_class = sink_registry.get(lite_local_config.forward_to.type)
        if forward_sink_class is not None:
            current_time = int(time.time() * 1000.0)
            try:
                forward_to = forward_sink_class.create(
                    lite_local_config.forward_to.config or {},
                    PipelineContext(run_id=f"lite-forward_{current_time}"),
                )
                return DataHubLiteWrapper(lite, forward_to)
            except Exception as e:
                logger.warning(
                    f"Failed to set up forwarding due to {e}, will not forward events"
                )
                logger.debug(
                    "Failed to set up forwarding, will not forward events", exc_info=e
                )
                return lite
        else:
            raise Exception(
                f"Failed to find a registered forwarding sink for type {lite_local_config.forward_to.type}. Valid values are {[k for k in sink_registry.mapping.keys()]}"
            )
    else:
        return lite
