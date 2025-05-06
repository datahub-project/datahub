import json
import logging
import re
from typing import Dict, List, Optional, Set, Union, cast

from pydantic import BaseModel, Field

from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    MetadataChangeLogClass,
    MetadataChangeProposalClass,
)
from datahub_actions.action.action import Action
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.event.event_registry import METADATA_CHANGE_LOG_EVENT_V1_TYPE
from datahub_actions.pipeline.pipeline_context import PipelineContext

logger = logging.getLogger(__name__)


class MetadataChangeEmitterConfig(BaseModel):
    gms_server: Optional[str] = None
    gms_auth_token: Optional[str] = None
    aspects_to_exclude: Optional[List] = None
    aspects_to_include: Optional[List] = None
    entity_type_to_exclude: List[str] = Field(default_factory=list)
    extra_headers: Optional[Dict[str, str]] = None
    urn_regex: Optional[str] = None


class MetadataChangeSyncAction(Action):
    rest_emitter: DatahubRestEmitter
    aspects_exclude_set: Set
    # By default, we exclude the following aspects since different datahub instances have their own encryption keys for
    # encrypting tokens and secrets, we can't decrypt them even if these values sync to another datahub instance
    # also, we don't sync execution request aspects because the ingestion recipe might contain datahub secret
    # that another datahub instance could not decrypt
    DEFAULT_ASPECTS_EXCLUDE_SET = {
        "dataHubAccessTokenInfo",
        "dataHubAccessTokenKey",
        "dataHubSecretKey",
        "dataHubSecretValue",
        "dataHubExecutionRequestInput",
        "dataHubExecutionRequestKey",
        "dataHubExecutionRequestResult",
    }

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Action":
        action_config = MetadataChangeEmitterConfig.parse_obj(config_dict or {})
        return cls(action_config, ctx)

    def __init__(self, config: MetadataChangeEmitterConfig, ctx: PipelineContext):
        self.config = config
        assert isinstance(self.config.gms_server, str)
        self.rest_emitter = DatahubRestEmitter(
            gms_server=self.config.gms_server,
            token=self.config.gms_auth_token,
            extra_headers=self.config.extra_headers,
        )
        self.aspects_exclude_set = (
            self.DEFAULT_ASPECTS_EXCLUDE_SET.union(set(self.config.aspects_to_exclude))
            if self.config.aspects_to_exclude
            else self.DEFAULT_ASPECTS_EXCLUDE_SET
        )
        self.aspects_include_set = self.config.aspects_to_include

        extra_headers_keys = (
            list(self.config.extra_headers.keys())
            if self.config.extra_headers
            else None
        )
        logger.info(
            f"MetadataChangeSyncAction configured to emit mcp to gms server {self.config.gms_server} with extra headers {extra_headers_keys} and aspects to exclude {self.aspects_exclude_set} and aspects to include {self.aspects_include_set}"
        )
        self.urn_regex = self.config.urn_regex

    def act(self, event: EventEnvelope) -> None:
        """
        This method listens for MetadataChangeLog events, casts it to MetadataChangeProposal,
        and emits it to another datahub instance
        """
        # MetadataChangeProposal only supports UPSERT type for now
        if event.event_type is METADATA_CHANGE_LOG_EVENT_V1_TYPE:
            orig_event = cast(MetadataChangeLogClass, event.event)
            logger.debug(f"received orig_event {orig_event}")
            regexUrn = self.urn_regex
            if regexUrn is None:
                urn_match = re.match(".*", "default match")
            elif orig_event.entityUrn is not None:
                urn_match = re.match(regexUrn, orig_event.entityUrn)
            else:
                logger.warning(f"event missing entityUrn: {orig_event}")
                urn_match = None
            aspect_name = orig_event.get("aspectName")
            logger.info(f"urn_match {urn_match} for entityUrn {orig_event.entityUrn}")
            if (
                (
                    (
                        self.aspects_include_set is not None
                        and aspect_name in self.aspects_include_set
                    )
                    or (
                        self.aspects_include_set is None
                        and aspect_name not in self.aspects_exclude_set
                    )
                )
                and (
                    orig_event.get("entityType")
                    not in self.config.entity_type_to_exclude
                    if self.config.entity_type_to_exclude
                    else True
                )
                and urn_match is not None
            ):
                mcp = self.buildMcp(orig_event)

                if mcp is not None:
                    logger.debug(f"built mcp {mcp}")
                    self.emit(mcp)
            else:
                logger.debug(
                    f"skip emitting mcp for aspect {orig_event.get('aspectName')} or entityUrn {orig_event.entityUrn} or entity type {orig_event.get('entityType')} on exclude list"
                )

    def buildMcp(
        self, orig_event: MetadataChangeLogClass
    ) -> Union[MetadataChangeProposalClass, None]:
        try:
            changeType = orig_event.get("changeType")
            if changeType == ChangeTypeClass.RESTATE or changeType == "RESTATE":
                changeType = ChangeTypeClass.UPSERT
            mcp = MetadataChangeProposalClass(
                entityType=orig_event.get("entityType"),
                changeType=changeType,
                entityUrn=orig_event.get("entityUrn"),
                entityKeyAspect=orig_event.get("entityKeyAspect"),
                aspectName=orig_event.get("aspectName"),
                aspect=orig_event.get("aspect"),
            )
            return mcp
        except Exception as ex:
            logger.error(
                f"error when building mcp from mcl {json.dumps(orig_event.to_obj(), indent=4)}"
            )
            logger.error(f"exception: {ex}")
            return None

    def emit(self, mcp: MetadataChangeProposalClass) -> None:
        # Create an emitter to DataHub over REST
        try:
            # For unit test purpose, moving test_connection from initialization to here
            # if rest_emitter.server_config is empty, that means test_connection() has not been called before
            if not self.rest_emitter.server_config:
                self.rest_emitter.test_connection()
            logger.info(
                f"emitting the mcp: entityType {mcp.entityType}, changeType {mcp.changeType}, urn {mcp.entityUrn}, aspect name {mcp.aspectName}"
            )
            self.rest_emitter.emit_mcp(mcp)
            logger.info("successfully emit the mcp")
        except Exception as ex:
            logger.error(
                f"error when emitting mcp, {json.dumps(mcp.to_obj(), indent=4)}"
            )
            logger.error(f"exception: {ex}")

    def close(self) -> None:
        pass
