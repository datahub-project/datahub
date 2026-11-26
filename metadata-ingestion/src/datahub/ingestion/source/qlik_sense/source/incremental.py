import copy
import hashlib
import json
import logging
from typing import Dict, List, Optional

from datahub._version import __version__
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.qlik_sense.app_state import AppCheckpoint
from datahub.ingestion.source.qlik_sense.config import Constant
from datahub.ingestion.source.qlik_sense.constants import ENTITY_CHECK_BATCH_SIZE
from datahub.ingestion.source.qlik_sense.source.connections import DataConnectionMixin
from datahub.metadata.com.linkedin.pegasus2avro.common import Status
from datahub.metadata.schema_classes import ContainerPropertiesClass
from datahub.utilities.urns.urn import guess_entity_type

logger = logging.getLogger(__name__)


class IncrementalMixin(DataConnectionMixin):
    def _skips_unchanged_apps(self) -> bool:
        return (
            self.config.skips_unchanged_apps()
            and self.app_state.is_checkpointing_enabled()
        )

    def _init_fingerprint_salt(self) -> None:
        # The recipe and data connections shape every app's output, and a new
        # connector version may emit differently, so any change re-reads all apps.
        self._fingerprint_salt = json.dumps(
            {
                "version": __version__,
                "config": self.config.model_dump(
                    mode="json",
                    exclude={
                        "stateful_ingestion": True,
                        "max_workers": True,
                        "max_partial_reads_kept": True,
                        "on_prem": {"skip_unchanged_apps"},
                    },
                ),
                "connections": {
                    name: info.model_dump(mode="json")
                    for name, info in sorted(self._get_data_connections().items())
                },
            },
            sort_keys=True,
        )

    def _salted_fingerprint(self, fingerprint: str) -> str:
        assert self._fingerprint_salt is not None
        return hashlib.sha256(
            f"{self._fingerprint_salt}|{fingerprint}".encode("utf-8")
        ).hexdigest()

    def _keep_in_stale_state(self, urn: str) -> None:
        if self.stale_entity_removal_handler is not None:
            self.stale_entity_removal_handler.add_entity_to_state(
                guess_entity_type(urn), urn
            )

    def _usable_checkpoint(
        self, app_id: str, failures: List[str]
    ) -> Optional[AppCheckpoint]:
        """The app's last read, when it can stand in for this run's read: it
        is recorded by the run the stale entity removal state comes from, and
        it holds the entities to keep. A failed lookup goes to `failures`."""
        try:
            previous = self.app_state.previous(app_id)
        except Exception as e:
            logger.debug(f"Unable to load the last checkpoint of app {app_id}: {e}")
            failures.append(f"{app_id}: unable to load the last checkpoint: {e}")
            return None
        return previous if previous is not None and previous.urns else None

    def _is_app_unchanged(self, app_id: str, fingerprint: str) -> bool:
        previous = self._usable_checkpoint(app_id, self._change_check_failures)
        if previous is None or previous.fingerprint != self._salted_fingerprint(
            fingerprint
        ):
            return False
        if not self._on_cloud and (
            previous.qvds is None or previous.binary_tables is None
        ):
            # Recorded before QVD or BINARY lineage, which are only known by
            # reading it again.
            return False
        # Checkpoints are committed even when the sink failed, so only trust one
        # whose metadata actually reached DataHub and was not deleted since.
        graph = self.ctx.graph
        if graph is None:
            return False
        container_urn = self._gen_app_key(app_id).as_urn()
        try:
            properties = graph.get_aspect(container_urn, ContainerPropertiesClass)
            if (
                properties is None
                or (properties.customProperties or {}).get(Constant.FINGERPRINT)
                != previous.fingerprint
            ):
                return False
            return self._entities_present(graph, previous.urns)
        except Exception as e:
            logger.debug(f"Unable to read app container {container_urn}: {e}")
            self._change_check_failures.append(
                f"{app_id}: unable to read the app back from DataHub: {e}"
            )
            return False

    @staticmethod
    def _entities_present(graph: DataHubGraph, urns: List[str]) -> bool:
        """Whether every entity the app's last read produced is in DataHub and
        not soft-deleted, which also catches entities the sink failed to write
        and hard-deleted ones. Entities stale removal deleted after they left
        Qlik are not in the checkpoint."""
        by_type: Dict[str, List[str]] = {}
        for urn in urns:
            by_type.setdefault(guess_entity_type(urn), []).append(urn)
        for entity_type, typed_urns in by_type.items():
            for start in range(0, len(typed_urns), ENTITY_CHECK_BATCH_SIZE):
                batch = typed_urns[start : start + ENTITY_CHECK_BATCH_SIZE]
                entities = graph.get_entities(
                    entity_type, batch, aspects=[Status.ASPECT_NAME]
                )
                for urn in batch:
                    status = entities.get(urn, {}).get(Status.ASPECT_NAME)
                    if status is None or getattr(status[0], "removed", True):
                        return False
        return True

    def _gen_fingerprint_workunit(
        self, container_workunits: List[MetadataWorkUnit], fingerprint: str
    ) -> MetadataWorkUnit:
        properties = next(
            aspect
            for aspect in (
                wu.get_aspect_of_type(ContainerPropertiesClass)
                for wu in container_workunits
            )
            if aspect
        )
        stamped = copy.deepcopy(properties)
        stamped.customProperties = {
            **(stamped.customProperties or {}),
            Constant.FINGERPRINT: fingerprint,
        }
        urn = container_workunits[0].get_urn()
        return MetadataWorkUnit(
            id=f"{urn}-fingerprint",
            mcp=MetadataChangeProposalWrapper(entityUrn=urn, aspect=stamped),
        )
