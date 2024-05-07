import logging
import os
import pathlib
import sys
import textwrap
from abc import abstractmethod
from typing import Optional

import datahub.metadata.schema_classes as models
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.urns import DatasetUrn, GlossaryTermUrn, TagUrn, Urn
from datahub_actions.action.action import Action
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.event.event_registry import EntityChangeEvent
from datahub_actions.pipeline.pipeline_context import PipelineContext
from loguru import logger
from pydantic import BaseModel
from pygitops.exceptions import PyGitOpsStagedItemsError
from ruamel.yaml.comments import CommentedMap

from datahub_integrations.app import graph
from datahub_integrations.dbt.dbt_utils import (
    DbtFileLocator,
    DbtProject,
    locate_datahub_meta,
    locate_tags,
)
from datahub_integrations.dbt.git_utils import GitHubRepoConfig, GitHubRepoWrapper

_TEMP_DIR = pathlib.Path("/tmp/dbt-sync-action")
_TEMP_DIR.mkdir(exist_ok=True, parents=True)

_ACRYL_BRANCH_NAME = "acryl-dbt-sync"


class DbtSyncBackConfig(GitHubRepoConfig):
    subdir: str = "."

    dbt_platform_instance: Optional[str] = None


class DbtSyncBackAction(Action):
    def __init__(self, config: DbtSyncBackConfig, ctx: PipelineContext):
        self.config = config
        self.ctx = ctx

        self.repo = GitHubRepoWrapper(config, base_temp_dir=_TEMP_DIR)
        self.proj = DbtProject(self.repo.repo_dir / config.subdir, _TEMP_DIR)

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "DbtSyncBackAction":
        config = DbtSyncBackConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def pr_boilerplate(self) -> str:
        instance_url = graph.frontend_base_url
        return textwrap.dedent(
            f"""
            Keep dbt in sync with changes made in your [Acryl data catalog]({instance_url}).

            Changes:
            """
        ).strip()

    def act(self, event: EventEnvelope) -> None:
        # TODO extract event type
        # TODO extract dbt unique id

        operation = extract_dbt_operation(event)
        if not operation:
            return

        logger.info(
            f"Applying operation: {operation.oneliner_fragment()} to {operation.pretty_name}"
        )

        with self.repo.long_running_branch(_ACRYL_BRANCH_NAME):
            operation_oneliner = (
                f"{operation.oneliner_fragment()} to {operation.pretty_name}"
            )

            try:
                operation.apply(graph, self.proj)
            except AlreadySyncedError as e:
                logger.info(f"Nothing to do for '{operation_oneliner}': {e}")
                return

            operation_oneliner = (
                f"{operation.oneliner_fragment()} to {operation.pretty_name}"
            )
            try:
                self.repo.stage_commit_and_push(
                    branch_name=_ACRYL_BRANCH_NAME,
                    commit_message=f"Automated commit: {operation_oneliner}",
                )
            except PyGitOpsStagedItemsError:
                logger.warning(
                    f"Operation applier for {operation_oneliner} did not make any changes"
                )
                return

            self.repo.upsert_github_pr(
                branch=_ACRYL_BRANCH_NAME,
                title="chore: Sync dbt with Acryl",
                body_for_create=f"{self.pr_boilerplate()}\n\n- {operation_oneliner}\n",
                body_for_append=f"- {operation_oneliner}\n",
            )
            logger.info(f"Synced back: {operation_oneliner}")

    def close(self) -> None:
        return super().close()


class SyncError(Exception):
    pass


class AlreadySyncedError(SyncError):
    pass


class DbtOperation(BaseModel):
    dataset_urn: str

    explicit_pretty_name: str | None = None

    @property
    def pretty_name(self) -> str:
        if self.explicit_pretty_name:
            return self.explicit_pretty_name
        return DatasetUrn.from_string(self.dataset_urn).name

    def apply(self, graph: DataHubGraph, proj: DbtProject) -> None:
        # TODO support platform instances
        # We need to check if the changed urn is for the project that we're currently working on.

        dataset_props = graph.get_aspect(
            self.dataset_urn, models.DatasetPropertiesClass
        )
        if not dataset_props:
            raise ValueError(f"Unable to find dataset {self.dataset_urn}")
        self.explicit_pretty_name = dataset_props.name

        # Get the dbt unique ID.
        dbt_unique_id = dataset_props.customProperties.get("dbt_unique_id")
        if not dbt_unique_id:
            raise ValueError(
                f"Dataset {self.dataset_urn} does not have a dbt_unique_id. "
                "Try ingesting with a more recent dbt connector."
            )
        logger.debug(
            f"Found dbt unique ID {dbt_unique_id} for dataset {self.dataset_urn}"
        )

        return self.apply_with_dbt_id(dbt_unique_id, proj)

    def apply_with_dbt_id(self, dbt_unique_id: str, proj: DbtProject) -> None:
        locator = DbtFileLocator(proj)
        with locator.get_dbt_yml_config_for_unique_id(dbt_unique_id) as node:
            self._apply_inner(dbt_unique_id, node)

    @abstractmethod
    def _apply_inner(
        self,
        dbt_unique_id: str,
        doc: CommentedMap,
    ) -> None:
        pass

    @abstractmethod
    def oneliner_fragment(self) -> str:
        # e.g. "add tag my_new_tag"
        # TODO Add oneliner fragment markdown mode
        pass


def extract_dbt_operation(envelope: EventEnvelope) -> Optional[DbtOperation]:
    if envelope.event_type != "EntityChangeEvent_v1":
        return None

    event = envelope.event
    assert isinstance(event, EntityChangeEvent)

    urn_str = event.entityUrn
    urn = Urn.from_string(event.entityUrn)
    if (
        not isinstance(urn, DatasetUrn)
        or urn.get_data_platform_urn().platform_name != "dbt"
    ):
        logger.debug(f"Skipping change event for non-dbt urn {urn}")
        return None

    parameters = event._inner_dict.get("__parameters_json", {})

    if event.category == "TAG":
        tag = event.modifier
        if event.operation == "ADD":
            return AddTagOperation(dataset_urn=urn_str, tag=tag)
        elif event.operation == "REMOVE":
            return RemoveTagOperation(dataset_urn=urn_str, tag=tag)
        else:
            logger.debug(f"Skipping unknown tag operation {event.operation} for {urn}")

    elif event.category == "GLOSSARY_TERM":
        term = parameters["termUrn"]
        if event.operation == "ADD":
            return AddTermOperation(dataset_urn=urn_str, term=term)
        elif event.operation == "REMOVE":
            return RemoveTermOperation(dataset_urn=urn_str, term=term)
        else:
            logger.debug(
                f"Skipping unknown glossary term operation {event.operation} for {urn}"
            )

    elif event.category == "OWNER":
        owner = parameters["ownerUrn"]
        owner_type = parameters["ownerType"]

        if event.operation == "ADD":
            return AddOwnerOperation(
                dataset_urn=urn_str, owner=owner, owner_type=owner_type
            )
        elif event.operation == "REMOVE":
            return RemoveOwnerOperation(
                dataset_urn=urn_str, owner=owner, owner_type=owner_type
            )
        else:
            logger.debug(
                f"Skipping unknown owner operation {event.operation} for {urn}"
            )

    elif event.category == "DOCUMENTATION":
        docs = parameters["description"]
        if event.operation == "ADD":
            return SetDocumentation(dataset_urn=urn_str, docs=docs)
        else:
            logger.debug(
                f"Skipping unknown documentation operation {event.operation} for {urn}"
            )

    else:
        logger.debug(f"Skipping unknown event category {event.category} for {urn}")
    return None


class AddTagOperation(DbtOperation):
    tag: str

    @property
    def tag_urn(self) -> TagUrn:
        return TagUrn.from_string(self.tag)

    def _apply_inner(self, dbt_unique_id: str, doc: CommentedMap) -> None:
        # TODO: We don't support the hierarchical nature of dbt tags yet.
        tag = self.tag_urn.name

        tags = locate_tags(dbt_unique_id, doc)

        if tag in tags:
            raise AlreadySyncedError(f"Tag {tag} already exists")
        tags.append(tag)

    def oneliner_fragment(self) -> str:
        return f"Add tag {self.tag_urn.name}"


class RemoveTagOperation(DbtOperation):
    tag: str

    @property
    def tag_urn(self) -> TagUrn:
        return TagUrn.from_string(self.tag)

    def _apply_inner(self, dbt_unique_id: str, doc: CommentedMap) -> None:
        tag = self.tag_urn.name

        tags = locate_tags(dbt_unique_id, doc)

        if tag not in tags:
            raise AlreadySyncedError(f"Tag {tag} isn't applied")
        tags.remove(tag)

    def oneliner_fragment(self) -> str:
        return f"Remove tag {self.tag_urn.name}"


class AddTermOperation(DbtOperation):
    term: str

    @property
    def term_urn(self) -> GlossaryTermUrn:
        return GlossaryTermUrn.from_string(self.term)

    def _apply_inner(self, dbt_unique_id: str, doc: CommentedMap) -> None:
        dh_meta = locate_datahub_meta(dbt_unique_id, doc)
        terms = dh_meta.setdefault("terms", [])

        if self.term_urn.name in terms:
            raise AlreadySyncedError(f"Term {self.term} already exists")

        terms.append(self.term_urn.name)

    def oneliner_fragment(self) -> str:
        return f"Add term {self.term_urn.name}"


class RemoveTermOperation(DbtOperation):
    term: str

    @property
    def term_urn(self) -> GlossaryTermUrn:
        return GlossaryTermUrn.from_string(self.term)

    def _apply_inner(self, dbt_unique_id: str, doc: CommentedMap) -> None:
        dh_meta = locate_datahub_meta(dbt_unique_id, doc)
        terms = dh_meta.setdefault("terms", [])

        if self.term_urn.name not in terms:
            raise AlreadySyncedError(f"Term {self.term} doesn't exist")

        terms.remove(self.term_urn.name)

    def oneliner_fragment(self) -> str:
        return f"Remove term {self.term_urn.name}"


class AddOwnerOperation(DbtOperation):
    owner: str
    owner_type: str

    def _apply_inner(self, dbt_unique_id: str, doc: CommentedMap) -> None:
        dh_meta = locate_datahub_meta(dbt_unique_id, doc)

        owners = dh_meta.setdefault("owners", [])
        for owner in owners:
            # TODO: Change this to reuse some of the logic from dbt's meta mappings code.
            if owner["owner"] == self.owner:
                current_owner_type = owner.get("owner_type", "DATAOWNER")
                if current_owner_type == self.owner_type:
                    raise AlreadySyncedError(
                        f"Owner {self.owner} of type {self.owner_type} already exists"
                    )
                else:
                    logger.debug(
                        f"Changing owner type for {self.owner} from {current_owner_type} to {self.owner_type}"
                    )
                    owner["owner_type"] = self.owner_type
                    return

        owners.append({"owner": self.owner, "owner_type": self.owner_type})

    def oneliner_fragment(self) -> str:
        return f"Add owner {self.owner} (type: {self.owner_type})"


class RemoveOwnerOperation(DbtOperation):
    owner: str
    owner_type: str

    def _apply_inner(self, dbt_unique_id: str, doc: CommentedMap) -> None:
        dh_meta = locate_datahub_meta(dbt_unique_id, doc)
        owners = dh_meta.setdefault("owners", [])

        for owner in owners:
            if owner["owner"] == self.owner:
                owners.remove(owner)
                return

        raise SyncError(f"Owner {self.owner} not found")

    def oneliner_fragment(self) -> str:
        return f"Remove owner {self.owner} (type: {self.owner_type})"


class SetDocumentation(DbtOperation):
    docs: str

    def _apply_inner(self, dbt_unique_id: str, doc: CommentedMap) -> None:
        doc["description"] = self.docs

    def oneliner_fragment(self) -> str:
        return f'Set documentation to "{self.docs}"'


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    # logging.getLogger("datahub").setLevel(logging.DEBUG)

    DBT_SUBDIR = os.environ.get("DBT_PROJECT_FOLDER", ".")
    GITHUB_TOKEN = os.environ["GITHUB_TOKEN"]

    config = DbtSyncBackConfig(
        github_repo_org="acryldata",
        github_repo_name="harshal-sample-dbt-tests",
        token=GITHUB_TOKEN,
        subdir=DBT_SUBDIR,
    )

    repo = GitHubRepoWrapper(config, base_temp_dir=_TEMP_DIR)
    proj = DbtProject(repo.repo_dir / config.subdir, _TEMP_DIR)

    command = sys.argv[1]

    if command == "no-op":
        pass
    elif command == "test-dbt-locator":
        locator = DbtFileLocator(proj)
        with locator.get_dbt_yml_config_for_unique_id(sys.argv[2]) as node:
            print(node)
    elif command == "test-dbt-add-tag":
        op = AddTagOperation(
            dataset_urn="-dummy-",
            tag="urn:li:tag:my_new_tag",
        )
        dbt_unique_id = sys.argv[2]
        op.apply_with_dbt_id(dbt_unique_id, proj)
    else:
        raise ValueError(f"Unknown mode {command}")
