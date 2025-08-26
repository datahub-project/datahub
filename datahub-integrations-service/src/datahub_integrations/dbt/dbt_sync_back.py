from __future__ import annotations

import dataclasses
import logging
import os
import pathlib
import sys
import textwrap
from abc import abstractmethod
from typing import Optional, Tuple

import datahub.metadata.schema_classes as models
import pydantic
from datahub.configuration.common import ConfigModel
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.urns import CorpGroupUrn, DatasetUrn, GlossaryTermUrn, TagUrn, Urn
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.event.event_registry import EntityChangeEvent
from datahub_actions.pipeline.pipeline_context import PipelineContext
from loguru import logger
from pydantic import BaseModel
from pygitops.exceptions import PyGitOpsStagedItemsError
from ruamel.yaml.comments import CommentedMap, CommentedSeq

from datahub_integrations.actions.oss.stats_util import (
    ActionStageReport,
    EventProcessingStats,
    ReportingAction,
)
from datahub_integrations.app import graph
from datahub_integrations.dbt.dbt_utils import (
    AdvancedDbtProject,
    DbtFileLocator,
    DbtIncorrectProjectError,
    DbtProject,
    LocatorError,
    YmlFileCreationMode,
    locate_datahub_meta,
    locate_tags,
)
from datahub_integrations.dbt.git_utils import GitHubRepoConfig, GitHubRepoWrapper

_TEMP_DIR = pathlib.Path("/tmp/dbt-sync-action")
_TEMP_DIR.mkdir(exist_ok=True, parents=True)

_DEFAULT_ACRYL_BRANCH_NAME = "acryl-dbt-sync"
DEFAULT_YML_FILE_CREATION_MODE = YmlFileCreationMode.DIRECTORY_SCHEMA_YML


class DbtOperationExtractConfig(ConfigModel):
    # This must match whatever ingestion is run with.
    dbt_tag_prefix: str = "dbt:"


class DbtSyncBackConfig(GitHubRepoConfig, DbtOperationExtractConfig):
    subdir: str = "."

    # dbt_platform_instance: Optional[str] = None

    # When enabled, we'll create a venv and use `dbt ls` to locate nodes.
    # This generally shouldn't be required.
    require_runtime_dbt_resolver: bool = pydantic.Field(
        False, json_schema_extra={"hidden_from_docs": True}
    )

    open_draft_prs: bool = False

    yml_file_creation_mode: YmlFileCreationMode = (
        YmlFileCreationMode.DIRECTORY_SCHEMA_YML
    )

    acryl_branch_name: str = _DEFAULT_ACRYL_BRANCH_NAME


class DbtSyncBackReport(ActionStageReport):
    total_ignored_changes: int = 0
    total_ignored_incorrect_project: int = 0
    total_already_synced: int = 0
    total_unexpected_no_changes: int = 0
    total_unsupported_docs_blocks: int = 0
    total_synced_back: int = 0


class DbtSyncBackAction(ReportingAction):
    def __init__(self, config: DbtSyncBackConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.config = config
        self.report = DbtSyncBackReport()

        self.repo = GitHubRepoWrapper(config, base_temp_dir=_TEMP_DIR)

        self.proj: DbtProject
        if config.require_runtime_dbt_resolver:
            self.proj = AdvancedDbtProject(
                self.repo.repo_dir / config.subdir, _TEMP_DIR
            )
        else:
            self.proj = DbtProject(self.repo.repo_dir / config.subdir)

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
        if not self.report.event_processing_stats:
            self.report.event_processing_stats = EventProcessingStats()
        self.report.event_processing_stats.start(event)

        success = True
        try:
            self.act_internal(event)
        except Exception as e:
            success = False
            logger.exception(f"dbt sync action failed: {e}")
        finally:
            self.report.event_processing_stats.end(event, success=success)

    def act_internal(self, event: EventEnvelope) -> None:
        # TODO extract event type
        # TODO extract dbt unique id

        operation_tuple = extract_dbt_operation(
            envelope=event, config=self.config, graph=graph
        )
        if not operation_tuple:
            self.report.total_ignored_changes += 1
            return

        target, operation = operation_tuple
        self.report.increment_assets_processed(target.dataset_urn)

        # We might want to tweak when .enrich() is called, since the graph is available during extraction.
        operation.enrich(graph)

        logger.info(
            f"Applying operation: {operation.oneliner_fragment()} to {target.pretty_name}"
        )

        with self.repo.long_running_branch(self.config.acryl_branch_name):
            operation_oneliner = (
                f"{operation.oneliner_fragment()} to {target.pretty_name}"
            )

            try:
                operation.apply(
                    target=target,
                    context=ApplyContext(
                        graph=graph,
                        proj=self.proj,
                        yml_file_creation_mode=self.config.yml_file_creation_mode,
                    ),
                )
                self.report.increment_assets_impacted(target.dataset_urn)
            except AlreadySyncedError as e:
                logger.info(f"Nothing to do for '{operation_oneliner}': {e}")
                self.report.total_already_synced += 1
                return
            except UnsupportedDbtDocsBlockError as e:
                logger.info(e)
                self.report.total_unsupported_docs_blocks += 1
                return
            except DbtIncorrectProjectError as e:
                # This is fine, so we can just log it and move on.
                logger.info(e)
                self.report.total_ignored_incorrect_project += 1
                return

            operation_oneliner = (
                f"{operation.oneliner_fragment()} to {target.pretty_name}"
            )
            try:
                self.repo.stage_commit_and_push(
                    branch_name=self.config.acryl_branch_name,
                    commit_message=f"Automated commit: {operation_oneliner}",
                )
            except PyGitOpsStagedItemsError:
                logger.warning(
                    f"Operation applier for {operation_oneliner} did not make any changes"
                )
                self.report.total_unexpected_no_changes += 1
                return

            self.repo.upsert_github_pr(
                branch=self.config.acryl_branch_name,
                title="chore: Sync dbt with Acryl",
                body_for_create=f"{self.pr_boilerplate()}\n\n- {operation_oneliner}\n",
                body_for_append=f"- {operation_oneliner}\n",
                draft=self.config.open_draft_prs,
            )
            logger.info(f"Synced back: {operation_oneliner}")
            self.report.total_synced_back += 1

    def close(self) -> None:
        return super().close()


class SyncError(Exception):
    pass


class AlreadySyncedError(SyncError):
    pass


class OperationTarget(BaseModel):
    dataset_urn: str

    @property
    def pretty_name(self) -> str:
        return DatasetUrn.from_string(self.dataset_urn).name


@dataclasses.dataclass
class ApplyContext:
    graph: DataHubGraph
    proj: DbtProject

    yml_file_creation_mode: YmlFileCreationMode

    def resolve_dataset(self, dataset_urn: str) -> Tuple[str, str]:
        # returns dbt_unique_id, original_file_path

        # TODO support platform instances
        dataset_props = self.graph.get_aspect(
            dataset_urn, models.DatasetPropertiesClass
        )
        if not dataset_props:
            raise LocatorError(f"Unable to find dataset {dataset_urn}")

        # Get the dbt unique ID.
        dbt_unique_id = dataset_props.customProperties.get("dbt_unique_id")
        if not dbt_unique_id:
            raise LocatorError(
                f"Dataset {dataset_urn} does not have a dbt_unique_id. "
                "Try ingesting with a more recent dbt connector."
            )

        # Get the original file path.
        original_file_path: str | None
        if isinstance(self.proj, AdvancedDbtProject):
            original_file_path = self.proj.get_original_file_path(dbt_unique_id)
        else:
            original_file_path = dataset_props.customProperties.get("dbt_file_path")
        if not original_file_path:
            raise LocatorError(
                f"Could not find dbt file path for {dataset_urn}. "
                "Try ingesting with a more recent dbt connector."
            )

        logger.debug(f"Found dbt unique ID {dbt_unique_id} for dataset {dataset_urn}")

        return dbt_unique_id, original_file_path

    def make_locator(self) -> DbtFileLocator:
        return DbtFileLocator(
            self.proj, yml_file_creation_mode=self.yml_file_creation_mode
        )


class DbtOperation(BaseModel):
    def apply(
        self,
        target: OperationTarget,
        context: ApplyContext,
    ) -> None:
        dbt_unique_id, original_file_path = context.resolve_dataset(target.dataset_urn)

        with context.make_locator().get_dbt_yml_config(
            dbt_unique_id=dbt_unique_id, original_file_path=original_file_path
        ) as node:
            self._apply_inner(dbt_unique_id, node)

    @abstractmethod
    def _apply_inner(
        self,
        dbt_unique_id: str,
        doc: CommentedMap,
    ) -> None:
        pass

    def enrich(self, graph: DataHubGraph) -> None:
        # Hook to fetch additional information from the graph.
        pass

    @abstractmethod
    def oneliner_fragment(self) -> str:
        # e.g. "add tag my_new_tag"
        # TODO Add oneliner fragment markdown mode
        pass


def extract_dbt_operation(
    envelope: EventEnvelope,
    *,
    config: DbtOperationExtractConfig,
    graph: DataHubGraph,
) -> Optional[Tuple[OperationTarget, DbtOperation]]:
    if envelope.event_type != "EntityChangeEvent_v1":
        return None

    event = envelope.event
    assert isinstance(event, EntityChangeEvent)

    urn_str = event.entityUrn
    urn = Urn.from_string(event.entityUrn)
    if not isinstance(urn, DatasetUrn):
        # logger.debug(f"Skipping change event for non-dataset urn {urn}")
        return None

    # If the urn isn't for dbt, we still should check its siblings.
    # Once propagation copies these changes around, we can remove this.
    if urn.get_data_platform_urn().platform_name != "dbt":
        siblings = graph.get_aspect(urn_str, models.SiblingsClass)
        if siblings:
            if len(siblings.siblings) > 1:
                # TODO: Eventually we should compare with the project name to figure out
                # which urn is relevant.
                logger.debug(f"Skipping change event for {urn} with multiple siblings")
                return None
            elif len(siblings.siblings) == 1:
                sibling = siblings.siblings[0]
                sibling_urn = DatasetUrn.from_string(sibling)
                if sibling_urn.get_data_platform_urn().platform_name == "dbt":
                    logger.debug(f"Switching to sibling {sibling_urn} for {urn}")
                    urn_str = sibling
                    urn = sibling_urn

    if urn.get_data_platform_urn().platform_name != "dbt":
        logger.debug(f"Skipping change event for non-dbt urn {urn}")
        return None

    target = OperationTarget(dataset_urn=urn_str)

    parameters = event._inner_dict.get("__parameters_json", {})

    if event.category == "TAG":
        tag = event.modifier
        tag_urn = TagUrn.from_string(tag)

        if tag_urn.name.startswith("__"):
            # This is a system tag generated by metadata tests, so we want to ignore it.
            return None

        if config.dbt_tag_prefix and tag_urn.name.startswith(config.dbt_tag_prefix):
            # Remove the dbt tag prefix.
            # This is important for avoiding an infinite loop from double-prefixed
            # tags e.g. `dbt:dbt:tag_name`.
            tag_urn = TagUrn(tag_urn.name[len(config.dbt_tag_prefix) :])
            tag = tag_urn.urn()

        if event.operation == "ADD":
            return target, AddTagOperation(tag=tag)
        elif event.operation == "REMOVE":
            return target, RemoveTagOperation(tag=tag)
        else:
            logger.debug(f"Skipping unknown tag operation {event.operation} for {urn}")

    elif event.category == "GLOSSARY_TERM":
        term = parameters["termUrn"]
        if event.operation == "ADD":
            return target, AddTermOperation(term=term)
        elif event.operation == "REMOVE":
            return target, RemoveTermOperation(term=term)
        else:
            logger.debug(
                f"Skipping unknown glossary term operation {event.operation} for {urn}"
            )

    elif event.category == "OWNER":
        owner = parameters["ownerUrn"]

        # The ownerTypeUrn field is necessary to support custom ownership types.
        # Support for it was added in https://github.com/datahub-project/datahub/pull/10999.
        # The ownerType field should always be present, and is used as a fallback.
        owner_type = parameters.get("ownerTypeUrn") or parameters["ownerType"]

        if event.operation == "ADD":
            return target, AddOwnerOperation(owner=owner, owner_type=owner_type)
        elif event.operation == "REMOVE":
            return target, RemoveOwnerOperation(owner=owner, owner_type=owner_type)
        else:
            logger.debug(
                f"Skipping unknown owner operation {event.operation} for {urn}"
            )

    elif event.category == "DOCUMENTATION":
        docs = parameters["description"]
        if not docs:
            # HACK: Because of this, we are unable to _remove_ documentation from entities.
            # However, it's necessary because the Snowflake node might get empty docs from
            # ingestion, which then would try to overwrite dbt.
            logger.debug(f"Skipping empty documentation for {urn}")
            return None

        if event.operation in {"ADD", "MODIFY"}:
            return target, SetDocsOperation(docs=docs)
        else:
            logger.debug(
                f"Skipping unknown documentation operation {event.operation} for {urn}"
            )

    elif event.category == "DOMAIN":
        domain = parameters["domainUrn"]
        if event.operation == "ADD":
            return target, SetDomainOperation(domain=domain)
        elif event.operation == "REMOVE":
            # Our model has a list of domains, but in the UI and in most other places,
            # you can only have one domain. For a similar reason to docs, it makes sense
            # to ignore domain removes, since they'll likely be followed by an add.
            logger.debug(f"Skipping domain remove operation for {urn}")
        else:
            logger.debug(
                f"Skipping unknown domain operation {event.operation} for {urn}"
            )

    else:
        logger.debug(f"Skipping unknown event category {event.category} for {urn}")
    return None


class AddTagOperation(DbtOperation):
    tag: str
    tag_pretty_name: Optional[str] = None

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
        if self.tag_pretty_name and tag != self.tag_pretty_name:
            tags.yaml_add_eol_comment(self.tag_pretty_name, key=len(tags) - 1)

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


class TermOperation(DbtOperation):
    term: str
    term_pretty_name: Optional[str] = None

    @property
    def term_urn(self) -> GlossaryTermUrn:
        return GlossaryTermUrn.from_string(self.term)

    def enrich(self, graph: DataHubGraph) -> None:
        super().enrich(graph)

        term_info = graph.get_aspect(str(self.term_urn), models.GlossaryTermInfoClass)
        if term_info and term_info.name:
            self.term_pretty_name = term_info.name


class AddTermOperation(TermOperation):
    def _apply_inner(self, dbt_unique_id: str, doc: CommentedMap) -> None:
        dh_meta = locate_datahub_meta(dbt_unique_id, doc)
        terms: CommentedSeq = dh_meta.setdefault("terms", CommentedSeq())

        if str(self.term_urn) in terms or self.term_urn.name in terms:
            raise AlreadySyncedError(f"Term {self.term} already exists")

        terms.append(str(self.term_urn))
        if self.term_pretty_name and self.term_urn.name != self.term_pretty_name:
            terms.yaml_add_eol_comment(self.term_pretty_name, key=len(terms) - 1)

    def oneliner_fragment(self) -> str:
        return f"Add term {self.term_pretty_name or self.term_urn.name}"


class RemoveTermOperation(TermOperation):
    @property
    def term_urn(self) -> GlossaryTermUrn:
        return GlossaryTermUrn.from_string(self.term)

    def _apply_inner(self, dbt_unique_id: str, doc: CommentedMap) -> None:
        dh_meta = locate_datahub_meta(dbt_unique_id, doc)
        terms: CommentedSeq = dh_meta.setdefault("terms", CommentedSeq())

        if str(self.term_urn) not in terms:
            raise AlreadySyncedError(f"Term {self.term} doesn't exist")

        terms.remove(str(self.term_urn))

    def oneliner_fragment(self) -> str:
        return f"Remove term {self.term_pretty_name or self.term_urn.name}"


class AddOwnerOperation(DbtOperation):
    owner: str
    owner_type: str

    owner_pretty_name: Optional[str] = None

    def enrich(self, graph: DataHubGraph) -> None:
        super().enrich(graph)

        owner_urn = Urn.from_string(self.owner)
        if isinstance(owner_urn, CorpGroupUrn):
            group_info = graph.get_aspect(str(owner_urn), models.CorpGroupInfoClass)
            if group_info and group_info.displayName:
                self.owner_pretty_name = group_info.displayName

    def _apply_inner(self, dbt_unique_id: str, doc: CommentedMap) -> None:
        dh_meta = locate_datahub_meta(dbt_unique_id, doc)
        owners: CommentedSeq = dh_meta.setdefault("owners", CommentedSeq())

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
        if self.owner_pretty_name:
            owners.yaml_add_eol_comment(self.owner_pretty_name, key=len(owners) - 1)

    def oneliner_fragment(self) -> str:
        return f"Add owner {self.owner_pretty_name or self.owner} (type: {self.owner_type})"


class RemoveOwnerOperation(DbtOperation):
    owner: str
    owner_type: str

    def _apply_inner(self, dbt_unique_id: str, doc: CommentedMap) -> None:
        dh_meta = locate_datahub_meta(dbt_unique_id, doc)
        owners: CommentedSeq = dh_meta.setdefault("owners", CommentedSeq())

        for owner in owners:
            if owner["owner"] == self.owner:
                owners.remove(owner)
                return

        raise SyncError(f"Owner {self.owner} not found")

    def oneliner_fragment(self) -> str:
        return f"Remove owner {self.owner} (type: {self.owner_type})"


class UnsupportedDbtDocsBlockError(SyncError):
    pass


def truncate_docs(docs: str, maxlen: int = 100) -> str:
    docs = docs.lstrip()

    trunc_point = maxlen
    if 0 <= (newline_idx := docs.find("\n")) < trunc_point:
        trunc_point = newline_idx

    if trunc_point >= len(docs):
        return docs
    return docs[:trunc_point] + "..."


class SetDocsOperation(DbtOperation):
    docs: str

    def _apply_inner(self, dbt_unique_id: str, doc: CommentedMap) -> None:
        existing_docs = doc.get("description") or ""
        if "{{" in existing_docs:
            # See https://docs.getdbt.com/reference/resource-properties/description
            # and https://docs.getdbt.com/reference/dbt-jinja-functions/doc
            raise UnsupportedDbtDocsBlockError(
                f"Documentation references docs block: {existing_docs}, will not sync anything"
            )
        elif existing_docs == self.docs:
            raise AlreadySyncedError(
                f"Documentation already set to {truncate_docs(self.docs)}"
            )

        doc["description"] = self.docs

    def oneliner_fragment(self) -> str:
        return f'Set documentation to "{truncate_docs(self.docs)}"'


class SetDomainOperation(DbtOperation):
    domain: str
    domain_pretty_name: Optional[str] = None

    def enrich(self, graph: DataHubGraph) -> None:
        super().enrich(graph)

        domain_info = graph.get_aspect(self.domain, models.DomainPropertiesClass)
        if domain_info and domain_info.name:
            self.domain_pretty_name = domain_info.name

    def _apply_inner(self, dbt_unique_id: str, doc: CommentedMap) -> None:
        dh_meta = locate_datahub_meta(dbt_unique_id, doc)

        existing_domain = dh_meta.get("domain")
        if existing_domain == self.domain:
            raise AlreadySyncedError(f"Domain already set to {self.domain}")

        dh_meta["domain"] = self.domain
        if self.domain_pretty_name:
            # Add a comment to the domain field.
            dh_meta.yaml_add_eol_comment(self.domain_pretty_name, key="domain")

    def oneliner_fragment(self) -> str:
        return f"Set domain to {self.domain_pretty_name or self.domain}"


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    # logging.getLogger("datahub").setLevel(logging.DEBUG)

    DBT_SUBDIR = os.environ.get("DBT_PROJECT_SUBDIR", ".")
    GITHUB_TOKEN = os.environ["GITHUB_TOKEN"]

    config = DbtSyncBackConfig(
        github_repo_org="acryldata",
        github_repo_name="harshal-sample-dbt-tests",
        auth=GITHUB_TOKEN,
        subdir=DBT_SUBDIR,
    )

    dbt_unique_id = sys.argv[2]

    repo = GitHubRepoWrapper(config, base_temp_dir=_TEMP_DIR)
    proj: DbtProject
    if os.environ.get("DBT_USE_ADVANCED_METADATA", "false") == "true":
        proj = AdvancedDbtProject(repo.repo_dir / config.subdir, _TEMP_DIR)
        file_location = proj.get_original_file_path(dbt_unique_id)
    else:
        proj = DbtProject(repo.repo_dir / config.subdir)
        file_location = sys.argv[3]

    command = sys.argv[1]

    if command == "no-op":
        pass
    elif command == "test-dbt-locator":
        locator = DbtFileLocator(
            proj, yml_file_creation_mode=DEFAULT_YML_FILE_CREATION_MODE
        )
        with locator.get_dbt_yml_config(
            dbt_unique_id=dbt_unique_id, original_file_path=file_location
        ) as node:
            print(node)
    # elif command == "test-dbt-add-tag":
    #     op = AddTagOperation(
    #         dataset_urn="-dummy-",
    #         tag="urn:li:tag:my_new_tag",
    #     )
    #     op.apply_with_info(proj, dbt_unique_id, file_location)
    else:
        raise ValueError(f"Unknown mode {command}")
