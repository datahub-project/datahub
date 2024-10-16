import logging
import pathlib
import tempfile
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Set, Tuple

import lkml
import lkml.simple
from looker_sdk.error import SDKError

from datahub.configuration.git import GitInfo
from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import gen_containers
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import MetadataWorkUnitProcessor, SourceCapability
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import (
    BIContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.git.git_import import GitClone
from datahub.ingestion.source.looker.looker_common import (
    CORPUSER_DATAHUB,
    LookerExplore,
    LookerUtil,
    LookerViewId,
    ViewField,
    ViewFieldType,
    ViewFieldValue,
    deduplicate_fields,
    gen_project_key,
)
from datahub.ingestion.source.looker.looker_connection import (
    get_connection_def_based_on_connection_string,
)
from datahub.ingestion.source.looker.looker_lib_wrapper import LookerAPI
from datahub.ingestion.source.looker.looker_template_language import (
    load_and_preprocess_file,
)
from datahub.ingestion.source.looker.looker_view_id_cache import (
    LookerModel,
    LookerViewFileLoader,
    LookerViewIdCache,
)
from datahub.ingestion.source.looker.lookml_concept_context import (
    LookerFieldContext,
    LookerViewContext,
)
from datahub.ingestion.source.looker.lookml_config import (
    _BASE_PROJECT_NAME,
    _MODEL_FILE_EXTENSION,
    VIEW_LANGUAGE_LOOKML,
    VIEW_LANGUAGE_SQL,
    LookerConnectionDefinition,
    LookMLSourceConfig,
    LookMLSourceReport,
)
from datahub.ingestion.source.looker.lookml_refinement import LookerRefinementResolver
from datahub.ingestion.source.looker.view_upstream import (
    AbstractViewUpstream,
    create_view_upstream,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import BrowsePaths, Status
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageTypeClass,
    FineGrainedLineageDownstreamType,
    UpstreamClass,
    UpstreamLineage,
    ViewProperties,
)
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import (
    AuditStampClass,
    BrowsePathEntryClass,
    BrowsePathsV2Class,
    ContainerClass,
    DatasetPropertiesClass,
    FineGrainedLineageClass,
    FineGrainedLineageUpstreamTypeClass,
    SubTypesClass,
)
from datahub.sql_parsing.sqlglot_lineage import ColumnRef

logger = logging.getLogger(__name__)


@dataclass
class LookerView:
    id: LookerViewId
    absolute_file_path: str
    connection: LookerConnectionDefinition
    upstream_dataset_urns: List[str]
    fields: List[ViewField]
    raw_file_content: str
    view_details: Optional[ViewProperties] = None

    @classmethod
    def determine_view_file_path(
        cls, base_folder_path: str, absolute_file_path: str
    ) -> str:
        splits: List[str] = absolute_file_path.split(base_folder_path, 1)
        if len(splits) != 2:
            logger.debug(
                f"base_folder_path({base_folder_path}) and absolute_file_path({absolute_file_path}) not matching"
            )
            return ViewFieldValue.NOT_AVAILABLE.value

        file_path: str = splits[1]
        logger.debug(f"file_path={file_path}")

        return file_path.strip(
            "/"
        )  # strip / from path to make it equivalent to source_file attribute of LookerModelExplore API

    @classmethod
    def from_looker_dict(
        cls,
        project_name: str,
        model_name: str,
        view_context: LookerViewContext,
        looker_view_id_cache: LookerViewIdCache,
        reporter: LookMLSourceReport,
        max_file_snippet_length: int,
        config: LookMLSourceConfig,
        ctx: PipelineContext,
        extract_col_level_lineage: bool = False,
        populate_sql_logic_in_descriptions: bool = False,
    ) -> Optional["LookerView"]:

        view_name = view_context.name()

        logger.debug(f"Handling view {view_name} in model {model_name}")

        looker_view_id: LookerViewId = LookerViewId(
            project_name=project_name,
            model_name=model_name,
            view_name=view_name,
            file_path=view_context.view_file_name(),
        )

        view_upstream: AbstractViewUpstream = create_view_upstream(
            view_context=view_context,
            looker_view_id_cache=looker_view_id_cache,
            config=config,
            ctx=ctx,
            reporter=reporter,
        )

        field_type_vs_raw_fields = OrderedDict(
            {
                ViewFieldType.DIMENSION: view_context.dimensions(),
                ViewFieldType.DIMENSION_GROUP: view_context.dimension_groups(),
                ViewFieldType.MEASURE: view_context.measures(),
            }
        )  # in order to maintain order in golden file

        view_fields: List[ViewField] = []

        for field_type, fields in field_type_vs_raw_fields.items():
            for field in fields:
                upstream_column_ref: List[ColumnRef] = []
                if extract_col_level_lineage:
                    upstream_column_ref = view_upstream.get_upstream_column_ref(
                        field_context=LookerFieldContext(raw_field=field)
                    )

                view_fields.append(
                    ViewField.view_fields_from_dict(
                        field_dict=field,
                        upstream_column_ref=upstream_column_ref,
                        type_cls=field_type,
                        populate_sql_logic_in_descriptions=populate_sql_logic_in_descriptions,
                    )
                )

        # special case where view is defined as derived sql, however fields are not defined
        if (
            len(view_fields) == 0
            and view_context.is_sql_based_derived_view_without_fields_case()
        ):
            view_fields = view_upstream.create_fields()

        view_fields = deduplicate_fields(view_fields)

        # Prep "default" values for the view, which will be overridden by the logic below.
        view_logic = view_context.view_file.raw_file_content[:max_file_snippet_length]

        if view_context.is_sql_based_derived_case():
            view_logic = view_context.sql()
            view_details = ViewProperties(
                materialized=False,
                viewLogic=view_logic,
                viewLanguage=VIEW_LANGUAGE_SQL,
            )
        elif view_context.is_native_derived_case():
            # We want this to render the full lkml block
            # e.g. explore_source: source_name { ... }
            # As such, we use the full derived_table instead of the explore_source.
            view_logic = str(lkml.dump(view_context.derived_table()))[
                :max_file_snippet_length
            ]
            view_lang = VIEW_LANGUAGE_LOOKML

            materialized = view_context.is_materialized_derived_view()

            view_details = ViewProperties(
                materialized=materialized, viewLogic=view_logic, viewLanguage=view_lang
            )
        else:
            view_details = ViewProperties(
                materialized=False,
                viewLogic=view_logic,
                viewLanguage=VIEW_LANGUAGE_LOOKML,
            )

        return LookerView(
            id=looker_view_id,
            absolute_file_path=view_context.view_file.absolute_file_path,
            connection=view_context.view_connection,
            upstream_dataset_urns=view_upstream.get_upstream_dataset_urn(),
            fields=view_fields,
            raw_file_content=view_context.view_file.raw_file_content,
            view_details=view_details,
        )


@dataclass
class LookerRemoteDependency:
    name: str
    url: str
    ref: Optional[str]


@dataclass
class LookerManifest:
    # This must be set if the manifest has local_dependency entries.
    # See https://cloud.google.com/looker/docs/reference/param-manifest-project-name
    project_name: Optional[str]

    local_dependencies: List[str]
    remote_dependencies: List[LookerRemoteDependency]


@platform_name("Looker")
@config_class(LookMLSourceConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(
    SourceCapability.PLATFORM_INSTANCE,
    "Use the `platform_instance` and `connection_to_platform_map` fields",
)
@capability(SourceCapability.LINEAGE_COARSE, "Supported by default")
@capability(
    SourceCapability.LINEAGE_FINE,
    "Enabled by default, configured using `extract_column_level_lineage`",
)
class LookMLSource(StatefulIngestionSourceBase):
    """
    This plugin extracts the following:
    - LookML views from model files in a project
    - Name, upstream table names, metadata for dimensions, measures, and dimension groups attached as tags
    - If API integration is enabled (recommended), resolves table and view names by calling the Looker API, otherwise supports offline resolution of these names.

    :::note
    To get complete Looker metadata integration (including Looker dashboards and charts and lineage to the underlying Looker views, you must ALSO use the `looker` source module.
    :::
    """

    platform = "lookml"
    source_config: LookMLSourceConfig
    reporter: LookMLSourceReport
    looker_client: Optional[LookerAPI] = None

    # This is populated during the git clone step.
    base_projects_folder: Dict[str, pathlib.Path] = {}
    remote_projects_git_info: Dict[str, GitInfo] = {}

    def __init__(self, config: LookMLSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.source_config = config
        self.ctx = ctx
        self.reporter = LookMLSourceReport()

        # To keep track of projects (containers) which have already been ingested
        self.processed_projects: List[str] = []

        if self.source_config.api:
            self.looker_client = LookerAPI(self.source_config.api)
            self.reporter._looker_api = self.looker_client
            try:
                self.looker_client.all_connections()
            except SDKError as err:
                raise ValueError(
                    "Failed to retrieve connections from looker client. Please check to ensure that you have "
                    "manage_models permission enabled on this API key."
                ) from err

    def _load_model(self, path: str) -> LookerModel:
        logger.debug(f"Loading model from file {path}")

        parsed = load_and_preprocess_file(
            path=path,
            source_config=self.source_config,
        )

        looker_model = LookerModel.from_looker_dict(
            parsed,
            _BASE_PROJECT_NAME,
            self.source_config.project_name,
            self.base_projects_folder,
            path,
            self.source_config,
            self.reporter,
        )
        return looker_model

    def _get_upstream_lineage(
        self, looker_view: LookerView
    ) -> Optional[UpstreamLineage]:
        upstream_dataset_urns = looker_view.upstream_dataset_urns

        # Generate the upstream + fine grained lineage objects.
        upstreams = []
        observed_lineage_ts = datetime.now(tz=timezone.utc)
        for upstream_dataset_urn in upstream_dataset_urns:
            upstream = UpstreamClass(
                dataset=upstream_dataset_urn,
                type=DatasetLineageTypeClass.VIEW,
                auditStamp=AuditStampClass(
                    time=int(observed_lineage_ts.timestamp() * 1000),
                    actor=CORPUSER_DATAHUB,
                ),
            )
            upstreams.append(upstream)

        fine_grained_lineages: List[FineGrainedLineageClass] = []

        for field in looker_view.fields:
            fine_grained_lineages.append(
                FineGrainedLineageClass(
                    upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                    upstreams=[
                        make_schema_field_urn(cll_ref.table, cll_ref.column)
                        for cll_ref in field.upstream_fields
                    ],
                    downstreamType=FineGrainedLineageDownstreamType.FIELD,
                    downstreams=[
                        make_schema_field_urn(
                            looker_view.id.get_urn(self.source_config),
                            field.name,
                        )
                    ],
                )
            )

        if upstreams:
            return UpstreamLineage(
                upstreams=upstreams, fineGrainedLineages=fine_grained_lineages or None
            )
        else:
            return None

    def _get_custom_properties(self, looker_view: LookerView) -> DatasetPropertiesClass:
        assert self.source_config.base_folder  # this is always filled out
        base_folder = self.base_projects_folder.get(
            looker_view.id.project_name, self.source_config.base_folder
        )
        try:
            file_path = str(
                pathlib.Path(looker_view.absolute_file_path).relative_to(
                    base_folder.resolve()
                )
            )
        except Exception:
            file_path = None
            logger.warning(
                f"Failed to resolve relative path for file {looker_view.absolute_file_path} w.r.t. folder {self.source_config.base_folder}"
            )

        custom_properties = {
            "looker.file.path": file_path or looker_view.absolute_file_path,
            "looker.model": looker_view.id.model_name,
        }
        dataset_props = DatasetPropertiesClass(
            name=looker_view.id.view_name, customProperties=custom_properties
        )

        maybe_git_info = self.source_config.project_dependencies.get(
            looker_view.id.project_name,
            self.remote_projects_git_info.get(looker_view.id.project_name),
        )
        if isinstance(maybe_git_info, GitInfo):
            git_info: Optional[GitInfo] = maybe_git_info
        else:
            git_info = self.source_config.git_info
        if git_info is not None and file_path:
            # It should be that looker_view.id.project_name is the base project.
            github_file_url = git_info.get_url_for_file_path(file_path)
            dataset_props.externalUrl = github_file_url

        return dataset_props

    def _build_dataset_mcps(
        self, looker_view: LookerView
    ) -> List[MetadataChangeProposalWrapper]:

        view_urn = looker_view.id.get_urn(self.source_config)

        subTypeEvent = MetadataChangeProposalWrapper(
            entityUrn=view_urn,
            aspect=SubTypesClass(typeNames=[DatasetSubTypes.VIEW]),
        )
        events = [subTypeEvent]
        if looker_view.view_details is not None:
            viewEvent = MetadataChangeProposalWrapper(
                entityUrn=view_urn,
                aspect=looker_view.view_details,
            )
            events.append(viewEvent)

        project_key = gen_project_key(self.source_config, looker_view.id.project_name)

        container = ContainerClass(container=project_key.as_urn())
        events.append(
            MetadataChangeProposalWrapper(entityUrn=view_urn, aspect=container)
        )

        events.append(
            MetadataChangeProposalWrapper(
                entityUrn=view_urn,
                aspect=looker_view.id.get_browse_path_v2(self.source_config),
            )
        )

        return events

    def _build_dataset_mce(self, looker_view: LookerView) -> MetadataChangeEvent:
        """
        Creates MetadataChangeEvent for the dataset, creating upstream lineage links
        """
        logger.debug(f"looker_view = {looker_view.id}")

        dataset_snapshot = DatasetSnapshot(
            urn=looker_view.id.get_urn(self.source_config),
            aspects=[],  # we append to this list later on
        )
        browse_paths = BrowsePaths(
            paths=[looker_view.id.get_browse_path(self.source_config)]
        )

        dataset_snapshot.aspects.append(browse_paths)
        dataset_snapshot.aspects.append(Status(removed=False))
        upstream_lineage = self._get_upstream_lineage(looker_view)
        if upstream_lineage is not None:
            dataset_snapshot.aspects.append(upstream_lineage)
        schema_metadata = LookerUtil._get_schema(
            self.source_config.platform_name,
            looker_view.id.view_name,
            looker_view.fields,
            self.reporter,
        )
        if schema_metadata is not None:
            dataset_snapshot.aspects.append(schema_metadata)
        dataset_snapshot.aspects.append(self._get_custom_properties(looker_view))

        return MetadataChangeEvent(proposedSnapshot=dataset_snapshot)

    def get_project_name(self, model_name: str) -> str:
        if self.source_config.project_name is not None:
            return self.source_config.project_name

        assert (
            self.looker_client is not None
        ), "Failed to find a configured Looker API client"
        try:
            model = self.looker_client.lookml_model(model_name, fields="project_name")
            assert (
                model.project_name is not None
            ), f"Failed to find a project name for model {model_name}"
            return model.project_name
        except SDKError:
            raise ValueError(
                f"Could not locate a project name for model {model_name}. Consider configuring a static project name "
                f"in your config file"
            )

    def get_manifest_if_present(self, folder: pathlib.Path) -> Optional[LookerManifest]:
        manifest_file = folder / "manifest.lkml"
        if manifest_file.exists():

            manifest_dict = load_and_preprocess_file(
                path=manifest_file, source_config=self.source_config
            )

            manifest = LookerManifest(
                project_name=manifest_dict.get("project_name"),
                local_dependencies=[
                    x["project"] for x in manifest_dict.get("local_dependencys", [])
                ],
                remote_dependencies=[
                    LookerRemoteDependency(
                        name=x["name"], url=x["url"], ref=x.get("ref")
                    )
                    for x in manifest_dict.get("remote_dependencys", [])
                ],
            )
            return manifest
        else:
            return None

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.source_config, self.ctx
            ).workunit_processor,
        ]

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        with tempfile.TemporaryDirectory("lookml_tmp") as tmp_dir:
            # Clone the base_folder if necessary.
            if not self.source_config.base_folder:
                assert self.source_config.git_info
                # we don't have a base_folder, so we need to clone the repo and process it locally
                start_time = datetime.now()
                checkout_dir = self.source_config.git_info.clone(
                    tmp_path=tmp_dir,
                )
                self.reporter.git_clone_latency = datetime.now() - start_time
                self.source_config.base_folder = checkout_dir.resolve()

            self.base_projects_folder[
                _BASE_PROJECT_NAME
            ] = self.source_config.base_folder

            visited_projects: Set[str] = set()

            # We clone everything that we're pointed at.
            for project, p_ref in self.source_config.project_dependencies.items():
                # If we were given GitHub info, we need to clone the project.
                if isinstance(p_ref, GitInfo):
                    try:
                        p_checkout_dir = p_ref.clone(
                            tmp_path=f"{tmp_dir}/_included_/{project}",
                            # If a deploy key was provided, use it. Otherwise, fall back
                            # to the main project deploy key, if present.
                            fallback_deploy_key=(
                                self.source_config.git_info.deploy_key
                                if self.source_config.git_info
                                else None
                            ),
                        )

                        p_ref = p_checkout_dir.resolve()
                    except Exception as e:
                        logger.warning(
                            f"Failed to clone project dependency {project}. This can lead to failures in parsing lookml files later on: {e}",
                        )
                        visited_projects.add(project)
                        continue

                self.base_projects_folder[project] = p_ref

            self._recursively_check_manifests(
                tmp_dir, _BASE_PROJECT_NAME, visited_projects
            )

            yield from self.get_internal_workunits()

            if not self.report.events_produced and not self.report.failures:
                # Don't pass if we didn't produce any events.
                self.report.report_failure(
                    "No Metadata Produced",
                    "No metadata was produced. Check the logs for more details.",
                )

    def _recursively_check_manifests(
        self, tmp_dir: str, project_name: str, project_visited: Set[str]
    ) -> None:
        if project_name in project_visited:
            return
        project_visited.add(project_name)

        project_path = self.base_projects_folder.get(project_name)
        if not project_path:
            logger.warning(
                f"Could not find {project_name} in the project_dependencies config. This can lead to failures in parsing lookml files later on.",
            )
            return

        manifest = self.get_manifest_if_present(project_path)
        if not manifest:
            return

        # Special case handling if the root project has a name in the manifest file.
        if project_name == _BASE_PROJECT_NAME and manifest.project_name:
            if (
                self.source_config.project_name is not None
                and manifest.project_name != self.source_config.project_name
            ):
                logger.warning(
                    f"The project name in the manifest file '{manifest.project_name}'"
                    f"does not match the configured project name '{self.source_config.project_name}'. "
                    "This can lead to failures in LookML include resolution and lineage generation."
                )
            elif self.source_config.project_name is None:
                self.source_config.project_name = manifest.project_name

        # Clone the remote project dependencies.
        for remote_project in manifest.remote_dependencies:
            if remote_project.name in project_visited:
                continue
            if remote_project.name in self.base_projects_folder:
                # In case a remote_dependency is specified in the project_dependencies config,
                # we don't need to clone it again.
                continue

            p_cloner = GitClone(f"{tmp_dir}/_remote_/{remote_project.name}")
            try:
                # TODO: For 100% correctness, we should be consulting
                # the manifest lock file for the exact ref to use.

                p_checkout_dir = p_cloner.clone(
                    ssh_key=(
                        self.source_config.git_info.deploy_key
                        if self.source_config.git_info
                        else None
                    ),
                    repo_url=remote_project.url,
                )

                self.base_projects_folder[
                    remote_project.name
                ] = p_checkout_dir.resolve()
                repo = p_cloner.get_last_repo_cloned()
                assert repo
                remote_git_info = GitInfo(
                    url_template=remote_project.url,
                    repo="dummy/dummy",  # set to dummy values to bypass validation
                    branch=repo.active_branch.name,
                )
                remote_git_info.repo = (
                    ""  # set to empty because url already contains the full path
                )
                self.remote_projects_git_info[remote_project.name] = remote_git_info

            except Exception as e:
                logger.warning(
                    f"Failed to clone remote project {project_name}. This can lead to failures in parsing lookml files later on: {e}",
                )
                project_visited.add(project_name)
            else:
                self._recursively_check_manifests(
                    tmp_dir, remote_project.name, project_visited
                )

        for project in manifest.local_dependencies:
            self._recursively_check_manifests(tmp_dir, project, project_visited)

    def get_internal_workunits(self) -> Iterable[MetadataWorkUnit]:  # noqa: C901
        assert self.source_config.base_folder

        viewfile_loader = LookerViewFileLoader(
            self.source_config.project_name,
            self.base_projects_folder,
            self.reporter,
            self.source_config,
        )

        # Some views can be mentioned by multiple 'include' statements and can be included via different connections.

        # This map is used to keep track of which views files have already been processed
        # for a connection in order to prevent creating duplicate events.
        # Key: connection name, Value: view file paths
        processed_view_map: Dict[str, Set[str]] = {}

        # This map is used to keep track of the connection that a view is processed with.
        # Key: view unique identifier - determined by variables present in config `view_naming_pattern`
        # Value: Tuple(model file name, connection name)
        view_connection_map: Dict[str, Tuple[str, str]] = {}

        # The ** means "this directory and all subdirectories", and hence should
        # include all the files we want.
        model_files = sorted(
            self.source_config.base_folder.glob(f"**/*{_MODEL_FILE_EXTENSION}")
        )
        model_suffix_len = len(".model")

        for file_path in model_files:
            self.reporter.report_models_scanned()
            model_name = file_path.stem[:-model_suffix_len]

            if not self.source_config.model_pattern.allowed(model_name):
                self.reporter.report_models_dropped(model_name)
                continue
            try:
                logger.debug(f"Attempting to load model: {file_path}")
                model = self._load_model(str(file_path))
            except Exception as e:
                self.reporter.report_warning(
                    title="Error Loading Model File",
                    message="Unable to load Looker model from file.",
                    context=f"Model Name: {model_name}, File Path: {file_path}",
                    exc=e,
                )
                continue

            assert model.connection is not None
            connection_definition = get_connection_def_based_on_connection_string(
                connection=model.connection,
                looker_client=self.looker_client,
                source_config=self.source_config,
                reporter=self.reporter,
            )

            if connection_definition is None:
                self.reporter.report_warning(
                    title="Failed to Load Connection",
                    message="Failed to load connection. Check your API key permissions and/or connection_to_platform_map configuration.",
                    context=f"Connection: {model.connection}",
                )
                self.reporter.report_models_dropped(model_name)
                continue

            explore_reachable_views: Set[str] = set()
            looker_refinement_resolver: LookerRefinementResolver = (
                LookerRefinementResolver(
                    looker_model=model,
                    connection_definition=connection_definition,
                    looker_viewfile_loader=viewfile_loader,
                    source_config=self.source_config,
                    reporter=self.reporter,
                )
            )

            if self.source_config.emit_reachable_views_only:
                model_explores_map = {d["name"]: d for d in model.explores}
                for explore_dict in model.explores:
                    try:
                        if LookerRefinementResolver.is_refinement(explore_dict["name"]):
                            continue

                        explore_dict = (
                            looker_refinement_resolver.apply_explore_refinement(
                                explore_dict
                            )
                        )
                        explore: LookerExplore = LookerExplore.from_dict(
                            model_name,
                            explore_dict,
                            model.resolved_includes,
                            viewfile_loader,
                            self.reporter,
                            model_explores_map,
                        )
                        if explore.upstream_views:
                            for view_name in explore.upstream_views:
                                explore_reachable_views.add(view_name.include)
                    except Exception as e:
                        self.reporter.report_warning(
                            title="Failed to process explores",
                            message="Failed to process explore dictionary.",
                            context=f"Explore Details: {explore_dict}",
                            exc=e,
                        )
                        logger.debug("Failed to process explore", exc_info=e)

            processed_view_files = processed_view_map.setdefault(
                model.connection, set()
            )

            project_name = self.get_project_name(model_name)

            looker_view_id_cache: LookerViewIdCache = LookerViewIdCache(
                project_name=project_name,
                model_name=model_name,
                looker_model=model,
                looker_viewfile_loader=viewfile_loader,
                reporter=self.reporter,
            )

            logger.debug(f"Model: {model_name}; Includes: {model.resolved_includes}")

            for include in model.resolved_includes:
                logger.debug(f"Considering {include} for model {model_name}")
                if include.include in processed_view_files:
                    logger.debug(f"view '{include}' already processed, skipping it")
                    continue
                logger.debug(f"Attempting to load view file: {include}")
                looker_viewfile = viewfile_loader.load_viewfile(
                    path=include.include,
                    project_name=include.project,
                    connection=connection_definition,
                    reporter=self.reporter,
                )

                if looker_viewfile is not None:
                    for raw_view in looker_viewfile.views:
                        raw_view_name = raw_view["name"]
                        if LookerRefinementResolver.is_refinement(raw_view_name):
                            continue

                        if (
                            self.source_config.emit_reachable_views_only
                            and raw_view_name not in explore_reachable_views
                        ):
                            logger.debug(
                                f"view {raw_view_name} is not reachable from an explore, skipping.."
                            )
                            self.reporter.report_unreachable_view_dropped(raw_view_name)
                            continue

                        self.reporter.report_views_scanned()
                        try:
                            raw_view = looker_refinement_resolver.apply_view_refinement(
                                raw_view=raw_view,
                            )

                            current_project_name: str = (
                                include.project
                                if include.project != _BASE_PROJECT_NAME
                                else project_name
                            )

                            # if project is base project then it is available as self.base_projects_folder[
                            # _BASE_PROJECT_NAME]
                            base_folder_path: str = str(
                                self.base_projects_folder.get(
                                    current_project_name,
                                    self.base_projects_folder[_BASE_PROJECT_NAME],
                                )
                            )

                            view_context: LookerViewContext = LookerViewContext(
                                raw_view=raw_view,
                                view_file=looker_viewfile,
                                view_connection=connection_definition,
                                view_file_loader=viewfile_loader,
                                looker_refinement_resolver=looker_refinement_resolver,
                                base_folder_path=base_folder_path,
                                reporter=self.reporter,
                            )

                            maybe_looker_view = LookerView.from_looker_dict(
                                project_name=current_project_name,
                                model_name=model_name,
                                view_context=view_context,
                                looker_view_id_cache=looker_view_id_cache,
                                reporter=self.reporter,
                                max_file_snippet_length=self.source_config.max_file_snippet_length,
                                extract_col_level_lineage=self.source_config.extract_column_level_lineage,
                                populate_sql_logic_in_descriptions=self.source_config.populate_sql_logic_for_missing_descriptions,
                                config=self.source_config,
                                ctx=self.ctx,
                            )
                        except Exception as e:
                            self.reporter.report_warning(
                                title="Error Loading View",
                                message="Unable to load Looker View.",
                                context=f"View Details: {raw_view}",
                                exc=e,
                            )

                            logger.debug(e, exc_info=e)

                            continue

                        if maybe_looker_view:
                            if self.source_config.view_pattern.allowed(
                                maybe_looker_view.id.view_name
                            ):
                                view_urn = maybe_looker_view.id.get_urn(
                                    self.source_config
                                )
                                view_connection_mapping = view_connection_map.get(
                                    view_urn
                                )
                                if not view_connection_mapping:
                                    view_connection_map[view_urn] = (
                                        model_name,
                                        model.connection,
                                    )
                                    # first time we are discovering this view
                                    logger.debug(
                                        f"Generating MCP for view {raw_view['name']}"
                                    )

                                    if (
                                        maybe_looker_view.id.project_name
                                        not in self.processed_projects
                                    ):
                                        yield from self.gen_project_workunits(
                                            maybe_looker_view.id.project_name
                                        )

                                        self.processed_projects.append(
                                            maybe_looker_view.id.project_name
                                        )

                                    for mcp in self._build_dataset_mcps(
                                        maybe_looker_view
                                    ):
                                        yield mcp.as_workunit()
                                    mce = self._build_dataset_mce(maybe_looker_view)
                                    yield MetadataWorkUnit(
                                        id=f"lookml-view-{maybe_looker_view.id}",
                                        mce=mce,
                                    )
                                    processed_view_files.add(include.include)
                                else:
                                    (
                                        prev_model_name,
                                        prev_model_connection,
                                    ) = view_connection_mapping
                                    if prev_model_connection != model.connection:
                                        # this view has previously been discovered and emitted using a different
                                        # connection
                                        logger.warning(
                                            f"view {maybe_looker_view.id.view_name} from model {model_name}, connection {model.connection} was previously processed via model {prev_model_name}, connection {prev_model_connection} and will likely lead to incorrect lineage to the underlying tables"
                                        )
                                        if (
                                            not self.source_config.emit_reachable_views_only
                                        ):
                                            logger.warning(
                                                "Consider enabling the `emit_reachable_views_only` flag to handle this case."
                                            )
                            else:
                                self.reporter.report_views_dropped(
                                    str(maybe_looker_view.id)
                                )

        if (
            self.source_config.tag_measures_and_dimensions
            and self.reporter.events_produced != 0
        ):
            # Emit tag MCEs for measures and dimensions:
            for tag_mce in LookerUtil.get_tag_mces():
                yield MetadataWorkUnit(
                    id=f"tag-{tag_mce.proposedSnapshot.urn}", mce=tag_mce
                )

    def gen_project_workunits(self, project_name: str) -> Iterable[MetadataWorkUnit]:
        project_key = gen_project_key(
            self.source_config,
            project_name,
        )
        yield from gen_containers(
            container_key=project_key,
            name=project_name,
            sub_types=[BIContainerSubTypes.LOOKML_PROJECT],
        )
        yield MetadataChangeProposalWrapper(
            entityUrn=project_key.as_urn(),
            aspect=BrowsePathsV2Class(
                path=[BrowsePathEntryClass("Folders")],
            ),
        ).as_workunit()

    def get_report(self):
        return self.reporter
