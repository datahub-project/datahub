import html
import logging
from collections import defaultdict
from pathlib import Path
from typing import Iterable, List

import sqlglot
from sqlalchemy import create_engine, text

from datahub.emitter.mce_builder import make_data_job_urn, make_dataset_urn
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import config_class, platform_name
from datahub.ingestion.api.source import Source
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.informatica.data_classes import (
    FolderInfo,
    SessionMappingEdge,
    SourceQualifierWidget,
    SourceWidget,
    Synonyms,
    TargetWidget,
    WidgetLineage,
    WorkflowInfo,
)
from datahub.ingestion.source.informatica.informatica_config import (
    InformaticaConfig,
    InformaticaSourceReport,
)
from datahub.ingestion.source.informatica.mappers.dataflow_mapper import (
    make_dataflow_workunit,
)
from datahub.ingestion.source.informatica.mappers.datajob_mapper import (
    make_datajob_workunit,
)
from datahub.ingestion.source.informatica.mappers.dataset_mapper import (
    make_dataset_snapshot_workunit,
)
from datahub.ingestion.source.informatica.mappers.lineage_mapper import (
    make_dataset_lineage_mcp,
    make_synonym_lineage_mcp,
)
from datahub.ingestion.source.informatica.sql_loader import load_sql
from datahub.metadata._schema_classes import MetadataChangeProposalClass

logger = logging.getLogger(__name__)
SQL_DIR = Path(__file__).parent / "sql"


@platform_name("Informatica")
@config_class(InformaticaConfig)
class InformaticaSource(Source):
    def __init__(self, config: InformaticaConfig, ctx: PipelineContext):
        self.config = config
        self.ctx = ctx
        self.report = InformaticaSourceReport()
        if config.type == "powercenter":
            self.engine = self._create_engine()

    def _create_engine(self):
        return create_engine(
            f"{self.config.scheme}://{self.config.username}:{self.config.password}@{self.config.host_port}/?service_name={self.config.service_name}"
        )

    @classmethod
    def create(cls, config_dict, ctx):
        config = InformaticaConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        # TODO: 실제 Informatica 메타DB 수집 로직
        folders = self.get_filtered_folders()
        # 폴더부터 수집
        for folder in folders:
            folder_name = folder.subj_name
            folder_id = folder.subj_id
            logger.debug(f"[폴더] {folder_name} (ID: {folder_id})")
            self.report.total_folders += 1
            workflows = self.get_filtered_workflows(folder_id)

            for wf in workflows:
                flow_id = f"{folder_name}.{wf.workflow_name}"  ## TODO: folder에 대한 설명은 어디에 넣어야하나.
                logger.debug(f"flow_id ingestion start, {flow_id}")
                # 1. DataFlow 등록
                yield make_dataflow_workunit(flow_id, wf, self.config.env)
                self.report.total_dataflows += 1
                # 2. 세션/매핑 추출 쿼리 실행
                workflow_lineages = self.get_workflow_lineage(wf.workflow_id)

                datajob_ids = set()
                # 3. datajob 등록
                for workflow_lineage_element in workflow_lineages:
                    if not workflow_lineage_element.to_mapping_id:
                        logger.debug(
                            f"workflow_lineage_element, {workflow_lineage_element} {workflow_lineage_element.to_instance}, mapping_id is None"
                        )
                        continue
                    datajob_id = str(workflow_lineage_element.to_mapping_id)
                    datajob_name = workflow_lineage_element.to_instance
                    logger.debug(
                        f"flow_id={flow_id}, job_id={datajob_id}, job_name={datajob_name}"
                    )

                    if datajob_id and datajob_id not in datajob_ids:
                        wu_list = self.build_dataset_lineages(
                            flow_id,
                            datajob_name,
                            workflow_lineage_element.to_mapping_id,
                            workflow_lineage_element.to_session_id,
                        )
                        for wu in wu_list:
                            yield wu
                            if isinstance(wu.metadata, MetadataChangeProposalClass):
                                if wu.metadata.aspectName == "dataJobInputOutput":
                                    self.report.total_lineages += 1

                        datajob_ids.add(datajob_id)

        if self.config.synonym_tracking_enabled:
            synonyms = self.get_synonym()
            for synonym in synonyms:
                logger.debug(
                    f"{synonym.owner}.{synonym.synonym_name} -> {synonym.table_owner}.{synonym.table_name}"
                )
                yield make_synonym_lineage_mcp(
                    f"{synonym.owner}.{synonym.synonym_name}".lower(),
                    f"{synonym.table_owner}.{synonym.table_name}".lower(),
                    "oracle",
                    self.config.env,
                )

    def get_report(self):
        return self.report

    def get_filtered_folders(self) -> List[FolderInfo]:
        """
        PowerCenter 메타DB에서 폴더 목록을 조회하고 allow/deny 패턴 필터링을 적용하여 반환
        """
        if self.config.type != "powercenter":
            raise ValueError("Only powercenter type supports DB connection")

        query = load_sql("get_folders.sql").format(
            metadata_schema=self.config.metadata_schema
        )

        with self.engine.connect() as conn:
            result = conn.execute(text(query))

            folder_infos = [FolderInfo(**row._mapping) for row in result]

        logger.debug(f"total folder count: {len(folder_infos)}")

        filtered = []
        for folder_info in folder_infos:
            folder_name = folder_info.subj_name

            if self.config.folder_patterns.allowed(folder_name):
                filtered.append(folder_info)

        logger.debug(f"filtered folder count: {len(filtered)}")

        return filtered

    def get_filtered_workflows(self, folder_id: int) -> List[WorkflowInfo]:
        """
        지정된 폴더(subj_id)에 대해 워크플로우 목록을 조회하고
        allow/deny 패턴을 적용하여 필터링된 리스트를 반환.
        """
        query = load_sql("get_workflows.sql").format(
            metadata_schema=self.config.metadata_schema
        )

        with self.engine.connect() as conn:
            result = conn.execute(text(query), {"folder_id": folder_id})
            workflow_infos = [WorkflowInfo(**row._mapping) for row in result]
        logger.debug(f"total workflow count: {len(workflow_infos)}")

        filtered = []

        for workflow_info in workflow_infos:
            wf_name = workflow_info.workflow_name
            if self.config.workflow_patterns.allowed(wf_name):
                filtered.append(workflow_info)

        logger.debug(f"filtered workflow count: {len(filtered)}")

        return filtered

    def get_workflow_lineage(self, workflow_id: int) -> List[SessionMappingEdge]:
        query = load_sql("get_workflow_lineage.sql").format(
            metadata_schema=self.config.metadata_schema
        )
        with self.engine.connect() as conn:
            result = conn.execute(text(query), {"workflow_id": workflow_id})
            return [SessionMappingEdge(**row._mapping) for row in result]

    # 위젯 타입 = 1 (source definition)
    def get_source_widgets(self, mapping_id: int) -> dict[int, SourceWidget]:
        query = load_sql("get_source_widgets.sql").format(
            metadata_schema=self.config.metadata_schema
        )
        with self.engine.connect() as conn:
            result = conn.execute(text(query), {"mapping_id": mapping_id})
            widget_list = [SourceWidget(**row._mapping) for row in result]
            widget_map = {}
            for widget in widget_list:
                widget_map[widget.widget_id] = widget
            return widget_map

    # 위젯 타입 = 3 (source qualifier)
    def get_source_qualifier_widgets(
        self, mapping_id: int, session_id: int
    ) -> dict[int, SourceQualifierWidget]:
        query = load_sql("get_source_qualifier_widgets.sql").format(
            metadata_schema=self.config.metadata_schema
        )
        with self.engine.connect() as conn:
            result = conn.execute(
                text(query), {"mapping_id": mapping_id, "session_id": session_id}
            )

            widget_list = [SourceQualifierWidget(**row._mapping) for row in result]
            widget_map = {}
            for widget in widget_list:
                widget_map[widget.widget_id] = widget
            return widget_map

    # 위젯 타입 = 2 (target definition)
    def get_target_widgets(
        self, mapping_id: int, session_id: int
    ) -> dict[int, TargetWidget]:
        query = load_sql("get_target_widgets.sql").format(
            metadata_schema=self.config.metadata_schema
        )
        with self.engine.connect() as conn:
            result = conn.execute(
                text(query), {"mapping_id": mapping_id, "session_id": session_id}
            )
            widget_list = [TargetWidget(**row._mapping) for row in result]
            widget_map = {}
            for widget in widget_list:
                widget_map[widget.widget_id] = widget
            return widget_map

    def get_widget_lineage(self, mapping_id: int) -> list[WidgetLineage]:
        query = load_sql("get_widget_lineage.sql").format(
            metadata_schema=self.config.metadata_schema
        )

        with self.engine.connect() as conn:
            result = conn.execute(text(query), {"mapping_id": mapping_id})
            return [WidgetLineage(**row._mapping) for row in result]

    def get_synonym(self) -> list[Synonyms]:
        query = load_sql("get_synonym.sql")

        with self.engine.connect() as conn:
            result = conn.execute(text(query))
            return [Synonyms(**row._mapping) for row in result]

    def extract_source_tables_from_sql(
        self, query: str, target_table_name: str
    ) -> list[str]:
        try:
            decoded_query = html.unescape(query)
            transform_query = decoded_query.replace(
                "$$P_TARGET_NAME", target_table_name
            ).replace("$$p_target_name", target_table_name)
            parsed = sqlglot.parse_one(transform_query)
            return [
                f"{t.args.get('db')}.{t.this.sql()}"
                if t.args.get("db")
                else t.this.sql()
                for t in parsed.find_all(sqlglot.exp.Table)
            ]
        except Exception as e:
            logger.debug(query)
            logger.warning(f"[SQL PARSE ERROR]: {e}")
            return []

    def build_dataset_lineages(
        self, flow_id: str, job_name: str, mapping_id: int, session_id: int
    ) -> list[MetadataWorkUnit]:
        lineage_list: list[WidgetLineage] = self.get_widget_lineage(mapping_id)

        source_qualifiers: dict[int, SourceQualifierWidget] = (
            self.get_source_qualifier_widgets(mapping_id, session_id)
        )
        sources: dict[int, SourceWidget] = self.get_source_widgets(mapping_id)
        targets: dict[int, TargetWidget] = self.get_target_widgets(
            mapping_id, session_id
        )

        ## lineage_list에서 to_instance_id 기준으로 묶어서 list[int, list[any]] 형태로 저장
        lineage_grouped = defaultdict(list)
        logger.debug(f"lineage_list length : {len(lineage_list)}")
        for lineage in lineage_list:
            logger.debug(lineage)
            if lineage.to_instance_id:
                lineage_grouped[lineage.to_instance_id].append(lineage)
        logger.debug(f"lineage_grouped length : {len(lineage_grouped)}")
        logger.debug(lineage_grouped)
        for group in lineage_grouped.items():
            sorted_group = sorted(
                group, key=lambda f: int(f.from_widget_type), reverse=True
            )
            inlets = []
            job_urn = ""
            to_urn = ""

            for edge in sorted_group:
                target = targets.get(edge.to_widget_id)
                if not target:
                    continue

                target_table_name = target.pv_table_name or target.target_name
                if not target_table_name or not target.schema_name:
                    continue

                datajob_id = f"{str(mapping_id)}_{edge.to_widget_id}"
                datajob_name = f"{job_name}-{target_table_name}"
                job_urn = make_data_job_urn(
                    orchestrator="informatica",
                    flow_id=flow_id,
                    job_id=datajob_id,
                    cluster=self.config.env,
                )
                yield make_datajob_workunit(
                    job_urn=job_urn, job_id=datajob_id, job_name=datajob_name
                )

                # 타겟 테이블 URN
                to_urn = make_dataset_urn(
                    "oracle",
                    f"{target.schema_name}.{target_table_name}".lower(),
                    env=self.config.env,
                )
                yield make_dataset_snapshot_workunit(
                    to_urn, f"{target.schema_name}.{target_table_name}".lower()
                )
                logger.debug(f"target is {target.schema_name}.{target_table_name}")
                ## TODO: platform은 어떻게 할 것인가
                ## 우선 테스트 진행해보자
                if edge.from_widget_type == "3":  # Source Qualifier
                    source_qualifier = source_qualifiers.get(edge.from_widget_id)
                    if (
                        source_qualifier
                        and source_qualifier.full_query
                        and source_qualifier.full_query.strip()
                    ):
                        source_tables = self.extract_source_tables_from_sql(
                            source_qualifier.full_query,
                            f"{target.schema_name}.{target_table_name}",
                        )

                        for table in source_tables:
                            if "." not in table:
                                table = f"default.{table}"
                            from_urn = make_dataset_urn(
                                "oracle", table.lower(), env=self.config.env
                            )
                            yield make_dataset_snapshot_workunit(
                                from_urn, table.lower()
                            )
                            logger.debug(
                                f"dataset_lineage, from_urn={from_urn}, to_urn={to_urn}, job_urn={job_urn}"
                            )
                            inlets.append(from_urn)
                        break

                if edge.from_widget_type == "1":
                    source = sources.get(edge.from_widget_id)
                    if source and source.db_name and source.source_name:
                        from_urn = make_dataset_urn(
                            "oracle",
                            f"{source.db_name}.{source.source_name}".lower(),
                            env=self.config.env,
                        )
                        yield make_dataset_snapshot_workunit(
                            from_urn, f"{source.db_name}.{source.source_name}".lower()
                        )
                        logger.debug(
                            f"dataset_lineage, from_urn={from_urn}, to_urn={to_urn}, job_urn={job_urn}"
                        )
                        inlets.append(from_urn)
                        break

            if job_urn != "":
                logger.debug(f"send lineage, {job_urn}")
                yield make_dataset_lineage_mcp(inlets, to_urn, job_urn)
