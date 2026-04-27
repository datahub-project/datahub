import logging
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional

import sqlglot
import sqlglot.expressions as sqlglot_exp

from datahub.emitter.mce_builder import (
    make_data_flow_urn,
    make_data_job_urn_with_flow,
    make_group_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import (
    DataJobSubTypes,
    FlowContainerSubTypes,
)
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SnowflakeDataDictionary,
    SnowflakePipe,
)
from datahub.ingestion.source.snowflake.snowflake_stages import (
    SnowflakeStagesExtractor,
)
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeIdentifierBuilder,
)
from datahub.metadata.schema_classes import (
    DataFlowInfoClass,
    DataJobInfoClass,
    DataJobInputOutputClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    StatusClass,
    SubTypesClass,
)

logger: logging.Logger = logging.getLogger(__name__)

# Truncate the COPY INTO definition stored in customProperties to stay well
# within DataHub's aspect size limits.
_MAX_DEFINITION_LENGTH = 4000


@dataclass
class ParsedCopyInto:
    """Target table and source stage extracted from a COPY INTO statement.

    Both names are uppercase fully-qualified (`DB.SCHEMA.NAME`).
    """

    target_fqn: str
    stage_fqn: str


def _stage_reference_to_fqn(
    raw: str, default_db: str, default_schema: str
) -> Optional[str]:
    """Convert a raw stage reference like `@db.schema.stage/path` into a 3-part FQN."""
    if not raw.startswith("@"):
        return None
    stripped = raw[1:].split("/", 1)[0]
    if not stripped:
        return None

    parts = [part.strip('"') for part in stripped.split(".") if part.strip('"')]
    if len(parts) == 1:
        db, schema, name = default_db, default_schema, parts[0]
    elif len(parts) == 2:
        db, schema, name = default_db, parts[0], parts[1]
    elif len(parts) == 3:
        db, schema, name = parts[0], parts[1], parts[2]
    else:
        return None
    return f"{db}.{schema}.{name}".upper()


def parse_copy_into(
    definition: str, default_db: str, default_schema: str
) -> Optional[ParsedCopyInto]:
    """Parse a COPY INTO statement to extract target table and stage reference.

    Uses sqlglot with the Snowflake dialect, so quoted identifiers, fully
    qualified names, and trailing path / FILE_FORMAT options are all handled
    by the parser itself.

    Returns `None` if the statement is not a COPY INTO or required parts are
    missing.
    """
    if not definition:
        return None

    try:
        tree = sqlglot.parse_one(definition, dialect="snowflake")
    except sqlglot.errors.ParseError:
        return None

    if not isinstance(tree, sqlglot_exp.Copy):
        return None

    target = tree.this
    if not isinstance(target, sqlglot_exp.Table) or not target.name:
        return None
    target_db = target.catalog or default_db
    target_schema = target.db or default_schema
    target_fqn = f"{target_db}.{target_schema}.{target.name}".upper()

    files = tree.args.get("files") or []
    if not files:
        return None
    file_expr = files[0]
    stage_raw = (
        file_expr.this.name
        if isinstance(file_expr, sqlglot_exp.Table)
        and isinstance(file_expr.this, sqlglot_exp.Var)
        else None
    )
    if not stage_raw:
        return None
    stage_fqn = _stage_reference_to_fqn(stage_raw, default_db, default_schema)
    if not stage_fqn:
        return None

    return ParsedCopyInto(target_fqn=target_fqn, stage_fqn=stage_fqn)


@dataclass
class SnowflakePipesExtractor:
    config: SnowflakeV2Config
    report: SnowflakeV2Report
    data_dictionary: SnowflakeDataDictionary
    identifiers: SnowflakeIdentifierBuilder
    stages_extractor: SnowflakeStagesExtractor

    def get_workunits(
        self,
        db_name: str,
        schema_name: str,
    ) -> Iterable[MetadataWorkUnit]:
        pipes = self.data_dictionary.get_pipes_for_schema(db_name, schema_name)
        if not pipes:
            return

        allowed_pipes = [
            pipe
            for pipe in pipes
            if self.config.pipe_pattern.allowed(
                f"{db_name}.{schema_name}.{pipe.name}".upper()
            )
        ]
        if not allowed_pipes:
            return

        flow_id = self.identifiers.snowflake_identifier(
            f"{db_name}.{schema_name}.pipes"
        )
        flow_urn = make_data_flow_urn(
            orchestrator="snowflake",
            flow_id=flow_id,
            cluster=self.config.env,
            platform_instance=self.config.platform_instance,
        )

        yield from self._gen_data_flow(flow_urn, db_name, schema_name)

        for pipe in allowed_pipes:
            self.report.pipes_scanned += 1

            yield from self._gen_data_job(
                pipe=pipe,
                flow_urn=flow_urn,
                db_name=db_name,
                schema_name=schema_name,
            )

    def _gen_data_flow(
        self,
        flow_urn: str,
        db_name: str,
        schema_name: str,
    ) -> Iterable[MetadataWorkUnit]:
        yield MetadataChangeProposalWrapper(
            entityUrn=flow_urn,
            aspect=DataFlowInfoClass(
                name=f"{db_name}.{schema_name} Pipes",
                description=f"Snowflake Snowpipe objects in {db_name}.{schema_name}",
                customProperties={
                    "database": db_name,
                    "schema": schema_name,
                    "object_type": "SNOWFLAKE_PIPES",
                },
            ),
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=flow_urn,
            aspect=SubTypesClass(
                typeNames=[FlowContainerSubTypes.SNOWFLAKE_PIPE_GROUP],
            ),
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=flow_urn,
            aspect=StatusClass(removed=False),
        ).as_workunit()

    def _gen_data_job(
        self,
        pipe: SnowflakePipe,
        flow_urn: str,
        db_name: str,
        schema_name: str,
    ) -> Iterable[MetadataWorkUnit]:
        job_id = self.identifiers.snowflake_identifier(pipe.name)
        job_urn = make_data_job_urn_with_flow(flow_urn, job_id)

        parsed = parse_copy_into(pipe.definition, db_name, schema_name)
        if parsed is None and pipe.definition:
            self.report.warning(
                "Failed to parse COPY INTO statement for pipe lineage",
                f"{db_name}.{schema_name}.{pipe.name}",
            )
        target_fqn = parsed.target_fqn if parsed else None
        stage_fqn = parsed.stage_fqn if parsed else None

        custom_properties: Dict[str, str] = {}
        if pipe.auto_ingest:
            custom_properties["auto_ingest"] = "true"
        if pipe.notification_channel:
            custom_properties["notification_channel"] = pipe.notification_channel
        if pipe.definition:
            custom_properties["definition"] = pipe.definition[:_MAX_DEFINITION_LENGTH]
        if stage_fqn:
            custom_properties["stage_name"] = stage_fqn

        stage_entry = (
            self.stages_extractor.get_stage_lookup_entry(stage_fqn)
            if stage_fqn
            else None
        )
        if stage_entry:
            custom_properties["stage_type"] = stage_entry.stage.stage_type.value

        yield MetadataChangeProposalWrapper(
            entityUrn=job_urn,
            aspect=DataJobInfoClass(
                name=pipe.name,
                description=pipe.comment,
                type="COMMAND",
                customProperties=custom_properties,
            ),
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=job_urn,
            aspect=SubTypesClass(
                typeNames=[DataJobSubTypes.SNOWFLAKE_PIPE],
            ),
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=job_urn,
            aspect=StatusClass(removed=False),
        ).as_workunit()

        input_datasets: List[str] = []
        output_datasets: List[str] = []

        if target_fqn:
            target_dataset_identifier = self.identifiers.snowflake_identifier(
                target_fqn
            )
            target_urn = self.identifiers.gen_dataset_urn(target_dataset_identifier)
            output_datasets.append(target_urn)

        if stage_entry and stage_entry.dataset_urn:
            input_datasets.append(stage_entry.dataset_urn)

        if input_datasets or output_datasets:
            yield MetadataChangeProposalWrapper(
                entityUrn=job_urn,
                aspect=DataJobInputOutputClass(
                    inputDatasets=input_datasets,
                    outputDatasets=output_datasets,
                    inputDatajobs=[],
                ),
            ).as_workunit()

        if pipe.owner:
            yield MetadataChangeProposalWrapper(
                entityUrn=job_urn,
                aspect=OwnershipClass(
                    owners=[
                        OwnerClass(
                            owner=make_group_urn(pipe.owner),
                            type=OwnershipTypeClass.TECHNICAL_OWNER,
                        )
                    ]
                ),
            ).as_workunit()
