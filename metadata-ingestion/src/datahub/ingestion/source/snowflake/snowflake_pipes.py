import logging
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

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
    StageLookupEntry,
)
from datahub.ingestion.source.snowflake.snowflake_utils import (
    MAX_DEFINITION_LENGTH,
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


@dataclass(frozen=True)
class ParsedCopyInto:
    """Target table and source stages extracted from a COPY INTO statement.

    All names are uppercase fully-qualified (``DB.SCHEMA.NAME``). ``stage_fqns``
    is de-duplicated and typically length 1. For subqueries that UNION multiple
    stages, all referenced stages are collected; order follows sqlglot's AST
    traversal and may not match SQL source order for 3+ branches.
    """

    target_fqn: str
    stage_fqns: Tuple[str, ...]


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


def _extract_stage_references(expr: sqlglot_exp.Expression) -> List[str]:
    """Return all raw ``@stage`` references within ``expr``, in source order.

    Handles both direct ``FROM @stage`` (where ``expr`` is a ``Table``) and
    subquery forms like ``FROM (SELECT ... FROM @stage)``, including subqueries
    that ``UNION ALL`` over multiple stages. Duplicates are removed while
    preserving the first-seen order.
    """
    names = (
        node.this.name
        for node in expr.find_all(sqlglot_exp.Table)
        if isinstance(node.this, sqlglot_exp.Var) and node.this.name.startswith("@")
    )
    return list(dict.fromkeys(names))


def parse_copy_into(
    definition: str,
    default_db: str,
    default_schema: str,
    unresolved_refs: Optional[List[str]] = None,
) -> Optional[ParsedCopyInto]:
    """Parse a COPY INTO statement to extract target table and stage references.

    Uses sqlglot with the Snowflake dialect, so quoted identifiers, fully
    qualified names, and trailing path / FILE_FORMAT options are all handled
    by the parser itself.

    Returns ``None`` if the statement is not a COPY INTO or no usable stage
    reference can be resolved.

    ``unresolved_refs`` is an optional output collector. When provided, raw
    stage references that the parser saw but could not normalize to a 3-part
    FQN (e.g. >3 dotted parts, malformed names) are appended to it. This lets
    the caller distinguish "stage refs were present but unparseable" from
    "the COPY INTO had no stage refs at all".
    """
    if not definition:
        return None

    try:
        tree = sqlglot.parse_one(definition, dialect="snowflake")
    except sqlglot.errors.ParseError as e:
        # The caller emits a user-facing warning but has no visibility into
        # parser internals; re-running with -d/--debug surfaces this message.
        logger.debug("sqlglot failed to parse COPY INTO: %s", e)
        return None

    if not isinstance(tree, sqlglot_exp.Copy):
        return None

    # When COPY INTO specifies a column list (e.g. ``COPY INTO t(a, b) ...``),
    # sqlglot wraps the target in a Schema expression around the underlying Table.
    target = tree.this
    if isinstance(target, sqlglot_exp.Schema):
        target = target.this
    if not isinstance(target, sqlglot_exp.Table) or not target.name:
        return None
    target_db = target.catalog or default_db
    target_schema = target.db or default_schema
    target_fqn = f"{target_db}.{target_schema}.{target.name}".upper()

    files = tree.args.get("files") or []
    if not files:
        return None
    stage_fqns: List[str] = []
    for raw in _extract_stage_references(files[0]):
        fqn = _stage_reference_to_fqn(raw, default_db, default_schema)
        if fqn is None:
            if unresolved_refs is not None:
                unresolved_refs.append(raw)
            continue
        if fqn not in stage_fqns:
            stage_fqns.append(fqn)
    if not stage_fqns:
        return None

    return ParsedCopyInto(target_fqn=target_fqn, stage_fqns=tuple(stage_fqns))


def _looks_like_copy_into(definition: str) -> bool:
    """Heuristic: does the pipe body start with the COPY keyword?"""
    return definition.lstrip().upper().startswith("COPY")


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
        pipe_fqn = f"{db_name}.{schema_name}.{pipe.name}"

        unparsed_stage_refs: List[str] = []
        parsed = parse_copy_into(
            pipe.definition,
            db_name,
            schema_name,
            unresolved_refs=unparsed_stage_refs,
        )
        if unparsed_stage_refs:
            self.report.warning(
                "Pipe COPY INTO stage reference could not be normalized; "
                "lineage may be incomplete",
                f"{pipe_fqn} -> {', '.join(unparsed_stage_refs)}",
            )
        elif parsed is None and pipe.definition:
            if _looks_like_copy_into(pipe.definition):
                self.report.warning(
                    "Failed to parse COPY INTO statement for pipe lineage",
                    pipe_fqn,
                )
            else:
                # Non-COPY pipe bodies are legal but unsupported for lineage;
                # skip silently to avoid a warning on every such pipe.
                logger.debug(
                    "Pipe %s definition is not a COPY INTO; skipping lineage extraction",
                    pipe_fqn,
                )
        target_fqn = parsed.target_fqn if parsed else None
        stage_fqns: Tuple[str, ...] = parsed.stage_fqns if parsed else ()

        stage_entries: List[StageLookupEntry] = []
        unresolved_stages: List[str] = []
        for stage_fqn in stage_fqns:
            entry = self.stages_extractor.get_stage_lookup_entry(stage_fqn)
            if entry is None:
                unresolved_stages.append(stage_fqn)
            else:
                stage_entries.append(entry)

        if unresolved_stages:
            # Stage was referenced by the COPY INTO but not in the lookup
            # (filtered by `stage_pattern`, dropped, or not scanned). Surface this
            # so users can see why a pipe's input lineage may be incomplete.
            self.report.warning(
                "Stage referenced by pipe COPY INTO could not be resolved; "
                "pipe input lineage may be incomplete",
                f"{pipe_fqn} -> {', '.join(unresolved_stages)}",
            )

        custom_properties: Dict[str, str] = {}
        if pipe.auto_ingest:
            custom_properties["auto_ingest"] = "true"
        if pipe.notification_channel:
            custom_properties["notification_channel"] = pipe.notification_channel
        if pipe.definition:
            custom_properties["definition"] = pipe.definition[:MAX_DEFINITION_LENGTH]
        if stage_fqns:
            custom_properties["stage_name"] = ", ".join(stage_fqns)
        if stage_entries:
            unique_types = list(
                dict.fromkeys(e.stage.stage_type.value for e in stage_entries)
            )
            custom_properties["stage_type"] = ", ".join(unique_types)

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

        stages_without_urn: List[str] = []
        for entry in stage_entries:
            if entry.dataset_urn is None:
                stages_without_urn.append(
                    f"{entry.stage.database_name}."
                    f"{entry.stage.schema_name}."
                    f"{entry.stage.name}"
                )
            elif entry.dataset_urn not in input_datasets:
                input_datasets.append(entry.dataset_urn)

        if stages_without_urn:
            # Stage was found, but its underlying dataset URN couldn't be resolved
            # (e.g. unsupported external storage scheme in `_resolve_external_stage_url`).
            self.report.warning(
                "Stage referenced by pipe has no resolvable dataset URN; "
                "pipe input lineage will not include it",
                f"{pipe_fqn} -> {', '.join(stages_without_urn)}",
            )

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
