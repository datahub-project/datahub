import hashlib
import json
import re
import time
from dataclasses import dataclass
from datetime import datetime
from enum import Enum, auto
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Sequence,
    Union,
)

from datahub.emitter import mce_builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    AssertionNoteClass,
    AssertionResultClass,
    AssertionResultSeverityClass,
    AssertionResultTypeClass,
    AssertionRunEventClass,
    AssertionRunStatusClass,
    AssertionSourceClass,
    AssertionSourceTypeClass,
    AssertionStdAggregationClass,
    AssertionStdOperatorClass,
    AssertionStdParameterClass,
    AssertionStdParametersClass,
    AssertionStdParameterTypeClass,
    AssertionTypeClass,
    AuditStampClass,
    CustomAssertionInfoClass,
    DatasetAssertionScopeClass,
)

if TYPE_CHECKING:
    from datahub.ingestion.source.dbt.dbt_common import DBTNode


@dataclass
class DBTTest:
    qualified_test_name: str
    column_name: Optional[str]
    kw_args: dict
    # The model the test is declared on (manifest ``attached_node``, dbt 1.5+).
    attached_node: Optional[str] = None
    # The test's ``where:`` filter, if any.
    where: Optional[str] = None


@dataclass
class DBTTestResult:
    invocation_id: str

    status: str
    execution_time: datetime

    native_results: Dict[str, str]

    def has_success_status(self) -> bool:
        return self.status in ("pass", "success")


@dataclass
class DBTFreshnessCriteria:
    """Represents warn_after or error_after criteria from dbt freshness"""

    count: int
    period: str  # minute, hour, day


@dataclass
class DBTFreshnessInfo:
    """Freshness test information from dbt sources.json"""

    invocation_id: str
    status: str  # pass, warn, error, runtime error
    max_loaded_at: datetime
    snapshotted_at: datetime
    max_loaded_at_time_ago_in_s: float
    warn_after: Optional[DBTFreshnessCriteria]
    error_after: Optional[DBTFreshnessCriteria]


def parse_freshness_criteria(data: Optional[Dict]) -> Optional[DBTFreshnessCriteria]:
    """Parse warn_after or error_after criteria dict into DBTFreshnessCriteria."""
    if not data:
        return None
    count = data.get("count")
    period = data.get("period")
    if count is None or period is None:
        return None
    return DBTFreshnessCriteria(
        count=count,
        period=period,
    )


def _get_name_for_relationship_test(kw_args: Dict[str, str]) -> Optional[str]:
    """
    Try to produce a useful string for the name of a relationship constraint.
    Return None if we fail to
    """
    destination_ref = kw_args.get("to")
    source_ref = kw_args.get("model")
    column_name = kw_args.get("column_name")
    dest_field_name = kw_args.get("field")
    if not destination_ref or not source_ref or not column_name or not dest_field_name:
        # base assertions are violated, bail early
        return None
    m = re.match(r"^ref\(\'(.*)\'\)$", destination_ref)
    destination_table = m.group(1) if m else destination_ref

    m = re.search(r"ref\(\'(.*)\'\)", source_ref)
    source_table = m.group(1) if m else source_ref

    return f"{source_table}.{column_name} referential integrity to {destination_table}.{dest_field_name}"


@dataclass
class AssertionParams:
    scope: Union[DatasetAssertionScopeClass, str]
    operator: Union[AssertionStdOperatorClass, str]
    aggregation: Union[AssertionStdAggregationClass, str]
    parameters: Optional[Callable[[Dict[str, str]], AssertionStdParametersClass]] = None
    logic_fn: Optional[Callable[[Dict[str, str]], Optional[str]]] = None


_DBT_TEST_NAME_TO_ASSERTION_MAP: Dict[str, AssertionParams] = {
    "not_null": AssertionParams(
        scope=DatasetAssertionScopeClass.DATASET_COLUMN,
        operator=AssertionStdOperatorClass.NOT_NULL,
        aggregation=AssertionStdAggregationClass.IDENTITY,
    ),
    "unique": AssertionParams(
        scope=DatasetAssertionScopeClass.DATASET_COLUMN,
        operator=AssertionStdOperatorClass.EQUAL_TO,
        aggregation=AssertionStdAggregationClass.UNIQUE_PROPOTION,
        parameters=lambda _: AssertionStdParametersClass(
            value=AssertionStdParameterClass(
                value="1.0",
                type=AssertionStdParameterTypeClass.NUMBER,
            )
        ),
    ),
    "accepted_values": AssertionParams(
        scope=DatasetAssertionScopeClass.DATASET_COLUMN,
        operator=AssertionStdOperatorClass.IN,
        aggregation=AssertionStdAggregationClass.IDENTITY,
        parameters=lambda kw_args: AssertionStdParametersClass(
            value=AssertionStdParameterClass(
                value=json.dumps(kw_args.get("values")),
                type=AssertionStdParameterTypeClass.SET,
            ),
        ),
    ),
    "relationships": AssertionParams(
        scope=DatasetAssertionScopeClass.DATASET_COLUMN,
        operator=AssertionStdOperatorClass._NATIVE_,
        aggregation=AssertionStdAggregationClass.IDENTITY,
        parameters=lambda kw_args: AssertionStdParametersClass(
            value=AssertionStdParameterClass(
                value=json.dumps(kw_args.get("values")),
                type=AssertionStdParameterTypeClass.SET,
            ),
        ),
        logic_fn=_get_name_for_relationship_test,
    ),
    "dbt_expectations.expect_column_values_to_not_be_null": AssertionParams(
        scope=DatasetAssertionScopeClass.DATASET_COLUMN,
        operator=AssertionStdOperatorClass.NOT_NULL,
        aggregation=AssertionStdAggregationClass.IDENTITY,
    ),
    "dbt_expectations.expect_column_values_to_be_between": AssertionParams(
        scope=DatasetAssertionScopeClass.DATASET_COLUMN,
        operator=AssertionStdOperatorClass.BETWEEN,
        aggregation=AssertionStdAggregationClass.IDENTITY,
        parameters=lambda x: AssertionStdParametersClass(
            minValue=AssertionStdParameterClass(
                value=str(x.get("min_value", "unknown")),
                type=AssertionStdParameterTypeClass.NUMBER,
            ),
            maxValue=AssertionStdParameterClass(
                value=str(x.get("max_value", "unknown")),
                type=AssertionStdParameterTypeClass.NUMBER,
            ),
        ),
    ),
    "dbt_expectations.expect_column_values_to_be_in_set": AssertionParams(
        scope=DatasetAssertionScopeClass.DATASET_COLUMN,
        operator=AssertionStdOperatorClass.IN,
        aggregation=AssertionStdAggregationClass.IDENTITY,
        parameters=lambda kw_args: AssertionStdParametersClass(
            value=AssertionStdParameterClass(
                value=json.dumps(kw_args.get("value_set")),
                type=AssertionStdParameterTypeClass.SET,
            ),
        ),
    ),
}


def _string_map(input_map: Dict[str, Any]) -> Dict[str, str]:
    return {k: str(v) for k, v in input_map.items()}


def _map_dbt_test_status(
    status: str, test_warnings_are_errors: bool
) -> tuple[str, Optional[str]]:
    """Map a dbt test result status to (AssertionResultType, AssertionResultSeverity).

    dbt test statuses:
      - pass/success: the test ran and found no failing rows.
      - warn: the test ran, found failing rows, and is configured with
        ``severity: warn``. Surfaced as a soft failure.
      - fail: the test ran, found failing rows, and is configured with
        ``severity: error`` (the default). Hard failure.
      - error / runtime error: the test invocation itself could not complete
        (compilation/SQL/infra issue). No pass/fail verdict was produced.

    Severity is only meaningful on FAILURE results.
    """
    if status in ("pass", "success"):
        return AssertionResultTypeClass.SUCCESS, None
    if status in ("error", "runtime error"):
        return AssertionResultTypeClass.ERROR, None
    if status == "warn":
        if test_warnings_are_errors:
            return (
                AssertionResultTypeClass.FAILURE,
                AssertionResultSeverityClass.LOW,
            )
        return AssertionResultTypeClass.SUCCESS, None
    # Anything else (fail, or an unknown non-pass/non-error status) is a hard
    # failure.
    return AssertionResultTypeClass.FAILURE, AssertionResultSeverityClass.HIGH


def _map_dbt_freshness_status(
    status: str, test_warnings_are_errors: bool
) -> tuple[str, Optional[str]]:
    """Map a dbt source freshness status to (AssertionResultType, AssertionResultSeverity).

    Unlike regular dbt tests, freshness ``error`` means the ``error_after``
    threshold was exceeded - the check ran to completion and produced a verdict.
    Only ``runtime error`` indicates the freshness check itself failed to run.
    """
    if status == "pass":
        return AssertionResultTypeClass.SUCCESS, None
    if status == "runtime error":
        return AssertionResultTypeClass.ERROR, None
    if status == "warn":
        if test_warnings_are_errors:
            return (
                AssertionResultTypeClass.FAILURE,
                AssertionResultSeverityClass.LOW,
            )
        return AssertionResultTypeClass.SUCCESS, None
    # Covers "error" (error_after threshold exceeded) and any other unknown
    # non-pass status.
    return AssertionResultTypeClass.FAILURE, AssertionResultSeverityClass.HIGH


def make_custom_assertion_info(
    *,
    entity_urn: str,
    scope: Union[DatasetAssertionScopeClass, str],
    operator: Union[AssertionStdOperatorClass, str],
    aggregation: Union[AssertionStdAggregationClass, str],
    fields: Optional[List[str]] = None,
    native_type: Optional[str] = None,
    parameters: Optional[AssertionStdParametersClass] = None,
    logic: Optional[str] = None,
    native_parameters: Optional[Dict[str, str]] = None,
) -> CustomAssertionInfoClass:
    field_urns = fields or []
    return CustomAssertionInfoClass(
        type="dbt",
        entity=entity_urn,
        field=field_urns[0] if field_urns else None,
        fields=field_urns or None,
        scope=scope,
        operator=operator,
        aggregation=aggregation,
        parameters=parameters,
        nativeType=native_type,
        logic=logic,
        nativeParameters=native_parameters,
    )


# dbt replaces every run of characters outside [0-9a-zA-Z_] in a test argument.
_TEST_ARG_UNSAFE_CHARS = re.compile(r"[^0-9a-zA-Z_]+")
_HASHED_NAME_SUFFIX = r"_[0-9a-f]{32}"
# dbt Cloud reports a generic test's macro as `<namespace>.test_<name>`.
_TEST_MACRO_PREFIX = "test_"


@dataclass(frozen=True)
class _GenericTestNames:
    """The names dbt can give an unnamed generic test.

    dbt names the test ``full_name`` and, when that is 64+ characters, uses
    ``short_name`` (truncated identifier + md5) as its alias.
    """

    short_name: str
    full_name: str


@dataclass(frozen=True)
class _GenericTestTarget:
    """The test type and target name dbt builds a generic test's name from."""

    test_type: str
    target_name: str

    @property
    def identifier(self) -> str:
        return f"{self.test_type}_{self.target_name}"


def _test_arg_str(arg: Any, normalize_datetimes: bool) -> str:
    """How dbt stringifies a test argument when it builds the test name.

    YAML datetimes reach dbt as datetime objects (`str()` gives
    `2024-01-01 00:00:00`), but the manifest stores them in ISO form
    (`2024-01-01T00:00:00`). Converting back is only tried as a fallback, since a
    plain string argument can look the same.
    """
    if normalize_datetimes and isinstance(arg, str) and "T" in arg:
        try:
            return str(datetime.fromisoformat(arg))
        except ValueError:
            pass
    return str(arg)


def _synthesize_generic_test_names(
    target: _GenericTestTarget,
    kw_args: Dict[str, Any],
    normalize_datetimes: bool = False,
) -> _GenericTestNames:
    """Port of dbt-core's synthesize_generic_test_names (parser/generic_test_builders.py)."""
    flat_args: List[str] = []
    for arg_name in sorted(kw_args):
        if arg_name == "model":
            continue
        arg_val = kw_args[arg_name]
        if isinstance(arg_val, dict):
            parts = list(arg_val.values())
        elif isinstance(arg_val, (list, tuple)):
            parts = list(arg_val)
        else:
            parts = [arg_val]
        flat_args.extend(_test_arg_str(part, normalize_datetimes) for part in parts)

    unique = "__".join(_TEST_ARG_UNSAFE_CHARS.sub("_", arg) for arg in flat_args)
    full_name = f"{target.identifier}_{unique}"
    if len(full_name) >= 64:
        digest = hashlib.md5(full_name.encode("utf-8")).hexdigest()
        return _GenericTestNames(
            short_name=f"{target.identifier[:30]}_{digest}", full_name=full_name
        )
    return _GenericTestNames(short_name=full_name, full_name=full_name)


def _generic_test_targets(
    namespace: str, test_name: str, upstream_unique_id: str
) -> List[_GenericTestTarget]:
    """The target(s) dbt may have built the test name from (empty if unknown)."""
    parts = upstream_unique_id.split(".")
    if not test_name or len(parts) < 3:
        return []
    if parts[0] == "source":
        if len(parts) < 4:
            return []
        test_type = f"source_{test_name}"
        target_names = [f"{parts[2]}_{parts[3]}"]
    else:
        test_type = test_name
        # A versioned model's unique id is `model.<pkg>.<name>.v<version>`, where the
        # version may itself contain dots (v1.5); dbt names the target `<name>_v<version>`.
        # dbt node names cannot contain dots, so everything after the name is the version.
        version_part = ".".join(parts[3:])
        if not version_part:
            target_names = [parts[2]]
        else:
            # A test written against an unpinned ref() depends on the latest version
            # but is named after the plain model, so accept both spellings.
            target_names = [f"{parts[2]}_{version_part}", parts[2]]
    if namespace:
        test_type = f"{namespace}_{test_type}"
    return [
        _GenericTestTarget(test_type=test_type, target_name=target_name)
        for target_name in target_names
    ]


@dataclass(frozen=True)
class _CloudTestName:
    namespace: str
    test_name: str


def _cloud_test_name_candidates(qualified_test_name: str) -> List[_CloudTestName]:
    """Namespace/test-name pairs dbt may have used for a dbt Cloud test macro.

    dbt Cloud reports the macro (`dbt_utils.test_expression_is_true`), not the
    test's namespace and name. dbt names a package test `<namespace>_<name>` but a
    root-project test just `<name>`, and the macro alone cannot tell them apart, so
    both are accepted.
    """
    namespace, _, test_name = qualified_test_name.rpartition(".")
    if test_name.startswith(_TEST_MACRO_PREFIX):
        test_name = test_name[len(_TEST_MACRO_PREFIX) :]
    candidates = [_CloudTestName(namespace=namespace, test_name=test_name)]
    if namespace:
        candidates.append(_CloudTestName(namespace="", test_name=test_name))
    return candidates


def _matches_generated_name(
    node_name: str, test_info: DBTTest, upstream_unique_id: str
) -> Optional[bool]:
    if test_info.kw_args:
        namespace, _, test_name = test_info.qualified_test_name.rpartition(".")
        targets = _generic_test_targets(namespace, test_name, upstream_unique_id)
        if not targets:
            return None
        return any(
            node_name in (names.short_name, names.full_name)
            for target in targets
            for names in (
                _synthesize_generic_test_names(target, test_info.kw_args),
                _synthesize_generic_test_names(
                    target, test_info.kw_args, normalize_datetimes=True
                ),
            )
        )

    # dbt Cloud does not expose test kwargs, so only the generated prefix can be
    # checked. A generated full name is always `<identifier>_<args>`.
    targets = [
        target
        for candidate in _cloud_test_name_candidates(test_info.qualified_test_name)
        for target in _generic_test_targets(
            candidate.namespace, candidate.test_name, upstream_unique_id
        )
    ]
    if not targets:
        return None
    return any(
        node_name.startswith(f"{target.identifier}_")
        or re.fullmatch(
            re.escape(target.identifier[:30]) + _HASHED_NAME_SUFFIX, node_name
        )
        for target in targets
    )


def is_custom_test_name(
    node_name: str, test_info: DBTTest, upstream_unique_ids: Sequence[str]
) -> bool:
    """Whether a generic dbt test was given a user-authored `name:` instead of dbt's generated one.

    dbt generates the name from the node under test, which is one of the test's upstreams
    (a relationships test also depends on the referenced model), so the name is treated as
    generated if it matches for any upstream. With test kwargs (dbt Core manifest) the
    generated name is recomputed exactly; dbt Cloud does not expose test kwargs, so there
    only the generated prefix is checked.
    """
    if not test_info.qualified_test_name:
        return False
    matches = [
        _matches_generated_name(node_name, test_info, upstream_unique_id)
        for upstream_unique_id in upstream_unique_ids
    ]
    known = [m for m in matches if m is not None]
    return bool(known) and not any(known)


def _dbt_assertion_source() -> AssertionSourceClass:
    """External dbt test definition. created is for Native assertions and is omitted.

    dbt does not record when a test definition was created. Stamping the
    ingestion clock onto source.created makes an unchanged definition look modified.
    Execution time stays on AssertionRunEvent.
    """
    return AssertionSourceClass(type=AssertionSourceTypeClass.EXTERNAL)


def make_assertion_from_test(
    extra_custom_props: Dict[str, str],
    node: "DBTNode",
    assertion_urn: str,
    upstream_urn: str,
    upstream_unique_ids: Sequence[str] = (),
) -> MetadataChangeProposalWrapper:
    assert node.test_info
    qualified_test_name = node.test_info.qualified_test_name
    column_name = node.test_info.column_name
    kw_args = node.test_info.kw_args

    if qualified_test_name in _DBT_TEST_NAME_TO_ASSERTION_MAP:
        assertion_params = _DBT_TEST_NAME_TO_ASSERTION_MAP[qualified_test_name]
        fields = (
            [mce_builder.make_schema_field_urn(upstream_urn, column_name)]
            if (
                assertion_params.scope == DatasetAssertionScopeClass.DATASET_COLUMN
                and column_name
            )
            else []
        )
        custom_assertion = make_custom_assertion_info(
            entity_urn=upstream_urn,
            scope=assertion_params.scope,
            operator=assertion_params.operator,
            aggregation=assertion_params.aggregation,
            fields=fields,
            native_type=node.name,
            parameters=(
                assertion_params.parameters(kw_args)
                if assertion_params.parameters
                else None
            ),
            logic=(
                assertion_params.logic_fn(kw_args)
                if assertion_params.logic_fn
                else None
            ),
            native_parameters=_string_map(kw_args),
        )
    elif column_name:
        # no match with known test types, column-level test
        fields = [mce_builder.make_schema_field_urn(upstream_urn, column_name)]
        custom_assertion = make_custom_assertion_info(
            entity_urn=upstream_urn,
            scope=DatasetAssertionScopeClass.DATASET_COLUMN,
            operator=AssertionStdOperatorClass._NATIVE_,
            aggregation=AssertionStdAggregationClass._NATIVE_,
            fields=fields,
            native_type=node.name,
            logic=node.compiled_code or node.raw_code,
            native_parameters=_string_map(kw_args),
        )
    else:
        # no match with known test types, default to row-level test
        custom_assertion = make_custom_assertion_info(
            entity_urn=upstream_urn,
            scope=DatasetAssertionScopeClass.DATASET_ROWS,
            operator=AssertionStdOperatorClass._NATIVE_,
            aggregation=AssertionStdAggregationClass._NATIVE_,
            native_type=node.name,
            logic=node.compiled_code or node.raw_code,
            native_parameters=_string_map(kw_args),
        )

    # AssertionInfo.description is the assertion's display name in the UI and in
    # notifications. Only a user-authored dbt test name belongs there; generated names
    # are left out so the UI keeps building a structured title from customAssertion.
    assertion_info = AssertionInfoClass(
        type=AssertionTypeClass.CUSTOM,
        customProperties=extra_custom_props,
        description=(
            node.name
            if is_custom_test_name(node.name, node.test_info, upstream_unique_ids)
            else None
        ),
        source=_dbt_assertion_source(),
        customAssertion=custom_assertion,
    )

    return MetadataChangeProposalWrapper(
        entityUrn=assertion_urn,
        aspect=assertion_info,
    )


@dataclass(frozen=True)
class ExistingAssertionNote:
    """The assertion note currently stored in DataHub.

    ``written_by_ingestion`` is True only when the note carries the system actor and
    this pipeline's name. Any other note (a UI edit, another writer, or one written
    while no pipeline name was set) is left untouched.
    """

    note: AssertionNoteClass
    written_by_ingestion: bool


def make_assertion_note_from_test(
    node: "DBTNode",
    assertion_urn: str,
    existing_note: Optional[ExistingAssertionNote] = None,
) -> Optional[MetadataChangeProposalWrapper]:
    """Emit the dbt test's `description:` as the assertion note, which renders as markdown.

    A note not written by this ingestion is never overwritten. When the description
    is removed from dbt, a note this ingestion wrote is cleared.
    """
    content = (node.description or "").strip()
    if existing_note is None:
        if not content:
            return None
    elif (
        not existing_note.written_by_ingestion or existing_note.note.content == content
    ):
        return None
    return MetadataChangeProposalWrapper(
        entityUrn=assertion_urn,
        aspect=AssertionNoteClass(
            content=content,
            lastModified=AuditStampClass(
                time=int(time.time() * 1000), actor=mce_builder.SYSTEM_ACTOR
            ),
        ),
    )


def make_assertion_result_from_test(
    node: "DBTNode",
    test_result: DBTTestResult,
    assertion_urn: str,
    upstream_urn: str,
    test_warnings_are_errors: bool,
) -> MetadataChangeProposalWrapper:
    result_type, severity = _map_dbt_test_status(
        test_result.status, test_warnings_are_errors
    )
    assertionResult = AssertionRunEventClass(
        timestampMillis=int(test_result.execution_time.timestamp() * 1000.0),
        assertionUrn=assertion_urn,
        asserteeUrn=upstream_urn,
        runId=test_result.invocation_id,
        result=AssertionResultClass(
            type=result_type,
            severity=severity,
            nativeResults=test_result.native_results,
        ),
        status=AssertionRunStatusClass.COMPLETE,
    )

    return MetadataChangeProposalWrapper(
        entityUrn=assertion_urn,
        aspect=assertionResult,
    )


def make_assertion_from_freshness(
    extra_custom_props: Dict[str, str],
    node: "DBTNode",
    assertion_urn: str,
    upstream_urn: str,
) -> MetadataChangeProposalWrapper:
    """Create an AssertionInfo aspect for a dbt freshness test."""
    assert node.freshness_info
    freshness_info = node.freshness_info

    assert (
        freshness_info.error_after or freshness_info.warn_after
    )  # At least one of error_after/warn_after must be set

    custom_props = {**extra_custom_props, "dbt_freshness_test": "true"}
    if freshness_info.warn_after:
        custom_props["warn_after_count"] = str(freshness_info.warn_after.count)
        custom_props["warn_after_period"] = freshness_info.warn_after.period
    if freshness_info.error_after:
        custom_props["error_after_count"] = str(freshness_info.error_after.count)
        custom_props["error_after_period"] = freshness_info.error_after.period

    assertion_info = AssertionInfoClass(
        type=AssertionTypeClass.CUSTOM,
        customProperties=custom_props,
        source=_dbt_assertion_source(),
        customAssertion=CustomAssertionInfoClass(
            type="dbt Freshness", entity=upstream_urn
        ),
    )

    return MetadataChangeProposalWrapper(
        entityUrn=assertion_urn,
        aspect=assertion_info,
    )


def make_assertion_result_from_freshness(
    node: "DBTNode",
    assertion_urn: str,
    upstream_urn: str,
    test_warnings_are_errors: bool,
) -> MetadataChangeProposalWrapper:
    """Create an AssertionRunEvent aspect for a dbt freshness test result"""
    assert node.freshness_info
    freshness_info = node.freshness_info

    result_type, severity = _map_dbt_freshness_status(
        freshness_info.status, test_warnings_are_errors
    )

    native_results = {
        "status": freshness_info.status,
        "max_loaded_at": freshness_info.max_loaded_at.isoformat(),
        "snapshotted_at": freshness_info.snapshotted_at.isoformat(),
        "max_loaded_at_time_ago_in_s": str(freshness_info.max_loaded_at_time_ago_in_s),
    }

    assertion_result = AssertionRunEventClass(
        timestampMillis=int(freshness_info.snapshotted_at.timestamp() * 1000.0),
        assertionUrn=assertion_urn,
        asserteeUrn=upstream_urn,
        runId=freshness_info.invocation_id,
        result=AssertionResultClass(
            type=result_type,
            severity=severity,
            nativeResults=native_results,
        ),
        status=AssertionRunStatusClass.COMPLETE,
    )

    return MetadataChangeProposalWrapper(
        entityUrn=assertion_urn,
        aspect=assertion_result,
    )


_CONTRACT_PREFLIGHT_MARKERS = (
    "enforced contract that failed",
    "assert_columns_equivalent",
)


def is_contract_preflight_failure(message: Optional[str]) -> bool:
    """True when a model error message is dbt's schema-contract preflight."""
    if not message:
        return False
    lower = message.lower()
    return any(marker in lower for marker in _CONTRACT_PREFLIGHT_MARKERS)


class ContractRunClassification(Enum):
    SUCCESS = auto()
    PREFLIGHT_FAILURE = auto()


def classify_contract_model_run(
    status: str, message: Optional[str]
) -> Optional[ContractRunClassification]:
    """Classify a model build for contract AssertionRunEvents.

    ``None`` means the run must not be attributed to the contract (an
    unrelated SQL / warehouse error, including a DDL constraint violation,
    which dbt reports as a generic database error).
    """
    if status == "success":
        return ContractRunClassification.SUCCESS
    if status == "error" and is_contract_preflight_failure(message):
        return ContractRunClassification.PREFLIGHT_FAILURE
    return None


def make_contract_assertion_info(
    *,
    name: Optional[str],
    custom_properties: Dict[str, str],
    custom_assertion: CustomAssertionInfoClass,
) -> AssertionInfoClass:
    """AssertionInfo for a contract-derived assertion.

    As for dbt tests, ``description`` (the display name) is set only from a
    user-authored name, i.e. a named dbt constraint. Otherwise the UI builds
    the title from ``customAssertion``.
    """
    return AssertionInfoClass(
        type=AssertionTypeClass.CUSTOM,
        customProperties=custom_properties,
        source=_dbt_assertion_source(),
        description=name,
        customAssertion=custom_assertion,
    )


def make_assertion_result_from_model(
    *,
    assertion_urn: str,
    assertee_urn: str,
    run_id: str,
    timestamp: datetime,
    result_type: str,
    native_results: Optional[Dict[str, str]] = None,
    external_url: Optional[str] = None,
) -> MetadataChangeProposalWrapper:
    return MetadataChangeProposalWrapper(
        entityUrn=assertion_urn,
        aspect=AssertionRunEventClass(
            timestampMillis=int(timestamp.timestamp() * 1000.0),
            assertionUrn=assertion_urn,
            asserteeUrn=assertee_urn,
            runId=run_id,
            result=AssertionResultClass(
                type=result_type,
                nativeResults=native_results or None,
                externalUrl=external_url,
            ),
            status=AssertionRunStatusClass.COMPLETE,
        ),
    )
