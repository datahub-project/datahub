import time
from functools import partial
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Protocol,
    Sequence,
    Tuple,
    TypeVar,
)

from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.external_dq.config import ExternalDQConfig
from datahub.ingestion.source.external_dq.contract import (
    RESULTS_COLUMNS,
    RULES_COLUMNS,
    ContractColumn,
    LogicalType,
    ResultRow,
    RuleRow,
    parse_result_row,
    parse_rule_row,
)
from datahub.ingestion.source.external_dq.mapper import ExternalDQMapper
from datahub.ingestion.source.external_dq.report import ExternalDQReport
from datahub.ingestion.source.external_dq.state import (
    ExternalDQStateHandler,
    advance,
    plan_window,
    run_key,
)
from datahub.ingestion.source.external_dq.types import TypeProfile, coerce_value
from datahub.ingestion.source.external_dq.validate import PhysicalColumn, validate_table

# (physical column name, logical type — None for extension columns)
SelectColumn = Tuple[str, Optional[LogicalType]]

RowT = TypeVar("RowT", RuleRow, ResultRow)


class ExternalDQReader(Protocol):
    """Platform-specific access to the two contract tables.

    Returned rows are keyed by lower-cased column name. TIMESTAMP values are epoch
    millis (preferred) or datetimes. read_results returns rows with
    executed_at >= since_millis, ordered by (executed_at, run_id) ascending.
    """

    def describe(self, table: str) -> List[PhysicalColumn]: ...

    def read_rules(
        self, table: str, columns: Sequence[SelectColumn]
    ) -> Iterable[Mapping[str, Any]]: ...

    def read_results(
        self, table: str, columns: Sequence[SelectColumn], since_millis: int
    ) -> Iterable[Mapping[str, Any]]: ...


def _wall_clock_millis() -> int:
    return int(time.time() * 1000)


class ExternalDQExtractor:
    def __init__(
        self,
        *,
        config: ExternalDQConfig,
        reader: ExternalDQReader,
        mapper: ExternalDQMapper,
        profile: TypeProfile,
        source_report: SourceReport,
        report: ExternalDQReport,
        state: Optional[ExternalDQStateHandler],
        now_millis: Callable[[], int] = _wall_clock_millis,
    ) -> None:
        self.config = config
        self.reader = reader
        self.mapper = mapper
        self.profile = profile
        self.source_report = source_report
        self.report = report
        self.state = state
        self.now_millis = now_millis
        self._read_failed = False

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        rules_table, results_table = self.config.rules_table, self.config.results_table
        assert rules_table and results_table, "guaranteed by ExternalDQConfig"
        rule_columns = self._validated_columns(rules_table, RULES_COLUMNS)
        result_columns = self._validated_columns(results_table, RESULTS_COLUMNS)
        if rule_columns is None or result_columns is None:
            return

        if self.state is None:
            self.source_report.warning(
                title="External DQ results are re-read every run",
                message="Stateful ingestion is disabled, so results from the last "
                "initial_lookback_days are re-emitted on every run and subscribers may "
                "receive duplicate notifications. Enable stateful_ingestion to read "
                "results incrementally.",
                context=results_table,
            )

        rules: List[RuleRow] = []
        for raw in self._read(
            partial(self.reader.read_rules, rules_table, rule_columns), rules_table
        ):
            self.report.rules_read += 1
            rule = self._parse(raw, RULES_COLUMNS, parse_rule_row, rules_table)
            if rule is None:
                self.report.rules_skipped_invalid += 1
            else:
                rules.append(rule)
        if self._read_failed:
            return
        for mcp in self.mapper.map_rules(rules):
            yield mcp.as_workunit()

        yield from self._result_workunits(results_table, result_columns)

    def _result_workunits(
        self, table: str, columns: Sequence[SelectColumn]
    ) -> Iterable[MetadataWorkUnit]:
        last_watermark, last_recent = (
            self.state.load(table) if self.state else (None, {})
        )
        overlap_ms = self.config.late_arrival_minutes * 60_000
        window = plan_window(
            last_watermark=last_watermark,
            last_recent=last_recent,
            now_millis=self.now_millis(),
            initial_lookback_ms=self.config.initial_lookback_days * 86_400_000,
            overlap_ms=overlap_ms,
        )
        observed: Dict[str, int] = {}
        for raw in self._read(
            partial(self.reader.read_results, table, columns, window.start_millis),
            table,
        ):
            self.report.results_read += 1
            result = self._parse(raw, RESULTS_COLUMNS, parse_result_row, table)
            if result is None:
                self.report.results_skipped_invalid += 1
                continue
            key = run_key(result.rule_id, result.run_id)
            if key in window.seen:
                self.report.results_already_emitted += 1
                continue
            mcp = self.mapper.map_result(result)
            if mcp is None:
                # Unknown rule: not recorded, so it is retried while inside the window.
                continue
            observed[key] = result.executed_at_millis
            yield mcp.as_workunit()

        if self._read_failed or self.state is None:
            return
        watermark, recent = advance(
            last_watermark=last_watermark,
            last_recent=last_recent,
            observed=observed,
            overlap_ms=overlap_ms,
        )
        if watermark is not None:
            self.state.save(table, watermark, recent)

    def _validated_columns(
        self, table: str, contract: Sequence[ContractColumn]
    ) -> Optional[List[SelectColumn]]:
        try:
            physical = self.reader.describe(table)
        except Exception as e:
            self.source_report.failure(
                title="Failed to describe external DQ table",
                message="Could not read the table's column metadata; nothing was ingested from it.",
                context=table,
                exc=e,
            )
            return None
        validation = validate_table(
            physical,
            contract,
            self.profile,
            strict_column_order=self.config.strict_column_order,
        )
        for warning in validation.warnings:
            self.source_report.warning(
                title="External DQ table column order differs from the contract",
                message="Column order does not affect ingestion; set "
                "strict_column_order to enforce it.",
                context=f"{table}: {warning}",
            )
        if validation.errors:
            self.source_report.failure(
                title="External DQ table does not match the contract",
                message="Fix the table schema; nothing was read from this table.",
                context=f"{table}: " + "; ".join(validation.errors),
            )
            return None
        logical_types = {c.name: c.logical_type for c in contract}
        return [
            (column.name, logical_types.get(column.name.lower()))
            for column in sorted(physical, key=lambda c: c.position)
        ]

    def _read(
        self, open_rows: Callable[[], Iterable[Mapping[str, Any]]], table: str
    ) -> Iterator[Mapping[str, Any]]:
        # yield stays outside the try, so only reader errors are caught here.
        try:
            iterator = iter(open_rows())
        except Exception as e:
            self._report_read_failure(table, e)
            return
        while True:
            try:
                row = next(iterator)
            except StopIteration:
                return
            except Exception as e:
                self._report_read_failure(table, e)
                return
            yield row

    def _report_read_failure(self, table: str, error: Exception) -> None:
        self._read_failed = True
        self.source_report.failure(
            title="Failed to read external DQ table",
            message="Reading stopped early; the results checkpoint was not advanced, "
            "so the next run retries.",
            context=table,
            exc=error,
        )

    def _parse(
        self,
        raw: Mapping[str, Any],
        contract: Sequence[ContractColumn],
        parser: Callable[[Mapping[str, Any], Mapping[str, str]], RowT],
        table: str,
    ) -> Optional[RowT]:
        names = {c.name for c in contract}
        try:
            values = {
                c.name: coerce_value(raw.get(c.name), c.logical_type) for c in contract
            }
            extras = {
                k: str(v) for k, v in raw.items() if k not in names and v is not None
            }
            return parser(values, extras)
        except (ValueError, TypeError) as e:
            self.source_report.warning(
                title="Skipped invalid external DQ row",
                message="The row does not satisfy the external DQ table contract.",
                context=f"{table}: rule_id={raw.get('rule_id')!r} run_id={raw.get('run_id')!r}",
                exc=e,
            )
            return None
