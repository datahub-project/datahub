import hashlib
import json
import logging
import time
from datetime import datetime, timezone
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Optional,
)

from datahub.emitter.mce_builder import (
    make_assertion_source,
    make_assertion_urn,
    make_schema_field_urn,
    make_user_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sqlmesh.base import SqlmeshSourceBase
from datahub.ingestion.source.sqlmesh.compat import (
    SqlmeshModel,
)
from datahub.ingestion.source.sqlmesh.constants import (
    _SQLMESH_AUDIT_MAP,
    AUDIT_KWARG_COLUMNS,
    AUDIT_LOGIC_KWARGS,
    AUDIT_RESULT_METADATA,
    AUDIT_RESULT_RESULTS,
    AUDIT_RUN_ID_PREFIX,
    AUDIT_STATUS_FAIL,
    AUDIT_STATUS_PASS,
    AUDIT_STATUS_SKIP,
    CUSTOM_ASSERTION_TYPE,
    INCIDENT_CUSTOM_TYPE_PREFIX,
    INGEST_ACTOR,
    NATIVE_RESULT_FAILING_ROWS,
    PROP_AGGREGATION,
    PROP_AUDIT,
    PROP_FIELDS,
    PROP_NATIVE_PARAMETERS,
    PROP_OPERATOR,
    PROP_SCOPE,
    _AuditAssertionParams,
)
from datahub.ingestion.source.sqlmesh.models import (
    AuditResultEntry,
    AuditResultsMetadata,
    parse_model_audits,
)
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    AssertionResultClass,
    AssertionResultTypeClass,
    AssertionRunEventClass,
    AssertionRunStatusClass,
    AssertionTypeClass,
    AuditStampClass,
    CustomAssertionInfoClass,
    IncidentInfoClass,
    IncidentSourceClass,
    IncidentSourceTypeClass,
    IncidentStateClass,
    IncidentStatusClass,
    IncidentTypeClass,
    StatusClass,
)

logger = logging.getLogger(__name__)


class AssertionMixin(SqlmeshSourceBase):
    def _extract_audit_columns(self, kw: Dict[str, Any]) -> List[str]:
        col_array = kw.get(AUDIT_KWARG_COLUMNS)
        if col_array is None:
            return []
        try:
            return [
                expr.name
                for expr in col_array.expressions
                if hasattr(expr, "name") and expr.name
            ]
        except Exception:
            # Not fatal — the audit still becomes an assertion, just without
            # per-column targeting — but silently dropping the columns hides a
            # sqlglot shape we don't handle.
            logger.warning(
                "Could not extract audit columns from %r; emitting the audit "
                "without column targets",
                col_array,
                exc_info=True,
            )
            return []

    def _extract_literal_value(self, kw: Dict[str, Any], key: str) -> Optional[str]:
        expr = kw.get(key)
        if expr is None:
            return None
        try:
            return str(expr.this)
        except Exception:
            # A SQLGlot AST shape we don't handle would otherwise silently drop
            # this threshold / bound from the emitted assertion — make the
            # boundary visible instead of returning None quietly.
            logger.warning(
                "Could not extract literal %r from audit kwargs (%r); "
                "the assertion will omit it",
                key,
                expr,
                exc_info=True,
            )
            return None

    def _assertion_urn(self, dataset_urn: str, audit_name: str, suffix: str) -> str:
        raw = f"{dataset_urn}:{audit_name}:{suffix}"
        return make_assertion_urn(hashlib.md5(raw.encode()).hexdigest())

    @staticmethod
    def _audit_assertion_suffixes(
        params: Optional[_AuditAssertionParams], columns: List[str]
    ) -> List[str]:
        """The assertion-URN suffixes an audit maps to, given its params and
        column list.

        Single source of truth shared by the definition side
        (``_emit_single_audit``) and the run-event side
        (``_audit_run_events_for_entry``) so both hash to the same assertion
        URN. Diverging here previously pointed run events at a URN no
        definition existed for. The three shapes:

        - unknown / custom audit (``params is None``): one assertion, no column
          targeting → a single empty suffix.
        - column audit (``params.uses_columns``): one assertion per column
          (``[""]`` when the audit names no columns).
        - dataset-level audit: one assertion whose suffix joins all columns.
        """
        if params is None:
            return [""]
        if params.uses_columns:
            return list(columns or [""])
        return [",".join(columns)]

    def _emit_audit_run_events(self, path: str) -> Iterable[MetadataWorkUnit]:
        # Each entry is matched back to an assertion URN using the same
        # deterministic hash as the definition side (model → dataset_urn,
        # audit + columns → suffix), so run events link up automatically.
        try:
            with open(path) as f:
                payload = json.load(f)
        except Exception as e:
            self.report.warning(
                title="Could not read audit results file",
                message="Skipping audit run event emission.",
                context=f"{path}: {e}",
            )
            return

        # A syntactically valid file whose top level is not an object (a list,
        # string, number) would make payload.get(...) raise — warn and skip
        # instead, mirroring the per-entry fail-soft behaviour below.
        if not isinstance(payload, dict):
            self.report.warning(
                title="Audit results file is not a JSON object",
                message="Skipping audit run event emission.",
                context=f"{path}: top-level JSON is {type(payload).__name__}",
            )
            return

        # A malformed metadata block must not abort every result in the file;
        # fall back to an empty generated_at, which the parse below turns into
        # wall-clock time with a warning.
        try:
            generated_at = AuditResultsMetadata.model_validate(
                payload.get(AUDIT_RESULT_METADATA) or {}
            ).generated_at
        except Exception:
            generated_at = ""
        try:
            parsed = datetime.fromisoformat(generated_at)
            # A naive generated_at (no offset) would otherwise be interpreted in
            # the host timezone, making ts_ms — and therefore the derived run_id
            # and incident URNs — depend on where ingestion runs. Assume UTC so
            # the same audit-results file yields stable, reproducible IDs.
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            ts_ms = int(parsed.timestamp() * 1000)
        except Exception as e:
            # run_id and every derived incident URN hang off this timestamp.
            # Falling back to wall-clock makes them change on each run, so a
            # persistently malformed generated_at mints a NEW incident every
            # re-ingest instead of updating the existing one — warn loudly.
            ts_ms = int(time.time() * 1000)
            self.report.warning(
                title="Audit results generated_at is missing or unparseable",
                message="Falling back to wall-clock time; run and incident IDs "
                "will not be stable across re-ingests until this is fixed.",
                context=f"{path}: {generated_at!r} ({e})",
            )

        run_id = f"{AUDIT_RUN_ID_PREFIX}{ts_ms}"
        raw_results: List[Any] = payload.get(AUDIT_RESULT_RESULTS) or []
        emitted = 0

        # Validate each entry inside the loop (not the whole file at once) so a
        # single malformed entry is skipped rather than dropping every result.
        for raw in raw_results:
            try:
                entry = AuditResultEntry.model_validate(raw)
                events = list(self._audit_run_events_for_entry(entry, run_id, ts_ms))
            except Exception as e:
                self.report.warning(
                    title="Could not emit audit run event",
                    message="An entry in the audit results file was skipped.",
                    context=str(raw),
                    exc=e,
                )
                continue
            emitted += len(events)
            yield from events

        logger.info("Emitted %d assertion run events from %s", emitted, path)

    def _audit_run_events_for_entry(
        self, entry: AuditResultEntry, run_id: str, ts_ms: int
    ) -> Iterable[MetadataWorkUnit]:
        # audit/status arrive already lowercased from AuditResultEntry, so the
        # skip check, run-event classification, and incident emission all match
        # their lowercase literals regardless of the file's original casing.
        model_name = entry.model
        audit_name = entry.audit
        columns = entry.columns
        status = entry.status
        failing_rows = entry.failing_rows

        if not model_name or not audit_name or status == AUDIT_STATUS_SKIP:
            return

        dataset_urn = self._sqlmesh_urn_for_audit_result(model_name)
        if dataset_urn is None:
            return

        # Suffixes must match what _emit_single_audit used, so run events land
        # on the assertions whose definitions we already emitted. Both sides
        # derive them from the same helper.
        params = _SQLMESH_AUDIT_MAP.get(audit_name)
        suffixes = self._audit_assertion_suffixes(params, columns)
        for suffix in suffixes:
            assertion_urn = self._assertion_urn(dataset_urn, audit_name, suffix)
            yield self._make_run_event(
                assertion_urn, dataset_urn, run_id, ts_ms, status, failing_rows
            )
            if status == AUDIT_STATUS_FAIL:
                yield from self._emit_incident_for_failure(
                    assertion_urn=assertion_urn,
                    dataset_urn=dataset_urn,
                    run_id=run_id,
                    ts_ms=ts_ms,
                    audit_name=audit_name,
                    failing_rows=failing_rows,
                )

    def _sqlmesh_urn_for_audit_result(self, model_name: str) -> Optional[str]:
        """Resolve the SQLMesh URN an audit result belongs to.

        Prefers the URN cached while the model was emitted: rebuilding it from
        ``_resolved_effective`` uses the *default* gateway's platform instance
        and catalog, which is wrong for any model routed through another
        gateway, and would silently produce run events on a URN no assertion
        definition exists for.
        """
        cached = self._sqlmesh_urn_by_model_key.get(model_name)
        if cached is not None:
            return cached

        effective = self._resolved_effective
        if effective is None:
            self.report.warning(
                title="Skipped audit run events for a model",
                message="No SQLMesh model was ingested in this run, so the audit result cannot be matched to an assertion. Ensure project ingestion succeeds before audit results are read.",
                context=model_name,
            )
            return None

        # Not seen during ingestion: filtered out by model_name_pattern /
        # model_kind_filter, renamed, or named differently in the results file.
        # Fall back to the default gateway's config, which is right for
        # single-gateway projects — the common case.
        normalized = self._build_logical_fqn(model_name, effective)
        fallback = self._sqlmesh_urn_by_model_key.get(normalized)
        if fallback is not None:
            return fallback
        self.report.warning(
            title="Audit result for an un-ingested model",
            message="No assertion definition was emitted for this model, so its run events may not link to anything. Check model_name_pattern / model_kind_filter against the audit results file.",
            context=model_name,
        )
        return self._make_sqlmesh_urn(normalized, effective)

    def _emit_incident_for_failure(
        self,
        *,
        assertion_urn: str,
        dataset_urn: str,
        run_id: str,
        ts_ms: int,
        audit_name: str,
        failing_rows: int,
    ) -> Iterable[MetadataWorkUnit]:
        """Emit a DataHub Incident pointing at the failing dataset + assertion.

        URN is derived deterministically from (assertion_urn, run_id), so
        re-ingesting the same audit results JSON produces the same incident
        URN and updates the existing entity instead of creating a duplicate.

        Incident type is CUSTOM with customType="SQLMESH_AUDIT" because the
        SQLMesh audit set (not_null, unique_values, forall, ...) doesn't
        cleanly map to FRESHNESS / VOLUME / FIELD / DATA_SCHEMA / SQL. The
        full audit name lives in customType so the UI can render it.
        """
        if not self.config.emit_incidents_on_failure:
            return

        incident_id = hashlib.md5(f"{assertion_urn}:{run_id}".encode()).hexdigest()
        incident_urn = f"urn:li:incident:{incident_id}"

        title = f"SQLMesh audit '{audit_name}' failed ({failing_rows} failing rows)"
        description = (
            f"The `{audit_name}` audit on this dataset failed with "
            f"{failing_rows} failing rows in run {run_id}. See the "
            f"associated assertion for details."
        )
        created = AuditStampClass(
            time=ts_ms,
            actor=make_user_urn(INGEST_ACTOR),
        )
        incident_info = IncidentInfoClass(
            type=IncidentTypeClass.CUSTOM,
            customType=f"{INCIDENT_CUSTOM_TYPE_PREFIX}/{audit_name}",
            title=title,
            description=description,
            entities=[dataset_urn],
            status=IncidentStatusClass(
                state=IncidentStateClass.ACTIVE,
                lastUpdated=created,
            ),
            source=IncidentSourceClass(
                type=IncidentSourceTypeClass.ASSERTION_FAILURE,
                sourceUrn=assertion_urn,
            ),
            startedAt=ts_ms,
            created=created,
        )
        # Note: deliberately NOT emitting StatusClass on the incident entity —
        # blue (and likely other OSS GMS deployments) registers IncidentInfo
        # as an aspect on Incident but doesn't accept Status on it, returning
        # HTTP 422 "Unknown aspect status for entity incident". The
        # incidentInfo aspect alone is sufficient to create the entity.
        yield MetadataChangeProposalWrapper(
            entityUrn=incident_urn, aspect=incident_info
        ).as_workunit()

    def _make_run_event(
        self,
        assertion_urn: str,
        dataset_urn: str,
        run_id: str,
        ts_ms: int,
        status: str,
        failing_rows: int,
    ) -> MetadataWorkUnit:
        result_type = (
            AssertionResultTypeClass.SUCCESS
            if status == AUDIT_STATUS_PASS
            else AssertionResultTypeClass.FAILURE
        )
        return MetadataChangeProposalWrapper(
            entityUrn=assertion_urn,
            aspect=AssertionRunEventClass(
                timestampMillis=ts_ms,
                assertionUrn=assertion_urn,
                asserteeUrn=dataset_urn,
                runId=run_id,
                result=AssertionResultClass(
                    type=result_type,
                    nativeResults={NATIVE_RESULT_FAILING_ROWS: str(failing_rows)},
                ),
                status=AssertionRunStatusClass.COMPLETE,
            ),
        ).as_workunit()

    def _emit_assertions(
        self,
        model: "SqlmeshModel",
        sqlmesh_urn: str,
    ) -> Iterable[MetadataWorkUnit]:
        for audit in parse_model_audits(model):
            audit_name = audit.name.lower()
            params = _SQLMESH_AUDIT_MAP.get(audit_name)

            try:
                yield from self._emit_single_audit(
                    audit_name, audit.arguments, params, sqlmesh_urn
                )
            except Exception as e:
                self.report.num_assertions_failed += 1
                self.report.warning(
                    title="Failed to emit assertion",
                    message="An audit could not be converted into a DataHub assertion; data-quality metadata for it is missing.",
                    context=f"{audit_name} on {sqlmesh_urn}",
                    exc=e,
                )

    def _audit_native_parameters(self, kw: Dict[str, Any]) -> Optional[str]:
        """JSON-encode an audit's kwargs as flat key → string pairs.

        SQLMesh hands audit arguments over as SQLGlot expressions whose default
        repr is the whole parse tree, so long values are truncated rather than
        dumped into a custom property.
        """
        rendered: Dict[str, str] = {}
        for key, value in (kw or {}).items():
            text = str(value)
            rendered[str(key)] = text if len(text) <= 200 else text[:200] + "…"
        return json.dumps(rendered, sort_keys=True) if rendered else None

    def _emit_custom_audit(
        self,
        audit_name: str,
        kw: Dict[str, Any],
        params: Optional[_AuditAssertionParams],
        dataset_urn: str,
        *,
        assertion_urn: str,
        field_urn: Optional[str] = None,
        extra_properties: Optional[Dict[str, str]] = None,
    ) -> Iterable[MetadataWorkUnit]:
        """Emit one SQLMesh audit as an ``AssertionTypeClass.CUSTOM`` assertion.

        CUSTOM is the honest type: SQLMesh executes these audits itself as part
        of ``sqlmesh run`` / ``sqlmesh audit``, and DataHub only records the
        definition plus whatever results arrive through ``audit_results_path``.
        Typing them as DATASET or SQL implied DataHub could evaluate them, which
        it can't — and the SQL variant needed a fake ``SELECT 0`` statement to
        satisfy the schema.

        The audit's semantics (scope / operator / aggregation for the built-ins)
        and its arguments are carried as custom properties so the check stays
        inspectable in the UI.
        """
        custom_properties: Dict[str, str] = {PROP_AUDIT: audit_name}
        if params is not None:
            custom_properties[PROP_SCOPE] = params.scope
            custom_properties[PROP_OPERATOR] = params.operator
            custom_properties[PROP_AGGREGATION] = params.aggregation
        native_parameters = self._audit_native_parameters(kw)
        if native_parameters:
            custom_properties[PROP_NATIVE_PARAMETERS] = native_parameters
        if extra_properties:
            custom_properties.update(extra_properties)

        assertion_info = AssertionInfoClass(
            type=AssertionTypeClass.CUSTOM,
            source=make_assertion_source(),
            customProperties=custom_properties,
            description=f"SQLMesh audit '{audit_name}'. Executed by SQLMesh; results are ingested from audit_results_path.",
            customAssertion=CustomAssertionInfoClass(
                type=CUSTOM_ASSERTION_TYPE,
                entity=dataset_urn,
                field=field_urn,
                logic=self._extract_audit_logic(kw),
            ),
        )
        yield MetadataChangeProposalWrapper(
            entityUrn=assertion_urn, aspect=StatusClass(removed=False)
        ).as_workunit()
        yield MetadataChangeProposalWrapper(
            entityUrn=assertion_urn, aspect=assertion_info
        ).as_workunit()

    def _extract_audit_logic(self, kw: Dict[str, Any]) -> Optional[str]:
        """Return the audit's own SQL when SQLMesh exposes it in the kwargs.

        Non-standard audits carry their predicate under ``criteria`` /
        ``condition``. When neither is present we leave ``logic`` unset rather
        than inventing a statement — the authoritative SQL lives in the model
        file.
        """
        for key in AUDIT_LOGIC_KWARGS:
            expr = (kw or {}).get(key)
            if expr is None:
                continue
            try:
                return str(expr.sql())
            except Exception:
                # Fall back to the raw repr, but log it: a SQLGlot API change to
                # .sql() would otherwise degrade audit logic to an opaque repr
                # with no signal.
                logger.warning(
                    "Could not render audit logic via .sql() for %r; "
                    "falling back to its repr",
                    key,
                    exc_info=True,
                )
                return str(expr)
        return None

    def _emit_single_audit(
        self,
        audit_name: str,
        kw: Dict[str, Any],
        params: Optional[_AuditAssertionParams],
        dataset_urn: str,
    ) -> Iterable[MetadataWorkUnit]:
        # Per-audit extra properties (min/max bounds, accepted values, row-count
        # threshold, ...) are declared on _AuditAssertionParams so every audit's
        # semantics live in _SQLMESH_AUDIT_MAP and this method stays generic.
        # Unknown / custom audits (params is None) carry no columns or semantics.
        cols = self._extract_audit_columns(kw) if params is not None else []
        declared_extra = (
            self._audit_declared_properties(kw, params) if params is not None else {}
        )

        # Derive suffixes from the same helper the run-event side uses, so a
        # definition and its run events always hash to the same assertion URN.
        for suffix in self._audit_assertion_suffixes(params, cols):
            field_urn: Optional[str] = None
            extra = dict(declared_extra)
            if params is not None and params.uses_columns:
                # Column-level: the suffix is a single column ("" when none named).
                field_urn = (
                    make_schema_field_urn(dataset_urn, suffix) if suffix else None
                )
            elif params is not None and cols:
                # Dataset-level: record every column the audit covers.
                extra[PROP_FIELDS] = ",".join(cols)

            yield from self._emit_custom_audit(
                audit_name,
                kw,
                params,
                dataset_urn,
                assertion_urn=self._assertion_urn(dataset_urn, audit_name, suffix),
                field_urn=field_urn,
                extra_properties=extra,
            )

    def _audit_declared_properties(
        self, kw: Dict[str, Any], params: _AuditAssertionParams
    ) -> Dict[str, str]:
        """Build the customProperties an audit declares in _SQLMESH_AUDIT_MAP.

        Keeps _emit_single_audit generic: literal kwargs (thresholds, bounds)
        and expression-list kwargs (accepted values) are extracted per the
        audit's declarative spec rather than per-audit ``if`` branches.
        """
        extra: Dict[str, str] = {}
        for kwarg, prop_key in params.literal_props.items():
            value = self._extract_literal_value(kw, kwarg)
            if value is not None:
                extra[prop_key] = value
        for kwarg, prop_key in params.expression_list_props.items():
            values = self._extract_expression_values(kw, kwarg)
            if values:
                extra[prop_key] = ",".join(values)
        return extra

    def _extract_expression_values(self, kw: Dict[str, Any], key: str) -> List[str]:
        expr = kw.get(key)
        if expr is None:
            return []
        try:
            return [str(e.this) for e in expr.expressions]
        except Exception:
            logger.warning(
                "Could not extract %r values from audit kwargs", key, exc_info=True
            )
            return []
