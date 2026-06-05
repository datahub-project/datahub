import glob
import json
import logging
import os
import pathlib
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Set

import jsonschema
import yaml
from pydantic import BaseModel, ValidationError

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import Source, SourceCapability, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.odcs.odcs_config import ODCS_PLATFORM, ODCSSourceConfig
from datahub.ingestion.source.odcs.odcs_mapper import (
    odcs_platform_info_mcp,
    odcs_to_assertion_mcps,
    odcs_to_logical_dataset_mcps,
    odcs_to_logical_parent_mcp,
    odcs_to_physical_bindings,
)
from datahub.ingestion.source.odcs.odcs_models import (
    ODCSContract,
    ODCSProperty,
    ODCSQualityRule,
    ODCSSchemaObject,
    ODCSServer,
    ODCSTeamMember,
)
from datahub.metadata.schema_classes import StatusClass

logger = logging.getLogger(__name__)

_SCHEMA_DIR = pathlib.Path(__file__).parent / "odcs_schema"
_VERSION_TO_SCHEMA = {
    "3.0.0": "odcs-v3.0.2.json",
    "3.0.1": "odcs-v3.0.2.json",
    "3.0.2": "odcs-v3.0.2.json",
    "3.1.0": "odcs-v3.1.0.json",
}
_DEFAULT_VERSION = "3.1.0"

# URN prefix for the logical datasets ODCS owns. State and soft-delete are
# scoped strictly to these (plus the assertions ODCS emits).
_ODCS_DATASET_PREFIX = f"urn:li:dataset:(urn:li:dataPlatform:{ODCS_PLATFORM},"

_MODEL_FIELDS_CACHE: Dict[type, Set[str]] = {}


def _model_field_keys(model_cls: type[BaseModel]) -> Set[str]:
    """Return the set of keys (including aliases) that a pydantic model accepts."""
    cached = _MODEL_FIELDS_CACHE.get(model_cls)
    if cached is not None:
        return cached
    keys: Set[str] = set()
    for fname, finfo in model_cls.model_fields.items():
        keys.add(fname)
        alias = getattr(finfo, "alias", None)
        if alias:
            keys.add(alias)
    _MODEL_FIELDS_CACHE[model_cls] = keys
    return keys


@dataclass
class ODCSSourceReport(SourceReport):
    contracts_scanned: int = 0
    contracts_parsed: int = 0
    contracts_skipped: int = 0
    logical_datasets_emitted: int = 0
    physical_bindings_resolved: int = 0
    logical_parents_emitted: int = 0
    assertions_emitted: int = 0
    aspects_soft_deleted: int = 0
    unknown_fields_count: int = 0
    validation_errors: int = 0
    unmappable_servers: int = 0
    files_skipped: List[str] = field(default_factory=list)
    rules_skipped_no_threshold: List[str] = field(default_factory=list)
    rules_routed_to_custom: List[str] = field(default_factory=list)
    schema_type_fallbacks: List[str] = field(default_factory=list)


@platform_name("Open Data Contract Standard", id="odcs")
@config_class(ODCSSourceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(
    SourceCapability.SCHEMA_METADATA,
    "Canonical schema (types, descriptions, keys) from `schema[].properties[]`.",
)
@capability(SourceCapability.OWNERSHIP, "Owners derived from `team[]`.")
@capability(SourceCapability.TAGS, "Top-level and column-level `tags`.")
@capability(SourceCapability.DESCRIPTIONS, "Dataset and column descriptions.")
@capability(
    SourceCapability.DELETION_DETECTION,
    "ODCS soft-deletes only the logical `odcs` Datasets and Assertions it owns; "
    "physical datasets and their `logicalParent` links are never marked removed.",
)
class ODCSSource(Source):
    """Ingest Open Data Contract Standard (ODCS) v3.x YAML documents as logical models.

    ODCS v3 describes a producer-published dataset specification, so each
    `schema[]` entry is materialized as a **logical dataset** on the `odcs`
    platform (DatasetProperties / SchemaMetadata / GlobalTags / Ownership /
    InstitutionalMemory). When a physical dataset can be resolved, the source
    also links it with `logicalParent` (the `PhysicalInstanceOf` relationship)
    and emits one Assertion per `quality[]` rule against the physical dataset.
    """

    def __init__(self, config: ODCSSourceConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.config = config
        self.report = ODCSSourceReport()
        self._validators = self._load_validators()
        # URNs ODCS emitted on this run, indexed by source-file. Persisted
        # across runs (when state_file_path is set). Only contains logical
        # `odcs` Dataset and Assertion URNs — never physical datasets.
        self._current_state: Dict[str, Dict[str, List[str]]] = {}
        self._prior_state: Dict[str, Dict[str, List[str]]] = self._load_prior_state()
        # Logical `odcs` dataset URNs emitted this run, to detect collisions when
        # two contracts resolve to the same {contract_id}.{schema_name}.
        self._seen_logical_urns: Set[str] = set()

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "ODCSSource":
        config = ODCSSourceConfig.model_validate(config_dict)
        return cls(config=config, ctx=ctx)

    # ------------------------------------------------------------------
    # State management for scoped soft-delete
    # ------------------------------------------------------------------

    def _state_path(self) -> Optional[pathlib.Path]:
        if not self.config.state_file_path:
            return None
        return pathlib.Path(os.path.expanduser(self.config.state_file_path))

    def _load_prior_state(self) -> Dict[str, Dict[str, List[str]]]:
        """Load `{file_path: {"datasets": [urns], "assertions": [urns]}}` from disk.

        First-run-no-state, missing-file, and corrupt-JSON all degrade to "no
        prior state" (empty dict) with a warning so soft-delete becomes a
        no-op rather than crashing the source.
        """
        path = self._state_path()
        if path is None or not path.exists():
            return {}
        try:
            with open(path) as fp:
                raw = json.load(fp)
        except (OSError, json.JSONDecodeError) as e:
            self.report.warning(
                title="Could not read ODCS state file",
                message=(
                    "Failed to load prior-run state; soft-delete will be a no-op "
                    "for this run. The state file will be rewritten at end-of-run."
                ),
                context=str(path),
                exc=e,
            )
            return {}
        if not isinstance(raw, dict):
            return {}
        # Filter aggressively to entity types ODCS owns. Refuse to load any URN
        # that is not a logical `odcs` dataset or an assertion — physical
        # datasets are out of scope for ODCS soft-delete.
        cleaned: Dict[str, Dict[str, List[str]]] = {}
        for file_key, payload in raw.items():
            if not isinstance(payload, dict):
                continue
            datasets = [
                u
                for u in payload.get("datasets", [])
                if isinstance(u, str) and u.startswith(_ODCS_DATASET_PREFIX)
            ]
            assertions = [
                u
                for u in payload.get("assertions", [])
                if isinstance(u, str) and u.startswith("urn:li:assertion:")
            ]
            cleaned[file_key] = {"datasets": datasets, "assertions": assertions}
        return cleaned

    def _persist_state(self) -> None:
        path = self._state_path()
        if path is None:
            return
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, "w") as fp:
                json.dump(self._current_state, fp, indent=2, sort_keys=True)
        except OSError as e:
            self.report.warning(
                title="Could not write ODCS state file",
                message=(
                    "End-of-run state could not be persisted; next run will not "
                    "be able to soft-delete URNs that fell out of this run."
                ),
                context=str(path),
                exc=e,
            )

    def _emit_soft_deletes(self) -> Iterable[MetadataWorkUnit]:
        """Diff current vs. prior state and emit Status(removed=True) for fall-outs.

        Scoped strictly to logical `odcs` Dataset + Assertion URNs. The diff is
        per source-file: a logical dataset removed from `file_a.yaml` is only a
        soft-delete candidate if the file was processed this run (or the file
        disappeared entirely — the legitimate "contract removed" case).
        """
        for file_key, prior in self._prior_state.items():
            current = self._current_state.get(
                file_key, {"datasets": [], "assertions": []}
            )
            current_set = set(current.get("datasets", [])) | set(
                current.get("assertions", [])
            )
            for urn in list(prior.get("datasets", [])) + list(
                prior.get("assertions", [])
            ):
                if urn in current_set:
                    continue
                if not (
                    urn.startswith(_ODCS_DATASET_PREFIX)
                    or urn.startswith("urn:li:assertion:")
                ):
                    continue
                self.report.aspects_soft_deleted += 1
                yield MetadataChangeProposalWrapper(
                    entityUrn=urn,
                    aspect=StatusClass(removed=True),
                ).as_workunit()

    # ------------------------------------------------------------------
    # Schema validation + file IO
    # ------------------------------------------------------------------

    def _load_validators(self) -> dict:
        validators: dict = {}
        for version, filename in _VERSION_TO_SCHEMA.items():
            path = _SCHEMA_DIR / filename
            if not path.exists():
                continue
            with open(path) as fp:
                schema = json.load(fp)
            validators[version] = jsonschema.Draft202012Validator(schema)
        return validators

    def _glob_root(self, pattern: str) -> pathlib.Path:
        """Return the literal directory prefix of a glob pattern (everything
        before the first wildcard segment). Used to enforce containment of
        symlink-resolved targets when `follow_symlinks=True`.
        """
        parts = pathlib.Path(pattern).parts
        prefix: List[str] = []
        for part in parts:
            if any(ch in part for ch in ("*", "?", "[")):
                break
            prefix.append(part)
        if not prefix:
            return pathlib.Path(".")
        return pathlib.Path(*prefix)

    def _is_within(self, child: pathlib.Path, root: pathlib.Path) -> bool:
        """True if `child` (resolved) is inside `root` (resolved)."""
        try:
            child_resolved = child.resolve()
            root_resolved = root.resolve()
        except (OSError, RuntimeError):
            return False
        try:
            common = os.path.commonpath([str(child_resolved), str(root_resolved)])
        except ValueError:
            return False
        return common == str(root_resolved)

    def _accept_symlink_target(
        self, candidate: pathlib.Path, root: Optional[pathlib.Path]
    ) -> bool:
        """Apply the symlink policy to a discovered path.

        Returns True if the candidate should be kept. Emits a SourceReport
        warning and returns False when the candidate is rejected.
        """
        try:
            is_symlink = candidate.is_symlink()
        except OSError:
            return False
        if is_symlink and not self.config.follow_symlinks:
            self.report.warning(
                title="Skipping ODCS symlink",
                message="Symlinks are not followed by default; set follow_symlinks=true to opt in.",
                context=str(candidate),
            )
            return False
        if (
            self.config.follow_symlinks
            and is_symlink
            and root is not None
            and not self._is_within(candidate, root)
        ):
            self.report.warning(
                title="ODCS symlink escapes configured root",
                message="Symlink target resolves outside the configured root; skipping.",
                context=str(candidate),
            )
            return False
        return True

    def _resolve_paths(self) -> Iterable[pathlib.Path]:
        raw_paths = (
            self.config.path
            if isinstance(self.config.path, list)
            else [self.config.path]
        )
        seen: Set[str] = set()
        for raw in raw_paths:
            expanded = os.path.expanduser(raw)
            if any(ch in expanded for ch in ("*", "?", "[")):
                glob_root = self._glob_root(expanded)
                matches = sorted(glob.glob(expanded, recursive=True))
                for m in matches:
                    p = pathlib.Path(m)
                    if not (p.is_file() and self._matches_extension(p)):
                        continue
                    if str(p) in seen:
                        continue
                    if not self._accept_symlink_target(p, glob_root):
                        continue
                    seen.add(str(p))
                    yield p
                continue
            p = pathlib.Path(expanded)
            if p.is_symlink() and not self.config.follow_symlinks:
                self.report.warning(
                    title="Skipping ODCS symlink",
                    message="Configured path is a symlink; set follow_symlinks=true to opt in.",
                    context=str(p),
                )
                continue
            if p.is_file():
                if str(p) not in seen:
                    seen.add(str(p))
                    yield p
            elif p.is_dir():
                root = p
                for child in sorted(p.rglob("*")):
                    if not (child.is_file() and self._matches_extension(child)):
                        continue
                    if str(child) in seen:
                        continue
                    if not self._accept_symlink_target(child, root):
                        continue
                    seen.add(str(child))
                    yield child
            else:
                self.report.warning(
                    title="ODCS path does not exist",
                    message="Configured path was not found",
                    context=str(p),
                )

    def _matches_extension(self, path: pathlib.Path) -> bool:
        name = path.name.lower()
        return any(name.endswith(ext.lower()) for ext in self.config.file_extensions)

    def _validate(self, raw_dict: dict, file_path: pathlib.Path) -> bool:
        api_version = (raw_dict.get("apiVersion") or "").lstrip("v")
        if api_version and api_version not in self.config.odcs_versions:
            self.report.warning(
                title="Unsupported ODCS apiVersion",
                message="Contract apiVersion is not in the configured supported versions",
                context=f"{file_path}: apiVersion={api_version}, supported={self.config.odcs_versions}",
            )
            return False
        validator = self._validators.get(api_version)
        if validator is None:
            validator = self._validators.get(_DEFAULT_VERSION)
        if validator is None:
            return True
        errors = sorted(validator.iter_errors(raw_dict), key=lambda e: e.path)
        if not errors:
            return True
        self.report.validation_errors += len(errors)
        message = "; ".join(
            f"{'/'.join(str(p) for p in err.absolute_path) or '<root>'}: {err.message}"
            for err in errors[:5]
        )
        if self.config.strict_validation:
            self.report.warning(
                title="ODCS contract failed JSON Schema validation",
                message="Skipping contract due to schema validation errors",
                context=f"{file_path}: {message}",
            )
            return False
        self.report.warning(
            title="ODCS contract has JSON Schema validation issues",
            message="Proceeding with parse despite validation errors (strict_validation=False)",
            context=f"{file_path}: {message}",
        )
        return True

    def _load_yaml(self, file_path: pathlib.Path) -> Optional[tuple]:
        try:
            file_size = os.stat(file_path).st_size
        except OSError as e:
            self.report.warning(
                title="Failed to stat ODCS file",
                message="Could not stat file; skipping",
                context=str(file_path),
                exc=e,
            )
            self.report.files_skipped.append(str(file_path))
            return None
        if file_size > self.config.max_input_file_bytes:
            self.report.warning(
                title="ODCS file exceeds max_input_file_bytes",
                message=(
                    f"File size {file_size} bytes exceeds configured limit "
                    f"{self.config.max_input_file_bytes}; skipping. Raise "
                    "max_input_file_bytes to ingest larger files."
                ),
                context=str(file_path),
            )
            self.report.files_skipped.append(str(file_path))
            return None
        try:
            raw_text = file_path.read_text(encoding="utf-8")
            raw_dict = yaml.safe_load(raw_text)
        except (OSError, yaml.YAMLError) as e:
            self.report.warning(
                title="Failed to read ODCS file",
                message="Could not parse YAML; skipping",
                context=str(file_path),
                exc=e,
            )
            self.report.files_skipped.append(str(file_path))
            return None
        if not isinstance(raw_dict, dict):
            self.report.warning(
                title="ODCS file is not a YAML object",
                message="Top-level YAML must be a mapping; skipping",
                context=str(file_path),
            )
            self.report.files_skipped.append(str(file_path))
            return None
        return raw_text, raw_dict

    # ------------------------------------------------------------------
    # Unknown-field detection
    # ------------------------------------------------------------------

    def _warn_unknown_fields(self, raw_dict: dict, file_path: pathlib.Path) -> None:
        """Walk the raw YAML dict and warn on any key not declared on the model.

        Pydantic's `extra="ignore"` silently drops these — ODCS files often
        contain typos or version-specific fields, and a silent drop is a
        footgun. One warning per (model, field) pair per file.
        """

        def walk(node: Any, model_cls: type[BaseModel], path_hint: str) -> None:
            if not isinstance(node, dict):
                return
            allowed = _model_field_keys(model_cls)
            for key, value in node.items():
                if key not in allowed:
                    self.report.unknown_fields_count += 1
                    self.report.warning(
                        title="Unknown ODCS field",
                        message=(
                            f"Field '{key}' on {model_cls.__name__} is not "
                            "recognized; check spelling or version compatibility."
                        ),
                        context=f"{file_path}: {path_hint}.{key}",
                    )
                    continue
                if model_cls is ODCSContract:
                    if key == "schema" and isinstance(value, list):
                        for i, item in enumerate(value):
                            walk(item, ODCSSchemaObject, f"{path_hint}.schema[{i}]")
                    elif key == "servers" and isinstance(value, list):
                        for i, item in enumerate(value):
                            walk(item, ODCSServer, f"{path_hint}.servers[{i}]")
                    elif key == "team" and isinstance(value, list):
                        for i, item in enumerate(value):
                            walk(item, ODCSTeamMember, f"{path_hint}.team[{i}]")
                    elif key == "quality" and isinstance(value, list):
                        for i, item in enumerate(value):
                            walk(item, ODCSQualityRule, f"{path_hint}.quality[{i}]")
                elif model_cls is ODCSSchemaObject or model_cls is ODCSProperty:
                    if key == "properties" and isinstance(value, list):
                        for i, item in enumerate(value):
                            walk(item, ODCSProperty, f"{path_hint}.properties[{i}]")
                    elif key == "quality" and isinstance(value, list):
                        for i, item in enumerate(value):
                            walk(item, ODCSQualityRule, f"{path_hint}.quality[{i}]")

        walk(raw_dict, ODCSContract, "<root>")

    # ------------------------------------------------------------------
    # Per-file processing
    # ------------------------------------------------------------------

    def _file_state_key(self, file_path: pathlib.Path) -> str:
        """Stable key for storing per-file state (absolute path, str-form)."""
        try:
            return str(file_path.resolve())
        except OSError:
            return str(file_path)

    def _record_state(
        self,
        file_path: pathlib.Path,
        dataset_urns: List[str],
        assertion_urns: List[str],
    ) -> None:
        key = self._file_state_key(file_path)
        self._current_state[key] = {
            "datasets": list(dataset_urns),
            "assertions": list(assertion_urns),
        }

    def _process_file(self, file_path: pathlib.Path) -> Iterable[MetadataWorkUnit]:
        self.report.contracts_scanned += 1
        loaded = self._load_yaml(file_path)
        if loaded is None:
            self.report.contracts_skipped += 1
            return
        _raw_text, raw_dict = loaded
        if not self._validate(raw_dict, file_path):
            self.report.contracts_skipped += 1
            return

        self._warn_unknown_fields(raw_dict, file_path)

        try:
            contract = ODCSContract.model_validate(raw_dict)
        except ValidationError as e:
            self.report.warning(
                title="ODCS contract failed Pydantic validation",
                message="Could not coerce ODCS document into the expected model; skipping",
                context=f"{file_path}: {e}",
            )
            self.report.contracts_skipped += 1
            return

        bindings = odcs_to_physical_bindings(contract, self.config)
        if not bindings:
            # No schema[] entries — nothing to emit. Record empty state so a
            # prior-run state file with this key gets soft-deleted.
            self._record_state(file_path, [], [])
            self.report.contracts_skipped += 1
            return

        self.report.contracts_parsed += 1
        source_file = file_path.name
        emitted_dataset_urns: List[str] = []
        emitted_assertion_urns: List[str] = []

        # Record state in a finally so that an exception part-way through the
        # binding loop does not leave _current_state without this file's URNs —
        # which would make the next run soft-delete everything from the prior run.
        try:
            yield from self._emit_bindings(
                contract,
                bindings,
                file_path,
                source_file,
                emitted_dataset_urns,
                emitted_assertion_urns,
            )
        finally:
            self._record_state(file_path, emitted_dataset_urns, emitted_assertion_urns)

    def _emit_bindings(
        self,
        contract: ODCSContract,
        bindings: list,
        file_path: pathlib.Path,
        source_file: str,
        emitted_dataset_urns: List[str],
        emitted_assertion_urns: List[str],
    ) -> Iterable[MetadataWorkUnit]:
        for binding in bindings:
            schema_entry = binding.schema_entry
            logical_urn = binding.logical_urn
            self.report.logical_datasets_emitted += 1
            emitted_dataset_urns.append(logical_urn)

            if logical_urn in self._seen_logical_urns:
                self.report.warning(
                    title="Duplicate logical ODCS dataset URN",
                    message=(
                        "Two ODCS schema entries resolved to the same logical "
                        "dataset URN; their aspects collide (last-writer-wins). "
                        "Use distinct contract `id`/schema names or set "
                        "logical_dataset_name_template to include {contract_version}."
                    ),
                    context=f"file={file_path} urn={logical_urn}",
                )
            self._seen_logical_urns.add(logical_urn)

            logical_mcps, unmapped_types = odcs_to_logical_dataset_mcps(
                contract=contract,
                schema_entry=schema_entry,
                logical_urn=logical_urn,
                source_file=source_file,
                tag_prefix=self.config.tag_prefix,
                replicate_contract_metadata=self.config.replicate_contract_metadata,
            )
            for mcp in logical_mcps:
                yield mcp.as_workunit()
            for unmapped in unmapped_types:
                self.report.schema_type_fallbacks.append(
                    f"{contract.id}/{schema_entry.name}: {unmapped}"
                )

            if binding.physical_urn is None:
                # Strict gating: no binding means no logicalParent link and no
                # assertions. The unmapped reason is informational, not an error.
                if binding.unmapped_reason:
                    self.report.unmappable_servers += 1
                    self.report.info(
                        title="ODCS schema entry has no physical binding",
                        message=(
                            f"{binding.unmapped_reason}. Emitted the logical "
                            "dataset only; no logicalParent link or assertions."
                        ),
                        context=(
                            f"file={file_path} schema_index={binding.index} "
                            f"schema_name={schema_entry.name}"
                        ),
                    )
                continue

            self.report.physical_bindings_resolved += 1
            physical_urn = binding.physical_urn

            if self.config.emit_logical_parent:
                yield odcs_to_logical_parent_mcp(
                    physical_urn, logical_urn
                ).as_workunit()
                self.report.logical_parents_emitted += 1

            if self.config.emit_assertions:
                assertion_urns, assertion_mcps, trace = odcs_to_assertion_mcps(
                    contract=contract,
                    schema_entry=schema_entry,
                    physical_urn=physical_urn,
                    logical_urn=logical_urn,
                )
                self.report.rules_routed_to_custom.extend(trace.routed_to_custom)
                for skipped in trace.skipped_no_body:
                    self.report.rules_skipped_no_threshold.append(skipped)
                    self.report.warning(
                        title="ODCS quality rule skipped — no modelable body",
                        message=(
                            "Rule has no operator/threshold and no query / "
                            "implementation / description / name to use as "
                            "custom assertion logic; skipping."
                        ),
                        context=(
                            f"file={file_path} schema={schema_entry.name} "
                            f"rule={skipped}"
                        ),
                    )
                # Record state BEFORE yielding (mirrors the logical-dataset URN
                # above): if a downstream yield raises mid-loop, the finally in
                # _process_file must still see these URNs, or the next run would
                # soft-delete assertions that were already emitted.
                emitted_assertion_urns.extend(assertion_urns)
                for mcp in assertion_mcps:
                    self.report.assertions_emitted += 1
                    yield mcp.as_workunit()

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        # Register the `odcs` platform once per run (no boot-time registry entry).
        yield odcs_platform_info_mcp().as_workunit()

        for file_path in self._resolve_paths():
            try:
                yield from self._process_file(file_path)
            except Exception as e:
                # Per spec, one bad file should not crash the whole ingestion.
                self.report.warning(
                    title="Unhandled error processing ODCS file",
                    message="Skipping file due to unexpected error",
                    context=str(file_path),
                    exc=e,
                )
                self.report.files_skipped.append(str(file_path))
                self.report.contracts_skipped += 1

        # Surface the silent-by-default case: logical datasets were emitted but
        # nothing bound to a physical dataset. With no binding there are no
        # assertions, and the logical `odcs` datasets only render once the
        # LOGICAL_MODELS_ENABLED feature flag is enabled (off by default in OSS),
        # so the run can otherwise look like it did nothing.
        if (
            self.report.logical_datasets_emitted > 0
            and self.report.physical_bindings_resolved == 0
        ):
            self.report.warning(
                title="ODCS emitted logical datasets but resolved no physical bindings",
                message=(
                    f"{self.report.logical_datasets_emitted} logical `odcs` dataset(s) "
                    "were emitted with no physical binding, so no assertions were "
                    "produced. Logical Models also require the LOGICAL_MODELS_ENABLED "
                    "feature flag (off by default) to render in the UI. Configure "
                    "`servers_to_platform` or `physical_urn_overrides` to bind physical "
                    "datasets and emit assertions."
                ),
            )

        # End-of-run: diff prior vs. current state and emit Status(removed=true).
        yield from self._emit_soft_deletes()
        self._persist_state()

    def get_report(self) -> ODCSSourceReport:
        return self.report
