import glob
import json
import logging
import os
import pathlib
from dataclasses import dataclass, field
from functools import partial
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
from datahub.ingestion.api.source import (
    MetadataWorkUnitProcessor,
    SourceCapability,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.odcs.odcs_config import ODCS_PLATFORM, ODCSSourceConfig
from datahub.ingestion.source.odcs.odcs_mapper import (
    odcs_platform_info_mcp,
    odcs_to_assertion_mcps,
    odcs_to_logical_dataset_mcps,
    odcs_to_logical_parent_mcp,
    odcs_to_physical_bindings,
    odcs_to_schema_assertion_mcps,
)
from datahub.ingestion.source.odcs.odcs_models import (
    KNOWN_UNMAPPED_AUTHDEF_FIELDS,
    KNOWN_UNMAPPED_CONTRACT_FIELDS,
    KNOWN_UNMAPPED_PROPERTY_FIELDS,
    KNOWN_UNMAPPED_QUALITY_FIELDS,
    KNOWN_UNMAPPED_SCHEMA_FIELDS,
    KNOWN_UNMAPPED_SERVER_FIELDS,
    KNOWN_UNMAPPED_TEAM_FIELDS,
    KNOWN_UNMAPPED_TEAM_MEMBER_FIELDS,
    ODCSAuthoritativeDefinition,
    ODCSContract,
    ODCSProperty,
    ODCSQualityRule,
    ODCSSchemaObject,
    ODCSServer,
    ODCSTeam,
    ODCSTeamMember,
)
from datahub.ingestion.source.state.entity_removal_state import GenericCheckpointState
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    StaleEntityRemovalSourceReport,
    auto_stale_entity_removal,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.ingestion.workunit_processors.auto_stale_entity_removal import (
    AutoStaleEntityRemovalProcessor,
)
from datahub.metadata.schema_classes import LogicalParentClass, OwnershipClass

logger = logging.getLogger(__name__)

_SCHEMA_DIR = pathlib.Path(__file__).parent / "odcs_schema"
_VERSION_TO_SCHEMA = {
    "3.0.0": "odcs-v3.0.2.json",
    "3.0.1": "odcs-v3.0.2.json",
    "3.0.2": "odcs-v3.0.2.json",
    "3.1.0": "odcs-v3.1.0.json",
}
_DEFAULT_VERSION = "3.1.0"

_MODEL_FIELDS_CACHE: Dict[type, Set[str]] = {}

# Spec-valid-but-unmapped field sets per model, for the unknown-field walker.
_KNOWN_UNMAPPED_BY_MODEL: Dict[type, frozenset] = {
    ODCSContract: KNOWN_UNMAPPED_CONTRACT_FIELDS,
    ODCSSchemaObject: KNOWN_UNMAPPED_SCHEMA_FIELDS,
    ODCSProperty: KNOWN_UNMAPPED_PROPERTY_FIELDS,
    ODCSQualityRule: KNOWN_UNMAPPED_QUALITY_FIELDS,
    ODCSServer: KNOWN_UNMAPPED_SERVER_FIELDS,
    ODCSTeam: KNOWN_UNMAPPED_TEAM_FIELDS,
    ODCSTeamMember: KNOWN_UNMAPPED_TEAM_MEMBER_FIELDS,
    ODCSAuthoritativeDefinition: KNOWN_UNMAPPED_AUTHDEF_FIELDS,
}

# Which list-valued keys of each model recurse into which child model, for the
# unknown-field walker. `authoritativeDefinitions` is handled generically, and
# a dict-valued `team` (the v3.1 canonical object form) is special-cased in the
# walker itself.
_CHILD_MODEL_BY_KEY: Dict[type, Dict[str, type]] = {
    ODCSContract: {
        "schema": ODCSSchemaObject,
        "servers": ODCSServer,
        "team": ODCSTeamMember,
    },
    ODCSTeam: {"members": ODCSTeamMember},
    ODCSSchemaObject: {"properties": ODCSProperty, "quality": ODCSQualityRule},
    ODCSProperty: {"properties": ODCSProperty, "quality": ODCSQualityRule},
}


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
class ODCSSourceReport(StaleEntityRemovalSourceReport):
    contracts_scanned: int = 0
    contracts_parsed: int = 0
    contracts_skipped: int = 0
    logical_datasets_emitted: int = 0
    physical_bindings_resolved: int = 0
    logical_parents_emitted: int = 0
    assertions_emitted: int = 0
    schema_assertions_emitted: int = 0
    unknown_fields_count: int = 0
    validation_errors: int = 0
    unmappable_servers: int = 0
    physical_urns_verified: int = 0
    physical_urns_unverified: int = 0
    physical_urns_link_conflicts: int = 0
    physical_names_passthrough: int = 0
    owners_unresolved: int = 0
    files_skipped: List[str] = field(default_factory=list)
    rules_skipped_no_threshold: List[str] = field(default_factory=list)
    rules_routed_to_custom: List[str] = field(default_factory=list)
    schema_type_fallbacks: List[str] = field(default_factory=list)
    spec_fields_ignored: List[str] = field(default_factory=list)


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
    "Via standard stateful ingestion (`stateful_ingestion.remove_stale_metadata`): only "
    "the logical `odcs` Datasets and Assertions ODCS owns are stale-removed; physical "
    "datasets and their `logicalParent` links are never marked removed.",
)
class ODCSSource(StatefulIngestionSourceBase):
    """Ingest Open Data Contract Standard (ODCS) v3.x YAML documents as logical models.

    ODCS v3 describes a producer-published dataset specification, so each
    `schema[]` entry is materialized as a **logical dataset** on the `odcs`
    platform (DatasetProperties / SchemaMetadata / GlobalTags / Ownership /
    InstitutionalMemory), with one Assertion per `quality[]` rule and one
    schema-compliance assertion — all targeting the logical dataset. When a
    physical dataset can be resolved from the contract's `servers[]`, the
    source also links it with `logicalParent` (the `PhysicalInstanceOf`
    relationship); propagation of the assertions onto physical datasets is
    handled by DataHub, not by this source.
    """

    platform = ODCS_PLATFORM

    def __init__(self, config: ODCSSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)  # type: ignore[arg-type]
        self.config = config
        self.report: ODCSSourceReport = ODCSSourceReport()
        self._validators = self._load_validators()
        # Stale-entity removal via standard stateful ingestion. Ownership is
        # scoped per-workunit: aspects emitted onto entities ODCS does not own
        # (the logicalParent link on physical datasets, the platform-info
        # aspect) are marked is_primary_source=False and are never removed.
        self.stale_entity_removal_handler = StaleEntityRemovalHandler(
            state_provider=self.state_provider,
            report=self.report,
            config=self.config,
            state_type_class=GenericCheckpointState,
            pipeline_name=ctx.pipeline_name,
            run_id=ctx.run_id,
            platform=self.platform,
        )
        # Logical `odcs` dataset URNs emitted this run, to detect collisions when
        # two contracts resolve to the same {contract_id}.{schema_name}.
        self._seen_logical_urns: Set[str] = set()
        # Physical URNs already claimed by a logicalParent link this run:
        # logicalParent is single-valued, so a second contract binding the same
        # physical dataset silently overwrites the first (last-writer-wins).
        self._seen_physical_urns: Dict[str, str] = {}
        # Per-run cache for graph existence checks (one lookup per unique URN).
        self._urn_exists_cache: Dict[str, bool] = {}
        # Per-run cache of the persisted `logicalParent` target already on each
        # physical URN (one get_aspect per unique URN), for cross-run conflict
        # detection. `None` means "no existing link" (or lookup unavailable).
        self._physical_parent_cache: Dict[str, Optional[str]] = {}
        # Owner URNs already warned about (report-only resolution check).
        self._owners_warned: Set[str] = set()

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "ODCSSource":
        config = ODCSSourceConfig.model_validate(config_dict)
        return cls(config=config, ctx=ctx)

    def get_excluded_workunit_processors(self):
        # Stale removal is wired manually below so the handler instance is
        # shared with this source; exclude the framework's automatic processor.
        return [AutoStaleEntityRemovalProcessor]

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            partial(auto_stale_entity_removal, self.stale_entity_removal_handler),
        ]

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
        """Walk the raw YAML dict and classify keys not declared on the model.

        Three-way classification per key:
          1. Declared on the model — parsed; recurse into known list shapes.
          2. Spec-valid but unmapped (`KNOWN_UNMAPPED_*`) — collected and
             reported once per file as an info, NOT a warning: the field is
             legitimate ODCS that this source deliberately does not map.
          3. Genuinely unknown — a per-field warning. Pydantic's
             `extra="ignore"` silently drops these, and a silent drop of a
             typo'd field is a footgun.
        """
        spec_ignored: List[str] = []

        def walk(node: Any, model_cls: type[BaseModel], path_hint: str) -> None:
            if not isinstance(node, dict):
                return
            allowed = _model_field_keys(model_cls)
            known_unmapped = _KNOWN_UNMAPPED_BY_MODEL.get(model_cls, frozenset())
            child_models = _CHILD_MODEL_BY_KEY.get(model_cls, {})
            for key, value in node.items():
                if key not in allowed:
                    if key in known_unmapped:
                        spec_ignored.append(f"{path_hint}.{key}")
                        continue
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
                if (
                    model_cls is ODCSContract
                    and key == "team"
                    and isinstance(value, dict)
                ):
                    # v3.1 canonical team object; the list form is handled by
                    # the generic recursion below.
                    walk(value, ODCSTeam, f"{path_hint}.team")
                    continue
                child_cls = (
                    ODCSAuthoritativeDefinition
                    if key == "authoritativeDefinitions"
                    else child_models.get(key)
                )
                if child_cls is not None and isinstance(value, list):
                    for i, item in enumerate(value):
                        walk(item, child_cls, f"{path_hint}.{key}[{i}]")

        walk(raw_dict, ODCSContract, "<root>")
        if spec_ignored:
            self.report.spec_fields_ignored.extend(spec_ignored)
            self.report.info(
                title="ODCS contract uses spec fields DataHub does not map",
                message=(
                    "These fields are valid ODCS but are not mapped to DataHub "
                    "aspects by this source (e.g. SLA, support, pricing, "
                    "relationships). They are ignored."
                ),
                context=f"{file_path}: {', '.join(spec_ignored)}",
            )

    # ------------------------------------------------------------------
    # Per-file processing
    # ------------------------------------------------------------------

    def _urn_exists_in_graph(self, urn: str) -> Optional[bool]:
        """Best-effort, cached existence check for any URN.

        Returns None when no graph is attached (file sink). Lookup errors fail
        OPEN (return True) so a flaky GMS never changes emission behavior.
        One lookup per unique URN per run.
        """
        graph = self.ctx.graph
        if graph is None:
            return None
        cached = self._urn_exists_cache.get(urn)
        if cached is not None:
            return cached
        try:
            exists = bool(graph.exists(urn))
        except Exception as e:
            self.report.warning(
                title="Could not verify URN existence",
                message="Graph lookup failed; assuming the entity exists (fail-open).",
                context=urn,
                exc=e,
            )
            exists = True
        self._urn_exists_cache[urn] = exists
        return exists

    def _physical_urn_exists(self, urn: str) -> bool:
        exists = self._urn_exists_in_graph(urn)
        if exists is None:
            # No graph: emit without verification.
            return True
        if exists:
            self.report.physical_urns_verified += 1
        return exists

    def _existing_logical_parent(self, physical_urn: str) -> Optional[str]:
        """The logical URN a physical dataset is *already* linked to, if any.

        Best-effort, cached graph read of the persisted `logicalParent` aspect.
        Returns None when no graph is attached, when there is no existing link,
        or when the lookup fails — conflict detection is advisory and must
        never abort or alter emission.
        """
        graph = self.ctx.graph
        if graph is None:
            return None
        if physical_urn in self._physical_parent_cache:
            return self._physical_parent_cache[physical_urn]
        target: Optional[str] = None
        try:
            existing = graph.get_aspect(physical_urn, LogicalParentClass)
        except Exception:
            # Advisory-only: a flaky read must not change behavior or spam the
            # report (the existence check already surfaces graph errors).
            existing = None
        if isinstance(existing, LogicalParentClass) and existing.parent is not None:
            target = existing.parent.destinationUrn
        self._physical_parent_cache[physical_urn] = target
        return target

    def _check_owner_resolution(
        self, mcps: List[MetadataChangeProposalWrapper], file_path: pathlib.Path
    ) -> None:
        """Report-only: warn once per owner URN that does not resolve to an
        existing DataHub user/group. Owners are never dropped — contracts are
        routinely ingested before identity sync, and the reference becomes
        functional as soon as the user is provisioned.
        """
        for mcp in mcps:
            aspect = mcp.aspect
            if not isinstance(aspect, OwnershipClass):
                continue
            for owner in aspect.owners:
                if owner.owner in self._owners_warned:
                    continue
                if self._urn_exists_in_graph(owner.owner) is False:
                    self._owners_warned.add(owner.owner)
                    self.report.owners_unresolved += 1
                    self.report.warning(
                        title="Contract owner not found in DataHub",
                        message=(
                            "The ownership reference was emitted anyway and will "
                            "resolve once the user is provisioned. If your "
                            "identity source keys users differently, see "
                            "strip_owner_email_domain / owner_email_domain."
                        ),
                        context=f"file={file_path} owner={owner.owner}",
                    )

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

        overrides = self.config.physical_urn_overrides.get(contract.id)
        if overrides:
            schema_names = {entry.name for entry in contract.schema_ or []}
            unmatched = sorted(set(overrides) - schema_names)
            if unmatched:
                self.report.warning(
                    title="physical_urn_overrides keys match no schema entry",
                    message=(
                        "These override keys do not name any schema[] entry in "
                        "the contract — check for typos or renamed entries."
                    ),
                    context=f"file={file_path} contract={contract.id} keys={unmatched}",
                )

        bindings = odcs_to_physical_bindings(contract, self.config)
        if not bindings:
            # No schema[] entries — nothing to emit.
            self.report.contracts_skipped += 1
            return

        self.report.contracts_parsed += 1
        source_file = file_path.name
        yield from self._emit_bindings(contract, bindings, file_path, source_file)

    def _emit_bindings(
        self,
        contract: ODCSContract,
        bindings: list,
        file_path: pathlib.Path,
        source_file: str,
    ) -> Iterable[MetadataWorkUnit]:
        for binding in bindings:
            schema_entry = binding.schema_entry
            logical_urn = binding.logical_urn
            self.report.logical_datasets_emitted += 1

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
                strip_owner_email_domain=self.config.strip_owner_email_domain,
                owner_email_domain=self.config.owner_email_domain,
            )
            self._check_owner_resolution(logical_mcps, file_path)
            for mcp in logical_mcps:
                yield mcp.as_workunit()
            for unmapped in unmapped_types:
                self.report.schema_type_fallbacks.append(
                    f"{contract.id}/{schema_entry.name}: {unmapped}"
                )

            # Assertions target the LOGICAL dataset and are emitted whether or
            # not a physical binding resolved. Propagation of these
            # expectations onto bound physical datasets is handled by a
            # separate DataHub mechanism via the PhysicalInstanceOf link.
            if self.config.emit_assertions:
                assertion_urns, assertion_mcps, trace = odcs_to_assertion_mcps(
                    contract=contract,
                    schema_entry=schema_entry,
                    logical_urn=logical_urn,
                )
                self.report.rules_routed_to_custom.extend(trace.routed_to_custom)
                if trace.deprecated_rule_key:
                    self.report.info(
                        title="ODCS quality rules use the deprecated `rule` key",
                        message=(
                            "This v3.1 contract names library rules via the "
                            "deprecated `rule` key; use `metric` instead."
                        ),
                        context=(
                            f"file={file_path} schema={schema_entry.name} "
                            f"rules={', '.join(trace.deprecated_rule_key)}"
                        ),
                    )
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
                self.report.assertions_emitted += len(assertion_urns)
                for mcp in assertion_mcps:
                    yield mcp.as_workunit()

            if self.config.emit_schema_assertion:
                schema_assertion_urn, schema_assertion_mcps = (
                    odcs_to_schema_assertion_mcps(
                        contract=contract,
                        schema_entry=schema_entry,
                        logical_urn=logical_urn,
                        compatibility=self.config.schema_assertion_compatibility,
                    )
                )
                if schema_assertion_urn is not None:
                    self.report.schema_assertions_emitted += 1
                    for mcp in schema_assertion_mcps:
                        yield mcp.as_workunit()

            if binding.physical_urn is None:
                # No binding costs only the logicalParent link — never the
                # assertions. The unmapped reason is informational.
                if binding.unmapped_reason:
                    self.report.unmappable_servers += 1
                    self.report.info(
                        title="ODCS schema entry has no physical binding",
                        message=(
                            f"{binding.unmapped_reason}. Emitted the logical "
                            "dataset and its assertions; no logicalParent link."
                        ),
                        context=(
                            f"file={file_path} schema_index={binding.index} "
                            f"schema_name={schema_entry.name}"
                        ),
                    )
                continue

            physical_urn = binding.physical_urn
            if binding.name_passthrough:
                self.report.physical_names_passthrough += 1

            if self.config.verify_physical_urns_exist and not self._physical_urn_exists(
                physical_urn
            ):
                self.report.physical_urns_unverified += 1
                self.report.warning(
                    title="Derived physical dataset not found in DataHub",
                    message=(
                        "The physical URN derived from the contract's servers[] "
                        "does not exist in DataHub, so no logicalParent link was "
                        "emitted (this avoids creating a stub dataset). Ingest "
                        "the physical platform first, fix the derived name via "
                        "physical_urn_overrides, or set "
                        "verify_physical_urns_exist=false to emit optimistically."
                    ),
                    context=(
                        f"file={file_path} schema_name={schema_entry.name} "
                        f"urn={physical_urn}"
                    ),
                )
                continue

            self.report.physical_bindings_resolved += 1

            prior_claim = self._seen_physical_urns.get(physical_urn)
            if prior_claim is not None:
                self.report.warning(
                    title="Physical dataset bound by multiple ODCS schema entries",
                    message=(
                        "logicalParent is single-valued; the last writer wins "
                        f"(already bound by {prior_claim})."
                    ),
                    context=f"file={file_path} urn={physical_urn}",
                )
            self._seen_physical_urns[physical_urn] = (
                f"{contract.id}/{schema_entry.name}"
            )

            if self.config.emit_logical_parent:
                # `logicalParent` is single-valued and physical URNs are kept out
                # of the stateful checkpoint, so an in-run collision
                # (last-writer-wins) is caught above via _seen_physical_urns, but
                # a link written by a *prior* run to a different contract would
                # otherwise be overwritten with no signal. Read the persisted
                # link and warn on a genuine cross-run conflict.
                existing_parent = self._existing_logical_parent(physical_urn)
                if existing_parent is not None and existing_parent != logical_urn:
                    self.report.physical_urns_link_conflicts += 1
                    self.report.warning(
                        title="Physical dataset already linked to a different ODCS contract",
                        message=(
                            "logicalParent is single-valued, so this run's link "
                            "overwrites one written by a previous run for a "
                            "different logical ODCS dataset. In DataHub a physical "
                            "table carries exactly one ODCS contract link; if "
                            "multiple contracts legitimately describe this table, "
                            "keep only one bound (e.g. via physical_urn_overrides) "
                            "until multi-contract support lands."
                        ),
                        context=(
                            f"file={file_path} urn={physical_urn} "
                            f"existing={existing_parent} new={logical_urn}"
                        ),
                    )
                # is_primary_source=False: the physical dataset belongs to its
                # platform-of-record source. This keeps the URN out of ODCS's
                # stateful-ingestion checkpoint so stale-entity removal can
                # never soft-delete a physical dataset.
                yield odcs_to_logical_parent_mcp(physical_urn, logical_urn).as_workunit(
                    is_primary_source=False
                )
                self.report.logical_parents_emitted += 1

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        # Emit the odcs platform aspect once per run. Also registered at GMS
        # boot (PR #17332); this is the version-independent fallback, and its
        # fields must stay in sync with that entry -- see odcs_platform_info_mcp().
        # is_primary_source=False keeps the platform entity out of the
        # stateful-ingestion checkpoint.
        yield odcs_platform_info_mcp().as_workunit(is_primary_source=False)

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
        # nothing bound to a physical dataset. Assertions are unaffected (they
        # target the logical dataset), but no logicalParent links were produced,
        # and the logical `odcs` datasets only render once the
        # LOGICAL_MODELS_ENABLED feature flag is enabled (off by default in OSS),
        # so the run can otherwise look like it did nothing.
        if (
            self.report.logical_datasets_emitted > 0
            and self.report.physical_bindings_resolved == 0
        ):
            self.report.info(
                title="ODCS resolved no physical bindings",
                message=(
                    f"{self.report.logical_datasets_emitted} logical `odcs` dataset(s) "
                    "were emitted (with their assertions), but none bound to a "
                    "physical dataset, so no logicalParent links were produced. "
                    "Logical Models also require the LOGICAL_MODELS_ENABLED feature "
                    "flag (off by default) to render in the UI. Declare typed "
                    "`servers[]` entries in the contract, or configure "
                    "`server_overrides` / `physical_urn_overrides`, to link physical "
                    "datasets."
                ),
            )

    def get_report(self) -> ODCSSourceReport:
        return self.report
