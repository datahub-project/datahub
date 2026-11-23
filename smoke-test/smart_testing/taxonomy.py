"""Typed smart-testing taxonomy and module selection for the smoke-test suite.

The enums below mirror the common vocabulary in
``.github/smart-testing/taxonomy.yaml``; ``tests/unit/test_smart_testing.py`` keeps
the two in sync. Tags from the optional ``taxonomy.overlay.yaml`` (features only
one repository has) are mirrored in ``taxonomy_overlay.py``. Together they are the
only runtime source of known tags, so this module never reads the yaml and works
inside the smoke-test image.

Tests carry tags as one pytest marker per dimension, e.g.

    @pytest.mark.feature_user(FeatureUser.LINEAGE)
    @pytest.mark.compound(Compound.PROPAGATION_DOCS)

or through ``test_tags.yaml`` (plus ``test_tags.overlay.yaml`` when present),
which conftest applies as the same markers.

``change-type`` is the exception: it describes PRs only (CI, build, test
infrastructure, docs, tooling), so no test carries it and it never selects
tests by feature. Its ``pytest-plumbing`` tag adds ``plumbing_sample``.
"""

import fnmatch
import json
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Dict, FrozenSet, Iterable, List, Mapping, Optional, Set, Tuple, Type

import yaml

try:
    from .taxonomy_overlay import (  # type: ignore[import-not-found,unused-ignore]
        OVERLAY_TAGS,
    )
except ImportError:  # no overlay in this repository
    OVERLAY_TAGS = {}

TAG_MAP_PATH = Path(__file__).parent / "test_tags.yaml"
TAG_MAP_OVERLAY_PATH = TAG_MAP_PATH.with_name("test_tags.overlay.yaml")

# Narrowing only kicks in for very broad selections, and is abandoned if it
# would leave fewer than SAFETY_FLOOR modules.
NARROW_THRESHOLD = 200
SAFETY_FLOOR = 20


class Dimension(str, Enum):
    FEATURE_USER = "feature-user"
    FEATURE_DEV = "feature-dev"
    COMPOUND = "compound"
    TECHNICAL_LAYER = "technical-layer"
    ENTITY_SCOPE = "entity-scope"
    CHANGE_TYPE = "change-type"

    @property
    def marker(self) -> str:
        return self.value.replace("-", "_")


# Generated from .github/smart-testing/taxonomy.yaml by the datahub-test-taxonomy skill
# (`tags.py enums --write`). Edit taxonomy.yaml, not this block.
# BEGIN GENERATED ENUMS
class FeatureUser(str, Enum):
    """User-facing product concepts — features, UI surfaces, and documented
    capabilities."""

    LINEAGE = "lineage"
    OWNERSHIP = "ownership"
    GLOSSARY = "glossary"
    TAGS = "tags"
    DOMAINS = "domains"
    DATA_PRODUCTS = "data-products"
    ASSERTIONS = "assertions"
    INCIDENTS = "incidents"
    INGESTION = "ingestion"
    SEARCH = "search"
    DOCUMENTATION = "documentation"
    STRUCTURED_PROPERTIES = "structured-properties"
    FORMS = "forms"
    AUTHORIZATION = "authorization"
    SCHEMA_METADATA = "schema-metadata"
    VIEWS = "views"
    PROPOSALS = "proposals"
    DEPRECATION = "deprecation"
    HOME_PAGE = "home-page"
    DATA_CONTRACTS = "data-contracts"
    BUSINESS_ATTRIBUTES = "business-attributes"
    SETTINGS = "settings"
    STATS = "stats"
    CHANGE_HISTORY = "change-history"
    ANALYTICS = "analytics"


class FeatureDev(str, Enum):
    """Developer-facing internal mechanisms — architectural subsystems, infrastructure
    plumbing."""

    ASPECT_VALIDATION = "aspect-validation"
    MCE_MCL_PIPELINE = "mce-mcl-pipeline"
    SEARCH_INDEXING = "search-indexing"
    ENTITY_REGISTRY = "entity-registry"
    SYSTEM_METADATA = "system-metadata"
    RETENTION = "retention"
    TIMESERIES_ASPECTS = "timeseries-aspects"
    ENTITY_VERSIONING = "entity-versioning"
    BOOTSTRAP = "bootstrap"
    PATCH_SEMANTICS = "patch-semantics"
    GRAPH_INDEX = "graph-index"
    SIBLINGS = "siblings"
    BILLING_METRICS = "billing-metrics"
    FEATURE_FLAGS = "feature-flags"
    SEMANTIC_SEARCH = "semantic-search"
    ZERO_DOWNTIME_UPGRADE = "zero-downtime-upgrade"
    EXECUTOR = "executor"
    AUTHENTICATION = "authentication"
    STATEFUL_INGESTION = "stateful-ingestion"
    SQL_LINEAGE = "sql-lineage"
    CLASSIFICATION = "classification"
    PROFILING = "profiling"
    TRANSFORMS = "transforms"
    SCHEMA_TRANSLATION = "schema-translation"


class Compound(str, Enum):
    """Compound tags for concepts with typed relationships that flat tags would over-
    select."""

    PROPAGATION_DOCS = "propagation:docs"
    PROPAGATION_TAGS = "propagation:tags"
    PROPAGATION_TERMS = "propagation:terms"
    PROPAGATION_OWNERSHIP = "propagation:ownership"
    PROPAGATION_DOMAINS = "propagation:domains"
    PROPAGATION_STRUCTURED_PROPERTIES = "propagation:structured-properties"
    PROPAGATION_LINEAGE_PATH = "propagation:lineage-path"
    PROPAGATION_SIBLING_PATH = "propagation:sibling-path"
    PROPAGATION_LOGICAL_MODEL = "propagation:logical-model"
    INGESTION_CONNECTOR = "ingestion:connector"


class TechnicalLayer(str, Enum):
    """Infrastructure and protocol boundaries — the 'how' of each operation."""

    GRAPHQL = "graphql"
    OPENAPI = "openapi"
    RESTLI = "restli"
    KAFKA_EVENTS = "kafka-events"
    ELASTICSEARCH = "elasticsearch"
    EBEAN_MYSQL = "ebean-mysql"
    REACT_FRONTEND = "react-frontend"
    CLI = "cli"
    SPRING_CONFIG = "spring-config"
    PLAYWRIGHT_E2E = "playwright-e2e"


class EntityScope(str, Enum):
    """Entity types involved — the 'on what' of each operation. Only entities not
    already covered by a feature-user tag."""

    DATASET = "dataset"
    DASHBOARD_CHART = "dashboard-chart"
    DATAFLOW_DATAJOB = "dataflow-datajob"
    CONTAINER = "container"
    CORPUSER_CORPGROUP = "corpuser-corpgroup"
    ML_ENTITIES = "ml-entities"
    APPLICATION_SERVICE = "application-service"


class ChangeType(str, Enum):
    """PR-only: the kind of non-product change a PR makes (CI, build, test
    infrastructure, docs, tooling). Tests never carry these tags."""

    CI = "ci"
    BUILD = "build"
    TEST_INFRA = "test-infra"
    DOCS = "docs"
    TOOLING = "tooling"
    PYTEST_PLUMBING = "pytest-plumbing"


# END GENERATED ENUMS


TAG_ENUMS: Dict[Dimension, Type[Enum]] = {
    Dimension.FEATURE_USER: FeatureUser,
    Dimension.FEATURE_DEV: FeatureDev,
    Dimension.COMPOUND: Compound,
    Dimension.TECHNICAL_LAYER: TechnicalLayer,
    Dimension.ENTITY_SCOPE: EntityScope,
    Dimension.CHANGE_TYPE: ChangeType,
}

KNOWN_TAGS: Dict[Dimension, FrozenSet[str]] = {
    dim: frozenset(tag.value for tag in enum) | OVERLAY_TAGS.get(dim.value, frozenset())
    for dim, enum in TAG_ENUMS.items()
}

FEATURE_DIMENSIONS: Tuple[Dimension, ...] = (
    Dimension.FEATURE_USER,
    Dimension.FEATURE_DEV,
    Dimension.COMPOUND,
)
NARROWING_DIMENSIONS: Tuple[Dimension, ...] = (
    Dimension.TECHNICAL_LAYER,
    Dimension.ENTITY_SCOPE,
)

TagSet = Dict[Dimension, Set[str]]


def tag_value(tag: object) -> str:
    return tag.value if isinstance(tag, Enum) else str(tag)


def parse_tags(raw: Mapping[str, Optional[Iterable[object]]]) -> TagSet:
    """Build a TagSet from ``{"feature-user": [...], ...}``.

    Unknown dimension keys raise: they are a structural mistake, unlike an
    unknown tag value, which callers report for review.
    """
    tags: TagSet = {}
    for key, values in raw.items():
        dim = Dimension(key)
        found = {tag_value(v) for v in values or []}
        if found:
            tags[dim] = found
    return tags


def split_known(tags: TagSet) -> Tuple[TagSet, TagSet]:
    """Split tags into (known, unknown) against the taxonomy."""
    known: TagSet = {}
    unknown: TagSet = {}
    for dim, values in tags.items():
        if values & KNOWN_TAGS[dim]:
            known[dim] = values & KNOWN_TAGS[dim]
        if values - KNOWN_TAGS[dim]:
            unknown[dim] = values - KNOWN_TAGS[dim]
    return known, unknown


def feature_tags(tags: TagSet) -> Set[str]:
    return set().union(*(tags.get(dim, set()) for dim in FEATURE_DIMENSIONS))


def changes_pytest_plumbing(tags: TagSet) -> bool:
    return ChangeType.PYTEST_PLUMBING.value in tags.get(Dimension.CHANGE_TYPE, set())


@dataclass(frozen=True)
class TagRule:
    pattern: str
    tags: TagSet = field(hash=False)


@dataclass(frozen=True)
class TagMap:
    """Module path (relative to smoke-test/) → tags, from test_tags.yaml."""

    always_run: FrozenSet[str]
    plumbing_sample: FrozenSet[str]
    rules: Tuple[TagRule, ...]

    def tags_for(self, path: str) -> Optional[TagSet]:
        """Exact path wins over globs; among globs the longest pattern wins."""
        best: Optional[TagRule] = None
        for rule in self.rules:
            if rule.pattern == path:
                return rule.tags
            if fnmatch.fnmatch(path, rule.pattern) and (
                best is None or len(rule.pattern) > len(best.pattern)
            ):
                best = rule
        return best.tags if best else None


def load_tag_map(
    path: Path = TAG_MAP_PATH, overlay: Optional[Path] = TAG_MAP_OVERLAY_PATH
) -> TagMap:
    """The tag map, merged with the overlay when it exists (rules concatenate)."""
    always_run: Set[str] = set()
    plumbing_sample: Set[str] = set()
    rules: List[TagRule] = []
    for source in (path, overlay):
        if source is None or (source is overlay and not source.exists()):
            continue
        with open(source) as f:
            data = yaml.safe_load(f) or {}
        always_run.update(data.get("always_run") or [])
        plumbing_sample.update(data.get("plumbing_sample") or [])
        rules.extend(
            TagRule(pattern=entry["path"], tags=parse_tags(entry.get("tags") or {}))
            for entry in data.get("tests") or []
        )
    return TagMap(
        always_run=frozenset(always_run),
        plumbing_sample=frozenset(plumbing_sample),
        rules=tuple(rules),
    )


@dataclass
class PRTags:
    """What a PR touches, as classified against the taxonomy."""

    tags: TagSet
    confidence: str = "UNKNOWN"

    def to_json(self) -> str:
        payload: Dict[str, object] = {
            dim.value: sorted(values) for dim, values in self.tags.items()
        }
        payload["confidence"] = self.confidence
        return json.dumps(payload, sort_keys=True)

    @classmethod
    def from_json(cls, raw: str) -> "PRTags":
        data = json.loads(raw)
        if not isinstance(data, dict):
            raise ValueError("PR tags JSON must be an object")
        confidence = str(data.pop("confidence", "UNKNOWN"))
        return cls(tags=parse_tags(data), confidence=confidence)


def select_modules(
    module_tags: Mapping[str, Optional[TagSet]],
    pr: PRTags,
    always_run: Iterable[str],
    *,
    plumbing_sample: Iterable[str],
) -> Set[str]:
    """Modules to run for a PR.

    A module is selected when it shares any feature tag with the PR; untagged
    modules are always selected (fail safe). Very broad selections are then
    narrowed by technical layer and entity scope. With no PR feature tags only
    ``always_run`` is selected. A PR that changes the shared pytest harness
    (``change-type: pytest-plumbing``) also runs ``plumbing_sample``, with or
    without feature tags: a harness bug can break any module, and the sample
    spreads across the common fixtures and API surfaces.
    """
    wanted = feature_tags(pr.tags)
    selected: Set[str] = set()
    if wanted:
        selected = {
            module
            for module, tags in module_tags.items()
            if tags is None or feature_tags(tags) & wanted
        }

    if len(selected) > NARROW_THRESHOLD:
        for dim in NARROWING_DIMENSIONS:
            pr_dim = pr.tags.get(dim)
            if not pr_dim:
                continue
            narrowed = set()
            for module in selected:
                tags = module_tags[module]
                module_dim = tags.get(dim) if tags is not None else None
                if not module_dim or module_dim & pr_dim:
                    narrowed.add(module)
            if len(narrowed) >= SAFETY_FLOOR:
                selected = narrowed

    selected |= set(always_run)
    if changes_pytest_plumbing(pr.tags):
        selected |= set(plumbing_sample)
    return selected
