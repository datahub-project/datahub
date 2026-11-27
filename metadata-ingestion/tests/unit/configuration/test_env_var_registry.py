import importlib
import os
import pathlib
import subprocess
import sys
from unittest.mock import patch

import pytest

from datahub.configuration.env_var_registry import (
    EnvVarAudience,
    EnvVarSpec,
    get_accessor,
    get_conflicts,
    get_registry,
)

# Imported late, through sys.path: scripts/ is not a package, but the generator and this test
# must measure "what the code reads" the same way or the docs quietly go stale.
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[3] / "scripts"))
from env_var_scan import default_source_root, scan_tree

# Importing the package root is not enough: declarations live next to the reads they describe,
# so every module holding one must be imported before the registry is complete. Importing only
# env_vars left 31 of 130 variables out, and which of them were present depended on what an
# unrelated test had imported first -- so --random-order changed what was actually checked.
import datahub.configuration.env_vars  # noqa: F401  isort:skip


@pytest.fixture(scope="module", autouse=True)
def _load_all_declarations(scan):
    """Import every declaring module, and prove the registry then covers all of them."""
    failed = {}
    for module in sorted(scan.declaring_modules):
        try:
            importlib.import_module(module)
        except Exception as exc:  # pragma: no cover - a declaring module must import
            failed[module] = repr(exc)
    assert not failed, f"Declaring modules failed to import: {failed}"

    present = {spec.name for spec in get_registry(include_undocumented=True)}
    missing = sorted(set(scan.declared) - present)
    assert not missing, (
        "Declared in the source but absent from the registry, so every test in this file "
        f"would have silently skipped them: {missing}"
    )


# Variables the SDK reads but does not own. Each entry is a deliberate exemption, not a gap --
# adding a DataHub variable here instead of declaring it is what this allowlist is meant to
# make visible in review.
UNDECLARED_ALLOWLIST = {
    # Not variables at all: a prefix constant and an enum value that match the name pattern.
    "DATAHUB_AUTH_": "Prefix constant for the executor's protected-variable check, not a variable.",
    "INGESTION_ALTERNATE": "KnownAttribution enum value that happens to match the name pattern.",
    # Third-party credentials and settings, owned by the tool that defines them. Documenting
    # them here would duplicate -- and eventually contradict -- their own documentation.
    "AWS_ACCESS_KEY_ID": "AWS SDK credential, read only to detect whether credentials are present.",
    "AWS_PROFILE": "AWS SDK credential profile, read only for presence detection.",
    "COHERE_API_KEY": "Cohere SDK credential, used by the unstructured embedding provider.",
    "OPENAI_API_KEY": "OpenAI SDK credential, used by the unstructured embedding provider.",
    "VERTEX_AI_PROJECT_ID": "Vertex AI setting for the unstructured embedding provider.",
    "LOCAL_EMBEDDING_ENDPOINT": "Endpoint for a self-hosted embedding server.",
    "ONNX_EMBEDDING_MODEL_DIR": "Local ONNX model directory for the unstructured source.",
    "ONNX_EMBEDDING_POOLING": "ONNX pooling strategy for the unstructured source.",
    "ONNX_EMBEDDING_QUERY_INSTRUCTION": "ONNX query instruction prefix for the unstructured source.",
    "MLFLOW_TRACKING_USERNAME": "MLflow client credential, set from recipe config.",
    "MLFLOW_TRACKING_PASSWORD": "MLflow client credential, set from recipe config.",
    "ORACLE_CLIENT_LIBRARY_DIR": "Oracle instant client setting, set from recipe config.",
    "TNS_ADMIN": "Oracle TNS configuration directory, set from recipe config.",
    "KRB5_CLIENT_KTNAME": "Kerberos keytab path, set for the Presto dialect.",
    "KRB5CCNAME": "Kerberos credential cache path, set for the Presto dialect.",
    "SQLMESH__DEFAULT_GATEWAY": "SQLMesh's own setting, written and restored around a call.",
    "PGCONNECT_TIMEOUT": "libpq setting; DataHub defers to an operator-set value.",
    "JAVA_HOME": "Standard JVM location, read to find a Java runtime.",
    "VIRTUAL_ENV": "Standard Python virtualenv marker, set for ingestion subprocesses.",
    "PATH": "Standard search path, set for ingestion subprocesses.",
    "TZ": "Standard timezone variable, manipulated only by test helpers.",
}


@pytest.fixture(scope="module")
def scan():
    return scan_tree(default_source_root())


def test_every_env_var_read_is_declared_or_allowlisted(scan):
    """A new environment variable has to be declared before it can ship.

    Without this, the reference page documents whatever happened to be declared and silently
    omits the rest -- which is how roughly 40% of the user-settable variables came to be
    undocumented in the first place.
    """
    undeclared = {
        name: sites
        for name, sites in scan.reads.items()
        if name not in scan.declared and name not in UNDECLARED_ALLOWLIST
    }
    assert not undeclared, (
        "These environment variables are read but not declared:\n"
        + "\n".join(
            f"  {name}  ({sites[0]})" for name, sites in sorted(undeclared.items())
        )
        + "\n\nDeclare each one with @env_var or declare_env_var next to the code that reads it, "
        "so it appears in the environment variable reference. If it belongs to a third-party "
        "library rather than DataHub, add it to UNDECLARED_ALLOWLIST with a reason."
    )


def test_allowlist_has_no_stale_entries(scan):
    """An allowlisted variable that is no longer read should leave the allowlist."""
    stale = sorted(set(UNDECLARED_ALLOWLIST) - set(scan.reads))
    assert not stale, f"No longer read; drop from UNDECLARED_ALLOWLIST: {stale}"


def test_declared_defaults_match_the_code(monkeypatch):
    """The declared default must be what the accessor actually falls back to.

    Scope: this covers the 104 declared with @env_var, which have an accessor to call. The 26
    declared with declare_env_var have no function to invoke, so their defaults are NOT checked
    here -- for nine of those the declared default is a second copy of a literal at the read
    site and could drift unnoticed. Narrowing a default-checking test to the declaration style
    that happens to be testable is a known gap, not a design.
    """
    mismatches = []
    for spec in get_registry(include_undocumented=True):
        accessor = get_accessor(spec.name)
        if accessor is None:
            continue
        with patch.dict(os.environ, {}, clear=True):
            try:
                actual = accessor()
            except Exception as exc:  # pragma: no cover - defensive
                mismatches.append(
                    f"{spec.name}: accessor raised {exc!r} with a cleared environment"
                )
                continue
        if actual != spec.default:
            mismatches.append(
                f"{spec.name}: declared default {spec.default!r}, accessor returned {actual!r}"
            )
    assert not mismatches, "Declared defaults disagree with the code:\n" + "\n".join(
        mismatches
    )


def test_package_imports_under_optimized_python():
    """`python -OO` must not break the SDK.

    -OO strips docstrings. When descriptions were read from them, importing the package under
    -OO raised and took the whole SDK down -- in exactly the slim container and Lambda images
    likely to use it. Descriptions are explicit now; this keeps them that way.
    """
    result = subprocess.run(
        [sys.executable, "-OO", "-c", "import datahub.configuration.env_vars"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        "Importing under python -OO failed:\n" + result.stderr[-2000:]
    )


def test_no_description_is_empty():
    """A declared description must carry text.

    `description` is a required argument, so it cannot be forgotten -- but it can still be
    passed as "" or whitespace, which a required field does not catch. Checked here rather
    than at import, so an authoring mistake fails CI instead of the SDK.
    """
    empty = [
        spec.name
        for spec in get_registry(include_undocumented=True)
        if not spec.description.strip()
    ]
    assert not empty, f"Declared with an empty description: {empty}"


def test_enum_backed_choices_match_the_enum():
    """A documented value must be one the code actually accepts.

    These variables are looked up in an enum at import time, so a value that is documented but
    not a member does not degrade -- it raises and takes the run down. Listing `ENABLED` for
    DATAHUB_SQL_AGG_QUERY_LOG, which only has DISABLED / STORE_ALL / STORE_FAILED, did exactly
    that: following the docs crashed ingestion.
    """
    from datahub.emitter.rest_emitter import EmitMode, RestSinkEndpoint
    from datahub.ingestion.sink.datahub_rest import RestSinkMode
    from datahub.sql_parsing.sql_parsing_aggregator import QueryLogSetting

    enum_backed = {
        "DATAHUB_SQL_AGG_QUERY_LOG": QueryLogSetting,
        "DATAHUB_EMIT_MODE": EmitMode,
        "DATAHUB_REST_EMITTER_DEFAULT_ENDPOINT": RestSinkEndpoint,
        "DATAHUB_REST_SINK_DEFAULT_MODE": RestSinkMode,
    }
    registry = {spec.name: spec for spec in get_registry(include_undocumented=True)}

    problems = []
    for name, enum_cls in enum_backed.items():
        spec = registry.get(name)
        if spec is None:
            problems.append(f"{name}: no longer declared")
            continue
        if not spec.choices:
            problems.append(
                f"{name}: backed by {enum_cls.__name__} but declares no choices, so a wrong "
                "value in the description would go unnoticed"
            )
            continue
        members = {member.name for member in enum_cls}
        invented = set(spec.choices) - members
        undocumented = members - set(spec.choices)
        if invented:
            problems.append(
                f"{name}: documents {sorted(invented)}, which {enum_cls.__name__} rejects"
            )
        if undocumented:
            problems.append(f"{name}: omits {sorted(undocumented)} from choices")
    assert not problems, "\n".join(problems)


def test_the_pages_boolean_example_is_still_true():
    """The page names a variable that accepts `true` but not `1`. Keep that example honest.

    It used to name DATAHUB_DEBUG, and master then changed that accessor to accept `1` as
    well, leaving the page asserting something false.
    """
    registry = {spec.name: spec for spec in get_registry(include_undocumented=True)}
    spec = registry["NO_COLOR"]
    assert spec.choices == ("true",), (
        "metadata-ingestion/environment-variables.md says NO_COLOR=1 does not disable colour. "
        f"NO_COLOR now accepts {spec.choices}; update the page's example and this test."
    )


def test_specs_are_internally_consistent():
    registry = {spec.name: spec for spec in get_registry(include_undocumented=True)}
    problems = []
    for spec in registry.values():
        if spec.choices is not None and not spec.choices:
            problems.append(f"{spec.name}: choices is empty; omit it instead")
        if spec.replaced_by and spec.replaced_by not in registry:
            problems.append(
                f"{spec.name}: replaced_by names {spec.replaced_by}, which is not declared"
            )
        if spec.replaced_by and not spec.deprecated:
            problems.append(
                f"{spec.name}: has replaced_by but is not marked deprecated"
            )
        if not spec.description.strip():
            problems.append(f"{spec.name}: empty description")
    assert not problems, "\n".join(problems)


def test_conflicting_declaration_keeps_the_first_without_raising():
    """A conflict must not break an import.

    Raising here would fail any process importing both declaring modules, over metadata that
    nothing reads at runtime. The first declaration wins and the conflict is recorded for
    test_no_declaration_conflicts below.
    """
    from datahub.configuration.env_var_registry import EnvVarGroup, register_env_var

    name = "DATAHUB_TEST_DUPLICATE_DECLARATION"
    first = EnvVarSpec(
        name=name, type=str, description="First declaration.", group=EnvVarGroup.MISC
    )
    register_env_var(first)
    register_env_var(first)  # identical re-registration is a no-op

    kept = register_env_var(
        EnvVarSpec(
            name=name,
            type=str,
            description="Conflicting declaration.",
            group=EnvVarGroup.MISC,
        )
    )
    assert kept == first, "the first declaration must win"
    assert name in get_conflicts()
    _CONFLICTS_FROM_THIS_TEST.add(name)


_CONFLICTS_FROM_THIS_TEST: set = set()


def test_no_declaration_conflicts():
    """No two modules may declare the same variable differently."""
    real = {
        name: specs
        for name, specs in get_conflicts().items()
        if name not in _CONFLICTS_FROM_THIS_TEST
    }
    assert not real, f"Conflicting declarations for: {sorted(real)}"


# Variables whose caveat the reference page is the single source of truth for. Each was
# documented with a warning somewhere else (cli.md, a topic page) before this page existed;
# losing the warning in the move is the specific regression these guard against.
RISK_CAVEATS_THAT_MUST_SURVIVE = {
    "DATAHUB_MAX_TRANSFORM_OPERATION_LENGTH": "incremental_lineage",
    "DATAHUB_DATASET_URN_TO_LOWER": "duplicate",
    "DATAHUB_DEBUG": "sensitive",
    "DATAHUB_SKIP_CONFIG": "--profile",
    "DATAHUB_DISABLE_SECRET_MASKING": "credential",
}


def test_risky_variables_say_what_goes_wrong():
    """A warning mark with no explanation is worse than no mark.

    `risky=True` renders a warning on the row; the description has to tell the reader what
    actually breaks, or they are left alarmed and none the wiser.
    """
    thin = [
        spec.name
        for spec in get_registry(include_undocumented=True)
        if spec.risky and len(spec.description.strip()) < 80
    ]
    assert not thin, (
        "These are marked risky but their description is too thin to be useful: "
        f"{thin}. Say what changing it actually does."
    )


def test_known_risk_caveats_are_not_lost():
    registry = {spec.name: spec for spec in get_registry(include_undocumented=True)}
    missing = []
    for name, needle in RISK_CAVEATS_THAT_MUST_SURVIVE.items():
        spec = registry.get(name)
        if spec is None:
            missing.append(f"{name}: no longer declared")
        elif needle.lower() not in spec.description.lower():
            missing.append(f"{name}: description no longer mentions {needle!r}")
    assert not missing, (
        "Risk caveats dropped from the reference page:\n"
        + "\n".join(f"  {m}" for m in missing)
        + "\n\nThis page is the single source of truth for these variables, so a caveat that "
        "disappears here disappears for users."
    )


def test_internal_variables_are_not_documented():
    documented = {spec.name for spec in get_registry()}
    internal = {
        spec.name
        for spec in get_registry(include_undocumented=True)
        if spec.audience is EnvVarAudience.INTERNAL
    }
    assert internal and not (internal & documented)
