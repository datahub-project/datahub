import os
import pathlib
import sys
from unittest.mock import patch

import pytest

from datahub.configuration.env_var_registry import (
    EnvVarAudience,
    EnvVarSpec,
    get_accessor,
    get_registry,
)

# Imported late, through sys.path: scripts/ is not a package, but the generator and this test
# must measure "what the code reads" the same way or the docs quietly go stale.
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[3] / "scripts"))
from env_var_scan import default_source_root, scan_tree

# Importing the package root is not enough: declarations live next to the reads they describe,
# so the modules holding them have to be imported before the registry is complete.
import datahub.configuration.env_vars  # noqa: F401  isort:skip


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

    The decorator restates a default that lives in an os.getenv call a few lines below it.
    Nothing but this test stops the two drifting apart.
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


def test_duplicate_declaration_with_different_metadata_is_rejected():
    from datahub.configuration.env_var_registry import EnvVarGroup, register_env_var

    base = EnvVarSpec(
        name="DATAHUB_TEST_DUPLICATE_DECLARATION",
        type=str,
        description="First declaration.",
        group=EnvVarGroup.MISC,
    )
    register_env_var(base)
    register_env_var(base)  # identical re-registration is fine

    with pytest.raises(ValueError, match="declared twice"):
        register_env_var(
            EnvVarSpec(
                name="DATAHUB_TEST_DUPLICATE_DECLARATION",
                type=str,
                description="Conflicting declaration.",
                group=EnvVarGroup.MISC,
            )
        )


def test_internal_variables_are_not_documented():
    documented = {spec.name for spec in get_registry()}
    internal = {
        spec.name
        for spec in get_registry(include_undocumented=True)
        if spec.audience is EnvVarAudience.INTERNAL
    }
    assert internal and not (internal & documented)
