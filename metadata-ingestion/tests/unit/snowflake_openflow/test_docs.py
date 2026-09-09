import pathlib
import re
from typing import List, Set

import yaml

from datahub.ingestion.source.snowflake.snowflake_openflow import (
    CONNECTOR_DEFINITION_PLATFORM,
)
from datahub.ingestion.source.snowflake.snowflake_openflow_config import (
    SnowflakeOpenflowSourceConfig,
)

# The docs directory is keyed on the PLATFORM id ("openflow"), while the file
# prefixes are the plugin/recipe type ("snowflake-openflow"). docgen matches
# docs/sources/<platform>/<plugin>_pre.md.
DOC_DIR = pathlib.Path("docs/sources/openflow")
PRE = DOC_DIR / "snowflake-openflow_pre.md"
POST = DOC_DIR / "snowflake-openflow_post.md"
README = DOC_DIR / "README.md"
RECIPE = DOC_DIR / "snowflake-openflow_recipe.yml"


def headings(path: pathlib.Path, level: int) -> List[str]:
    pattern = re.compile(r"^#{%d} (.+)$" % level, re.MULTILINE)
    return pattern.findall(path.read_text())


def all_heading_levels(path: pathlib.Path) -> Set[int]:
    return {
        len(m.group(1)) for m in re.finditer(r"^(#+) ", path.read_text(), re.MULTILINE)
    }


def test_all_doc_files_exist():
    missing = [p.name for p in [README, PRE, POST, RECIPE] if not p.exists()]
    assert not missing, f"missing doc files: {missing}"


def test_readme_matches_docgen_contract():
    # docgen allows ONLY these two H2s, in this order, and forbids H1.
    assert headings(README, 2) == ["Overview", "Concept Mapping"]
    assert 1 not in all_heading_levels(README)


def test_pre_matches_docgen_contract():
    assert headings(PRE, 3) == ["Overview", "Prerequisites"]
    assert not {1, 2} & all_heading_levels(PRE)


def test_post_matches_docgen_contract():
    assert headings(POST, 3) == ["Capabilities", "Limitations", "Troubleshooting"]
    assert not {1, 2} & all_heading_levels(POST)


def test_permissions_are_documented_under_prerequisites():
    # The permissions material has to be H4+, since H3 is restricted to
    # Overview/Prerequisites. Assert the content is present and correctly nested.
    text = PRE.read_text()
    prerequisites = text.split("### Prerequisites", 1)[1]
    for required in ["MONITOR", "ACCOUNT_USAGE", "FUTURE"]:
        assert required in prerequisites, f"permissions docs omit {required}"


def test_measured_limitations_are_stated():
    # Each was measured against a live account and will otherwise arrive as a bug report.
    text = POST.read_text()
    assert "Gen 1" in text
    assert "zero rows" in text


def test_recipe_parses_and_names_the_source():
    recipe = yaml.safe_load(RECIPE.read_text())
    assert recipe["source"]["type"] == "snowflake-openflow"


def test_recipe_sets_authentication_type_for_key_pair_auth():
    # Setting private_key without authentication_type is rejected by
    # SnowflakeConnectionConfig at load time (defaults to DEFAULT_AUTHENTICATOR),
    # so the example recipe would fail on a reader's first run without this field.
    recipe = yaml.safe_load(RECIPE.read_text())
    connection = recipe["source"]["config"]["connection"]
    assert connection["authentication_type"] == "KEY_PAIR_AUTHENTICATOR"


def test_docs_disable_instruction_matches_the_parsed_default():
    # The docs tell operators to *disable* lineage by setting
    # include_openflow_lineage: false. That instruction is only coherent if the
    # parsed default is on, so this pins the docs/config contract rather than the
    # pydantic default on its own -- the default alone is what the framework
    # guarantees, the agreement between the two is what we guarantee.
    prose = POST.read_text() + RECIPE.read_text()
    assert "include_openflow_lineage" in prose, (
        "docs no longer mention the flag; drop this test or update the docs"
    )
    default = SnowflakeOpenflowSourceConfig.model_fields[
        "include_openflow_lineage"
    ].default
    assert default, (
        "docs instruct operators to disable lineage with `false`, which only makes "
        "sense while the default is enabled"
    )


def test_supported_connector_types_table_matches_the_mapping():
    # The table is hand-maintained prose; the mapping is what the code actually
    # consults. Adding a definition without documenting it would leave operators
    # reading a list that silently understates what produces upstream lineage.
    documented = set(re.findall(r"\|\s*`(OPENFLOW_[A-Z0-9_]+)`\s*\|", POST.read_text()))
    assert documented == set(CONNECTOR_DEFINITION_PLATFORM), (
        f"documented {sorted(documented)} != mapped "
        f"{sorted(CONNECTOR_DEFINITION_PLATFORM)}"
    )


def test_documented_upstream_platforms_match_the_mapping():
    rows = re.findall(
        r"\|\s*`(OPENFLOW_[A-Z0-9_]+)`\s*\|\s*`([a-z]+)`\s*\|", POST.read_text()
    )
    assert {d: p for d, p in rows} == {
        d: u.platform for d, u in CONNECTOR_DEFINITION_PLATFORM.items()
    }
