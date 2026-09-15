from unittest import mock

import click

from datahub.cli.skill_context import infer_skill_component, sanitize_skill_component
from datahub.ingestion.graph.config import ClientMode


def _click_context(obj: object) -> click.Context:
    return click.Context(click.Command("datahub"), obj=obj)


def test_sanitize_namespaces_and_normalizes():
    assert sanitize_skill_component("datahub-search") == "skill-datahub-search"
    assert sanitize_skill_component("  DataHub-Search  ") == "skill-datahub-search"
    assert sanitize_skill_component("skill-datahub-search") == "skill-datahub-search"


def test_sanitize_strips_user_agent_breaking_characters():
    # ";" and "/" delimit the component/caller field GMS parses out of the
    # User-Agent, so they must never survive into the component.
    assert sanitize_skill_component("my skill; datahub/search") == (
        "skill-my-skill-datahub-search"
    )
    assert sanitize_skill_component("a\r\nb") == "skill-a-b"


def test_sanitize_rejects_unusable_values():
    assert sanitize_skill_component("") is None
    assert sanitize_skill_component("   ") is None
    assert sanitize_skill_component("!!!") is None


def test_sanitize_truncates_long_values():
    component = sanitize_skill_component("x" * 200)
    assert component is not None
    assert len(component) == 64


def test_infer_returns_none_without_cli_context():
    assert infer_skill_component() is None


def test_infer_reads_skill_from_cli_context():
    with _click_context({"context": {"skill": "datahub-lineage"}}):
        assert infer_skill_component() == "skill-datahub-lineage"


def test_infer_returns_none_when_other_context_pairs_passed():
    with _click_context({"context": {"caller": "claude-code"}}):
        assert infer_skill_component() is None


def test_get_default_graph_attributes_the_invoking_skill():
    from datahub.ingestion.graph import client as client_module

    graph_config = mock.MagicMock()
    with (
        mock.patch.object(
            client_module.config_utils, "load_client_config", return_value=graph_config
        ),
        mock.patch.object(client_module, "DataHubGraph"),
        mock.patch.object(client_module.telemetry_instance, "set_context"),
        _click_context({"context": {"skill": "datahub-enrich"}}),
    ):
        client_module.get_default_graph.cache_clear()
        try:
            client_module.get_default_graph(ClientMode.CLI)
        finally:
            client_module.get_default_graph.cache_clear()

    assert graph_config.datahub_component == "skill-datahub-enrich"
