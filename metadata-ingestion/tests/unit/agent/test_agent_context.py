import importlib.resources


def test_recipe_agent_context_ships_and_mentions_workflow():
    """Verify the RECIPE_AGENT_CONTEXT resource ships and documents the workflow order."""
    text = (
        importlib.resources.files("datahub.cli.resources")
        .joinpath("RECIPE_AGENT_CONTEXT.md")
        .read_text(encoding="utf-8")
    )
    # The required workflow steps must be documented.
    for step in ["describe", "probe", "validate", "test-connection"]:
        assert step in text, f"Step '{step}' not found in RECIPE_AGENT_CONTEXT.md"

    # Verify it uses the correct command group.
    assert "datahub recipe" in text, "Resource must reference 'datahub recipe' commands"
