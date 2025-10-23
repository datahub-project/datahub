import importlib
import os
from unittest.mock import patch

from datahub_integrations.gen_ai.model_config import (
    BedrockModel,
    LiteLLMModel,
    get_bedrock_model_env_variable,
    get_litellm_model_env_variable,
    model_config,
)


def test_get_bedrock_model_env_variable() -> None:
    with patch.dict(
        os.environ,
        {"CHATBOT_MODEL": "us.anthropic.claude-3-7-sonnet-20250219-v1:0"},
        clear=True,
    ):
        assert (
            get_bedrock_model_env_variable(
                "CHATBOT_MODEL", BedrockModel.CLAUDE_35_SONNET
            )
            == "us.anthropic.claude-3-7-sonnet-20250219-v1:0"
        )

    with patch.dict(
        os.environ,
        {"CHATBOT_MODEL": "bedrock/us.anthropic.claude-3-7-sonnet-20250219-v1:0"},
        clear=True,
    ):
        assert (
            get_bedrock_model_env_variable(
                "CHATBOT_MODEL", BedrockModel.CLAUDE_35_SONNET
            )
            == "us.anthropic.claude-3-7-sonnet-20250219-v1:0"
        )


def test_get_litellm_model_env_variable() -> None:
    with patch.dict(
        os.environ,
        {"CHATBOT_MODEL": "us.anthropic.claude-3-7-sonnet-20250219-v1:0"},
        clear=True,
    ):
        # Use default model if model is not valid bedrock model
        assert (
            get_litellm_model_env_variable(
                "CHATBOT_MODEL", LiteLLMModel.CLAUDE_35_SONNET
            )
            == "bedrock/us.anthropic.claude-3-5-sonnet-20240620-v1:0"
        )

    with patch.dict(
        os.environ,
        {"CHATBOT_MODEL": "bedrock/us.anthropic.claude-3-7-sonnet-20250219-v1:0"},
        clear=True,
    ):
        assert (
            get_litellm_model_env_variable(
                "CHATBOT_MODEL", LiteLLMModel.CLAUDE_35_SONNET
            )
            == "bedrock/us.anthropic.claude-3-7-sonnet-20250219-v1:0"
        )


def test_get_bedrock_model_env_variable_with_alternate_env() -> None:
    with patch.dict(
        os.environ,
        {
            "CHATBOT_MODEL": "us.anthropic.claude-3-7-sonnet-20250219-v1:0",
        },
        clear=True,
    ):
        assert (
            get_bedrock_model_env_variable(
                "CHATBOT_MODEL",
                BedrockModel.CLAUDE_35_SONNET,
            )
            == "us.anthropic.claude-3-7-sonnet-20250219-v1:0"
        )

    with patch.dict(
        os.environ,
        {
            "CHATBOT_MODEL": "bedrock/us.anthropic.claude-sonnet-4-20250514-v1:0",
        },
        clear=True,
    ):
        assert (
            get_bedrock_model_env_variable(
                "CHATBOT_MODEL",
                BedrockModel.CLAUDE_35_SONNET,
            )
            == "us.anthropic.claude-sonnet-4-20250514-v1:0"
        )


def test_no_env_vars_set() -> None:
    env_vars: dict[str, str] = {}

    with patch.dict(os.environ, env_vars, clear=True):
        # Test that the module-level model_config uses default values
        assert (
            model_config.documentation_ai.model
            == "bedrock/us.anthropic.claude-3-haiku-20240307-v1:0"
        )
        assert (
            model_config.documentation_ai.query_description_model
            == "bedrock/us.anthropic.claude-3-7-sonnet-20250219-v1:0"
        )
        assert (
            model_config.chat_assistant_ai.model
            == "us.anthropic.claude-3-7-sonnet-20250219-v1:0"
        )
        assert (
            model_config.term_suggestion_ai.model
            == "us.anthropic.claude-3-7-sonnet-20250219-v1:0"
        )
        assert not model_config.chat_assistant_ai.planning_mode_enabled


def test_only_region_prefix_set_to_eu() -> None:
    # Test that when only region prefix is set, the system uses default models with that prefix
    env_vars = {"ANTHROPIC_CROSS_REGION_INFERENCE_PREFIX": "eu"}

    with patch.dict(os.environ, env_vars, clear=True):
        # Reload the module after setting the environment variable to get the correct regional prefix
        import datahub_integrations.gen_ai.model_config

        importlib.reload(datahub_integrations.gen_ai.model_config)
        eu_model_config = datahub_integrations.gen_ai.model_config.model_config

        # Test that the module-level model_config uses default values with "eu" prefix
        assert (
            eu_model_config.documentation_ai.model
            == "bedrock/eu.anthropic.claude-3-haiku-20240307-v1:0"
        )
        assert (
            eu_model_config.documentation_ai.query_description_model
            == "bedrock/eu.anthropic.claude-3-7-sonnet-20250219-v1:0"
        )
        assert (
            eu_model_config.chat_assistant_ai.model
            == "eu.anthropic.claude-3-7-sonnet-20250219-v1:0"
        )
        assert (
            eu_model_config.term_suggestion_ai.model
            == "eu.anthropic.claude-3-7-sonnet-20250219-v1:0"
        )
        assert not eu_model_config.chat_assistant_ai.planning_mode_enabled


def test_all_env_vars_set_legacy() -> None:
    # Test that legacy environment variables work correctly with APAC region
    env_vars = {
        "ANTHROPIC_CROSS_REGION_INFERENCE_PREFIX": "apac",
        "CHATBOT_MODEL": "apac.anthropic.claude-sonnet-4-20250514-v1:0",
        "TERM_SUGGESTION_MODEL": "apac.anthropic.claude-3-5-haiku-20241022-v1:0",
    }

    with patch.dict(os.environ, env_vars, clear=True):
        # Reload the module after setting the environment variables
        import datahub_integrations.gen_ai.model_config

        importlib.reload(datahub_integrations.gen_ai.model_config)
        legacy_model_config = datahub_integrations.gen_ai.model_config.model_config

        # Documentation should use defaults (not legacy)
        assert (
            legacy_model_config.documentation_ai.model
            == "bedrock/apac.anthropic.claude-3-haiku-20240307-v1:0"  # Default, not legacy
        )
        assert (
            legacy_model_config.documentation_ai.query_description_model
            == "bedrock/apac.anthropic.claude-3-7-sonnet-20250219-v1:0"
        )
        assert (
            legacy_model_config.term_suggestion_ai.model
            == "apac.anthropic.claude-3-5-haiku-20241022-v1:0"
        )
        assert (
            legacy_model_config.chat_assistant_ai.model
            == "apac.anthropic.claude-sonnet-4-20250514-v1:0"
        )
        assert not legacy_model_config.chat_assistant_ai.planning_mode_enabled


def test_current_env_vars_setup() -> None:
    # Test that new environment variables work correctly with LiteLLM format
    # Current state
    env_vars = {
        "ANTHROPIC_CROSS_REGION_INFERENCE_PREFIX": "us",
        "DESCRIPTION_GENERATION_MODEL": "bedrock/us.anthropic.claude-3-7-sonnet-20250219-v1:0",
        "QUERY_DESCRIPTION_GENERATION_MODEL": "bedrock/us.anthropic.claude-3-7-sonnet-20250219-v1:0",
        # old env vars below
        "TERM_SUGGESTION_MODEL": "us.anthropic.claude-sonnet-4-20250514-v1:0",
        "CHATBOT_MODEL": "us.anthropic.claude-sonnet-4-20250514-v1:0",
    }

    with patch.dict(os.environ, env_vars, clear=True):
        # Reload the module after setting the environment variables
        import datahub_integrations.gen_ai.model_config

        importlib.reload(datahub_integrations.gen_ai.model_config)
        new_model_config = datahub_integrations.gen_ai.model_config.model_config

        # Test that new env vars are used with LiteLLM format
        assert (
            new_model_config.documentation_ai.model
            == "bedrock/us.anthropic.claude-3-7-sonnet-20250219-v1:0"
        )
        assert (
            new_model_config.documentation_ai.query_description_model
            == "bedrock/us.anthropic.claude-3-7-sonnet-20250219-v1:0"
        )
        assert (
            new_model_config.chat_assistant_ai.model
            == "us.anthropic.claude-sonnet-4-20250514-v1:0"
        )
        assert (
            new_model_config.term_suggestion_ai.model
            == "us.anthropic.claude-sonnet-4-20250514-v1:0"
        )
        assert not new_model_config.chat_assistant_ai.planning_mode_enabled


def test_ideal_env_vars_setup() -> None:
    # Test that new environment variables work correctly with LiteLLM format
    # Future/Ideal state
    env_vars = {
        "DESCRIPTION_GENERATION_MODEL": "bedrock/us.anthropic.claude-3-7-sonnet-20250219-v1:0",
        "QUERY_DESCRIPTION_GENERATION_MODEL": "bedrock/us.anthropic.claude-3-7-sonnet-20250219-v1:0",
        # new env vars below
        "TERM_SUGGESTION_MODEL": "bedrock/us.anthropic.claude-sonnet-4-20250514-v1:0",
        "CHATBOT_MODEL": "bedrock/us.anthropic.claude-sonnet-4-20250514-v1:0",
    }

    with patch.dict(os.environ, env_vars, clear=True):
        # Reload the module after setting the environment variables
        import datahub_integrations.gen_ai.model_config

        importlib.reload(datahub_integrations.gen_ai.model_config)
        new_model_config = datahub_integrations.gen_ai.model_config.model_config

        # Test that new env vars are used with LiteLLM format
        assert (
            new_model_config.documentation_ai.model
            == "bedrock/us.anthropic.claude-3-7-sonnet-20250219-v1:0"
        )
        assert (
            new_model_config.documentation_ai.query_description_model
            == "bedrock/us.anthropic.claude-3-7-sonnet-20250219-v1:0"
        )
        assert (
            new_model_config.chat_assistant_ai.model
            == "us.anthropic.claude-sonnet-4-20250514-v1:0"
        )
        assert (
            new_model_config.term_suggestion_ai.model
            == "us.anthropic.claude-sonnet-4-20250514-v1:0"
        )
        assert not new_model_config.chat_assistant_ai.planning_mode_enabled


def test_planning_mode_enabled() -> None:
    # Test that planning mode can be enabled via environment variable
    env_vars = {
        "CHATBOT_PLANNING_ENABLED": "true",
    }

    with patch.dict(os.environ, env_vars, clear=True):
        # Reload the module after setting the environment variable
        import datahub_integrations.gen_ai.model_config

        importlib.reload(datahub_integrations.gen_ai.model_config)
        planning_model_config = datahub_integrations.gen_ai.model_config.model_config

        # Test that planning mode is enabled
        assert planning_model_config.chat_assistant_ai.planning_mode_enabled


def test_planning_mode_disabled_explicitly() -> None:
    # Test that planning mode can be explicitly disabled via environment variable
    env_vars = {
        "CHATBOT_PLANNING_ENABLED": "false",
    }

    with patch.dict(os.environ, env_vars, clear=True):
        # Reload the module after setting the environment variable
        import datahub_integrations.gen_ai.model_config

        importlib.reload(datahub_integrations.gen_ai.model_config)
        planning_model_config = datahub_integrations.gen_ai.model_config.model_config

        # Test that planning mode is disabled
        assert not planning_model_config.chat_assistant_ai.planning_mode_enabled
