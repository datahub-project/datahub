import enum
import functools
import os
from typing import Optional

import pydantic
from datahub.cli.env_utils import get_boolean_env_variable
from loguru import logger
from pydantic import BaseModel, Field

# e.g. "us", "eu", or "apac"
_ANTHROPIC_CROSS_REGION_INFERENCE_PREFIX = os.getenv(
    "ANTHROPIC_CROSS_REGION_INFERENCE_PREFIX", "us"
)


class BedrockModel(enum.Enum):
    # These are the system-defined inference profile name, not the raw model ID.
    # Cross-region inference profiles allow higher request and token quota.
    # See https://docs.aws.amazon.com/bedrock/latest/userguide/inference-profiles-support.html
    # for details on per-region availability.
    CLAUDE_3_HAIKU = f"{_ANTHROPIC_CROSS_REGION_INFERENCE_PREFIX}.anthropic.claude-3-haiku-20240307-v1:0"

    # DEPRECATED: Claude 3.5 Sonnet models are deprecated. Use CLAUDE_45_SONNET (recommended) or CLAUDE_37_SONNET instead.
    CLAUDE_35_SONNET = f"{_ANTHROPIC_CROSS_REGION_INFERENCE_PREFIX}.anthropic.claude-3-7-sonnet-20250219-v1:0"

    # WARNING: Claude 3.5 Haiku is only available in the US region, not EU or APAC.
    CLAUDE_35_HAIKU = f"{_ANTHROPIC_CROSS_REGION_INFERENCE_PREFIX}.anthropic.claude-3-5-haiku-20241022-v1:0"

    # DEPRECATED: Claude 3.5 Sonnet V2 is deprecated. Use CLAUDE_45_SONNET (recommended) or CLAUDE_37_SONNET instead.
    CLAUDE_35_SONNET_V2 = f"{_ANTHROPIC_CROSS_REGION_INFERENCE_PREFIX}.anthropic.claude-3-7-sonnet-20250219-v1:0"
    CLAUDE_37_SONNET = f"{_ANTHROPIC_CROSS_REGION_INFERENCE_PREFIX}.anthropic.claude-3-7-sonnet-20250219-v1:0"
    CLAUDE_4_SONNET = f"{_ANTHROPIC_CROSS_REGION_INFERENCE_PREFIX}.anthropic.claude-sonnet-4-20250514-v1:0"
    # Recommended: Claude Sonnet 4.5 is the latest and most capable model
    CLAUDE_45_SONNET = f"{_ANTHROPIC_CROSS_REGION_INFERENCE_PREFIX}.anthropic.claude-sonnet-4-5-20250929-v1:0"

    def __str__(self) -> str:
        """Return the model ID string when converted to string."""
        return self.value


def get_bedrock_model_env_variable(
    env_var: str, default_model: BedrockModel, alternate_env_var: Optional[str] = None
) -> BedrockModel | str:
    """Get model from environment variable with Pydantic validation (for legacy compatibility)."""
    model_value = os.getenv(env_var)
    if model_value is None and alternate_env_var is not None:
        model_value = os.getenv(alternate_env_var, default_model.value)
    else:
        model_value = model_value or default_model.value
    # This safely handles accidental litellm style model config in helm
    if model_value.startswith("bedrock/"):
        model_value = model_value[len("bedrock/") :]
    return pydantic.TypeAdapter(BedrockModel | str).validate_python(model_value)


class LiteLLMModel(enum.Enum):
    # These are the system-defined inference profile name, not the raw model ID.
    # Cross-region inference profiles allow higher request and token quota.
    # See https://docs.aws.amazon.com/bedrock/latest/userguide/inference-profiles-support.html
    # for details on per-region availability.
    CLAUDE_3_HAIKU = f"bedrock/{BedrockModel.CLAUDE_3_HAIKU.value}"

    # DEPRECATED: Claude 3.5 Sonnet models are deprecated. Use CLAUDE_45_SONNET (recommended) or CLAUDE_37_SONNET instead.
    CLAUDE_35_SONNET = f"bedrock/{BedrockModel.CLAUDE_35_SONNET.value}"

    # WARNING: Claude 3.5 Haiku is only available in the US region, not EU or APAC.
    CLAUDE_35_HAIKU = f"bedrock/{BedrockModel.CLAUDE_35_HAIKU.value}"

    CLAUDE_37_SONNET = f"bedrock/{BedrockModel.CLAUDE_37_SONNET.value}"
    CLAUDE_4_SONNET = f"bedrock/{BedrockModel.CLAUDE_4_SONNET.value}"
    # Recommended: Claude Sonnet 4.5 is the latest and most capable model
    CLAUDE_45_SONNET = f"bedrock/{BedrockModel.CLAUDE_45_SONNET.value}"

    # Open AI Models
    GPT_5 = "openai/gpt-5"
    GPT_5_NANO = "openai/gpt-5-nano"

    # GEMINI Models
    GEMINI_25_FLASH = "google_vertexai/gemini-2.5-flash"

    def __str__(self) -> str:
        """Return the model ID string when converted to string."""
        return self.value


class CustomModelProvider(BaseModel):
    base_url: Optional[str] = Field(
        description="Base URL for custom Open AI model Proxy provider"
    )
    api_key: Optional[str] = Field(
        description="API key for custom Open AI model Proxy provider"
    )
    cert_file: Optional[str] = Field(
        description="mTLS cert file for custom Open AI model Proxy provider"
    )
    key_file: Optional[str] = Field(
        description="mTLS key file for custom Open AI model Proxy provider"
    )

    def __hash__(self):
        return hash((self.base_url, self.api_key, self.cert_file, self.key_file))


def get_litellm_model_env_variable(
    env_var: str, default_model: LiteLLMModel, alternate_env_var: Optional[str] = None
) -> LiteLLMModel | str:
    model_value = os.getenv(env_var)
    if model_value is None and alternate_env_var is not None:
        model_value = os.getenv(alternate_env_var, default_model.value)
    else:
        model_value = model_value or default_model.value
    if not model_value.startswith(("bedrock/", "gemini/", "vertex_ai/")):
        logger.warning(
            f"Invalid model value for {env_var}: {model_value}, using default model: {default_model.value}"
        )
        model_value = default_model.value
    return pydantic.TypeAdapter(LiteLLMModel | str).validate_python(model_value)


def _get_model_value(model: LiteLLMModel | BedrockModel | str) -> str:
    """Convert a model to its string representation."""
    return str(model)


class DocumentationAIConfig(BaseModel):
    model: str = Field(description="Model identifier for documentation generation")
    query_description_model: str = Field(
        description="Model identifier for query description generation"
    )


class ChatAssistantAIConfig(BaseModel):
    model: str = Field(
        description="Model identifier for DataHub chat assistant and chat summary"
    )
    summary_model: str = Field(description="Model identifier for chat summary")
    planning_mode_enabled: bool = Field(description="Whether planning mode is enabled")


class TermSuggestionConfig(BaseModel):
    model: str = Field(
        description="Model identifier for classification/ term suggestion automation"
    )


class ModelConfig(BaseModel):
    """Configuration for AI models used in DataHub Integrations Service."""

    # Documentation Generation AI Configuration
    documentation_ai: DocumentationAIConfig = Field(
        description="Configuration for documentation generation AI"
    )

    # Chat Assistant AI Configuration
    chat_assistant_ai: ChatAssistantAIConfig = Field(
        description="Configuration for chat assistant AI"
    )

    term_suggestion_ai: TermSuggestionConfig = Field(
        description="Configuration for term suggestion"
    )

    custom_model_provider: Optional[CustomModelProvider] = Field(
        description="Configuration for custom model provider"
    )


@functools.lru_cache(maxsize=1)
def get_model_config() -> ModelConfig:
    """Get the model configuration from environment variables (cached)."""
    # Documentation Generation AI Configuration
    docs_ai_config = get_docs_ai_config()

    # Classification Automation AI Configuration
    terms_suggestion_config = get_term_suggestion_config()

    # Chat Assistant AI Configuration
    chat_assistant_config = get_chat_assistant_config()

    # Custom Model Configuration
    custom_model_provider_config = get_custom_model_provider_config()

    config = ModelConfig(
        documentation_ai=docs_ai_config,
        term_suggestion_ai=terms_suggestion_config,
        chat_assistant_ai=chat_assistant_config,
        custom_model_provider=custom_model_provider_config,
    )

    logger.info("AI model configuration: {}", config.model_dump())
    return config


def get_docs_ai_config() -> DocumentationAIConfig:
    doc_model = _get_model_value(
        get_litellm_model_env_variable(
            "DESCRIPTION_GENERATION_MODEL",
            LiteLLMModel.CLAUDE_3_HAIKU,
            "DESCRIPTION_GENERATION_BEDROCK_MODEL",
        )
    )

    # Query Description Generation AI Configuration
    query_desc_model = _get_model_value(
        get_litellm_model_env_variable(
            "QUERY_DESCRIPTION_GENERATION_MODEL",
            LiteLLMModel.CLAUDE_45_SONNET,
            "QUERY_DESCRIPTION_GENERATION_BEDROCK_MODEL",
        )
    )
    docs_ai_config = DocumentationAIConfig(
        model=doc_model, query_description_model=query_desc_model
    )

    return docs_ai_config


def get_term_suggestion_config() -> TermSuggestionConfig:
    term_suggestion_model = _get_model_value(
        get_litellm_model_env_variable(
            "TERM_SUGGESTION_MODEL",
            LiteLLMModel.CLAUDE_45_SONNET,
            "TERM_SUGGESTION_GENERATION_BEDROCK_MODEL",
        )
    )
    terms_suggestion_config = TermSuggestionConfig(model=term_suggestion_model)
    return terms_suggestion_config


def get_chat_assistant_config() -> ChatAssistantAIConfig:
    # We will update CHATBOT_MODEL, CHAT_SUMMARIZATION_MODEL and TERM_SUGGESTION_MODEL
    # when these usecases are implemented with litellm and then populate them
    # in datahub-helm-fork and datahub-apps
    chat_model = _get_model_value(
        get_litellm_model_env_variable(
            "CHATBOT_MODEL",
            LiteLLMModel.CLAUDE_45_SONNET,
        )
    )
    chat_summary_model = _get_model_value(
        get_litellm_model_env_variable(
            "CHAT_SUMMARIZATION_MODEL",
            LiteLLMModel.CLAUDE_45_SONNET,
        )
    )

    planning_mode_enabled = get_boolean_env_variable(
        "CHATBOT_PLANNING_ENABLED",
        True,
    )

    chat_assistant_config = ChatAssistantAIConfig(
        model=chat_model,
        summary_model=chat_summary_model,
        planning_mode_enabled=planning_mode_enabled,
    )

    return chat_assistant_config


@functools.lru_cache(maxsize=1)
def get_custom_model_provider_config() -> CustomModelProvider | None:
    custom_base_url = os.getenv("MODEL_CUSTOM_BASE_URL")
    if custom_base_url is not None:
        api_key = os.getenv("MODEL_CUSTOM_API_KEY")
        cert_file = os.getenv("MODEL_CUSTOM_CERT_FILE")
        key_file = os.getenv("MODEL_CUSTOM_KEY_FILE")
        custom_model_provider = CustomModelProvider(
            base_url=custom_base_url,
            api_key=api_key,
            cert_file=cert_file,
            key_file=key_file,
        )

        return custom_model_provider

    return None


# Module-level instance for all model configurations
model_config: ModelConfig = get_model_config()
