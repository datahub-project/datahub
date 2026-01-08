"""OpenAI LLM wrapper implementation."""

import os
import sys
import threading
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import httpx
import openai

# inotify is Linux-only, conditionally import it
if sys.platform == "linux":
    import inotify  # type: ignore[import-untyped]
    import inotify.adapters  # type: ignore[import-untyped]
else:
    inotify = None  # type: ignore[assignment]
from datahub.cli.env_utils import get_boolean_env_variable
from datahub.utilities.perf_timer import PerfTimer
from langchain_openai import ChatOpenAI
from loguru import logger
from pydantic import SecretStr

from datahub_integrations.gen_ai.llm.base import LLMWrapper
from datahub_integrations.gen_ai.llm.exceptions import (
    LlmAuthenticationException,
    LlmInputTooLongException,
    LlmRateLimitException,
    LlmValidationException,
)
from datahub_integrations.gen_ai.llm.types import ConverseResponse
from datahub_integrations.observability.decorators import otel_llm_call
from datahub_integrations.observability.metrics_constants import AIModule

if TYPE_CHECKING:
    from mypy_boto3_bedrock_runtime.type_defs import (
        MessageUnionTypeDef,
        SystemContentBlockTypeDef,
    )


class CustomOpenAIProxyLLMWrapper(LLMWrapper):
    """Custom OpenAI Proxy LLM wrapper using langchain."""

    provider_name = "custom_openai_proxy"

    def _initialize_client(self) -> Any:
        """Initialize Custom OpenAI Proxy client via langchain."""

        openai_api_key = None
        custom_client = None
        custom_async_client = None
        custom_base_url = None
        if not self.custom_model_provider:
            raise ValueError(
                "custom_model_provider provider is required for CustomOpenAIProxyLLMWrapper provider"
            )

        # Check if force reinitialize mode is enabled
        # This is to guard against caching issues in the client
        force_reinitialize = get_boolean_env_variable(
            "FORCE_CUSTOM_AI_CLIENT_REINITIALIZE"
        )

        # Prepare custom headers if provided
        custom_headers = None
        if self.custom_model_provider.custom_headers:
            custom_headers = self.custom_model_provider.custom_headers

        # create a custom http client if cert file and key file exist
        if self.custom_model_provider.cert_file and self.custom_model_provider.key_file:
            # Only set watches once (use instance variable to avoid cross-test pollution)
            if not hasattr(self, "cert_file_watches"):
                self.cert_file_watches: List[str] = []

            # Skip background watching if force reinitialize is enabled
            if len(self.cert_file_watches) == 0 and not force_reinitialize:
                cert_file_watch_path = str(
                    Path(self.custom_model_provider.cert_file).parent
                )
                self.cert_file_watches.append(cert_file_watch_path)
                self._start_background_watch(cert_file_watch_path)

                key_file_watch_path = str(
                    Path(self.custom_model_provider.key_file).parent
                )
                if cert_file_watch_path != key_file_watch_path:
                    self.cert_file_watches.append(key_file_watch_path)
                    self._start_background_watch(key_file_watch_path)

            custom_client = httpx.Client(
                cert=(
                    self.custom_model_provider.cert_file,
                    self.custom_model_provider.key_file,
                ),
                headers=custom_headers,
            )
            custom_async_client = httpx.AsyncClient(
                cert=(
                    self.custom_model_provider.cert_file,
                    self.custom_model_provider.key_file,
                ),
                headers=custom_headers,
            )
        elif custom_headers:
            # Create custom http clients with just headers (no mTLS)
            custom_client = httpx.Client(headers=custom_headers)
            custom_async_client = httpx.AsyncClient(headers=custom_headers)

        # Intentionally creating has_custom_openai_api_key to avoid accidentally logging the api key
        has_custom_openai_api_key = False
        if self.custom_model_provider.api_key:
            openai_api_key = str(self.custom_model_provider.api_key)
            has_custom_openai_api_key = True

        if self.custom_model_provider.base_url:
            custom_base_url = self.custom_model_provider.base_url

        # Get API key from environment if not already present
        if not openai_api_key:
            openai_api_key = os.environ.get("OPENAI_API_KEY")

        if not openai_api_key:
            raise ValueError(
                "OPENAI_API_KEY environment variable or custom model provider API key is required for OpenAI provider"
            )

        logger.info(
            f"Initializing CustomOpenAIProxyLLMWrapper with model={self.model_name}, \
            custom_base_url={custom_base_url}, \
            has_custom_openai_api_key={has_custom_openai_api_key}, \
            has_custom_http_client={custom_client is not None}, \
            cert_file={self.custom_model_provider.cert_file}, \
            key_file={self.custom_model_provider.key_file}, \
            has_custom_headers={custom_headers is not None}, \
            force_reinitialize={force_reinitialize}"
        )

        return ChatOpenAI(
            model=self.model_name,
            api_key=SecretStr(openai_api_key),  # Wrap in SecretStr for type safety
            http_client=custom_client,
            http_async_client=custom_async_client,  # Langchain requires a separate custom http client for async calls
            base_url=custom_base_url,
            temperature=0.5,  # Default, can be overridden per-request via invoke kwargs
            timeout=self.read_timeout,
            max_retries=self.max_attempts,
            # Note: max_tokens is not set here - it's passed to invoke() per-request
            # use_responses_api=True,
        )

    @property
    def exceptions(self) -> Any:
        """Get OpenAI-specific exception classes."""
        try:
            import openai

            return openai
        except ImportError:
            return Exception

    def _start_background_watch(self, watch_path: str) -> None:
        """Start watching a directory for certificate changes in a background thread."""
        thread = threading.Thread(
            target=self._watch_cert_directory_sync,
            args=(watch_path,),
            daemon=True,  # Daemon thread won't prevent program exit
            name=f"cert-watch-{watch_path}",
        )
        thread.start()
        logger.info(f"Started background certificate watch thread for: {watch_path}")

    def _watch_cert_directory_sync(self, watch_path: str) -> None:
        """Watch a directory for certificate file changes (synchronous, runs in thread).

        Only works on Linux systems. On other platforms, this is a no-op.
        Nobody should be running outside of Linux.
        Most users should not be using watches to reset certs with live updates.
        Ideally they would restart containers.
        """
        if inotify is None:
            logger.warning(
                f"Certificate file watching is not supported on {sys.platform}. "
                "Cert file changes will not trigger automatic client reinitialization."
            )
            return

        if not os.path.exists(watch_path):
            os.makedirs(watch_path)
            logger.info(f"Created directory: {watch_path}")

        watch_inotify_tree = inotify.adapters.InotifyTree(watch_path)
        logger.info(f"Watching directory tree for cert changes: {watch_path}.")

        try:
            for event in watch_inotify_tree.event_gen(yield_nones=False):
                (_, type_names, path, filename) = event
                # Only look at events for write close after a write is completed.
                if "IN_CLOSE_WRITE" in type_names:
                    # re initialize the client to fully ensure we have the correct certs
                    logger.info(
                        f"Certs updated for path:, {path}, file_name: {filename}. Updating Client.",
                    )
                    self._client = self._initialize_client()
        except Exception as e:
            logger.exception(
                f"Got exception in watch loop when trying to re-initialize certs: {e}"
            )

    @otel_llm_call(ai_module_param="ai_module")
    def converse(
        self,
        *,
        system: List["SystemContentBlockTypeDef"],
        messages: List["MessageUnionTypeDef"],
        ai_module: AIModule,
        toolConfig: Optional[Dict[str, Any]] = None,
        inferenceConfig: Optional[Dict[str, Any]] = None,
    ) -> ConverseResponse:
        """
        Call OpenAI via langchain, translating from Bedrock format to langchain format.

        High-level transformation flow:
        1. Convert Bedrock messages -> langchain messages
        2. Convert Bedrock tools -> langchain tools
        3. Call OpenAI via langchain
        4. Convert langchain response -> Bedrock format

        Handles:
        - Message format conversion (Bedrock multi-block -> langchain single content)
        - Tool calling (Bedrock toolSpec -> langchain function format)
        - Prompt caching (Bedrock cachePoint markers -> OpenAI automatic caching)
        - Response format conversion (langchain AIMessage -> Bedrock output structure)
        """
        # Force client reinitialization if environment variable is set
        force_reinitialize = get_boolean_env_variable("FORCE_CLIENT_REINITIALIZE")
        if force_reinitialize:
            logger.info(
                "Force reinitializing client due to FORCE_CLIENT_REINITIALIZE env var"
            )
            self._client = self._initialize_client()

        # STEP 1 & 2: Convert Bedrock messages to langchain format
        # Use shared helper from base class
        lc_messages = self._convert_bedrock_messages_to_langchain(system, messages)

        # STEP 3: Log before API call with structured fields
        logger.info(
            "Calling OpenAI LLM (streaming)",
            extra={
                "provider": "openai",
                "model": self.model_name,
                "temperature": inferenceConfig.get("temperature")
                if inferenceConfig
                else None,
                "max_tokens": inferenceConfig.get("maxTokens")
                if inferenceConfig
                else None,
            },
        )

        try:
            # Time the entire API call
            with PerfTimer() as timer:
                # Use shared langchain streaming helper from base class
                # This handles: inference config mapping, tool conversion, cachePoint filtering,
                # and streaming with langchain's built-in chunk combining
                response = self._invoke_with_langchain(
                    lc_messages, toolConfig, inferenceConfig
                )

            # Extract token usage for logging (may be None)
            usage_metadata = getattr(response, "usage_metadata", None)
            if usage_metadata:
                input_tokens = usage_metadata.get("input_tokens", 0)
                output_tokens = usage_metadata.get("output_tokens", 0)
                total_tokens = usage_metadata.get("total_tokens", 0)
            else:
                input_tokens = output_tokens = total_tokens = 0

            # Extract response metadata
            response_metadata = getattr(response, "response_metadata", {})

            # Log after API call with structured fields
            logger.info(
                "Custom OpenAI Proxy LLM call completed (streaming)",
                extra={
                    "provider": self.provider_name,
                    "model": self.model_name,
                    "duration_seconds": round(timer.elapsed_seconds(), 3),
                    "input_tokens": input_tokens,
                    "output_tokens": output_tokens,
                    "total_tokens": total_tokens,
                    "has_content": bool(response.content),
                    "content_length": len(response.content) if response.content else 0,
                    "finish_reason": response_metadata.get("finish_reason", "N/A"),
                },
            )

            # Track cost for observability (shared helper from base class)
            self._record_langchain_usage(response, ai_module)

        except Exception as e:
            # Translate provider-specific exceptions to standardized LLM exceptions
            #
            # Note: Langchain doesn't wrap provider exceptions - they bubble through directly.
            # We catch openai.* exceptions directly (not wrapped in langchain exceptions).
            # Verified: openai.AuthenticationError, openai.RateLimitError, etc. are raised as-is.
            if isinstance(e, openai.AuthenticationError):
                # API key invalid or missing
                raise LlmAuthenticationException(str(e)) from e
            elif isinstance(e, openai.RateLimitError):
                # Rate limit exceeded (too many requests or tokens)
                raise LlmRateLimitException(str(e)) from e
            elif isinstance(e, openai.BadRequestError):
                # Invalid request - check if it's a context length error
                error_msg = str(e)
                if (
                    "context_length_exceeded" in error_msg
                    or "maximum context length" in error_msg
                ):
                    raise LlmInputTooLongException(error_msg) from e
                else:
                    # Other bad request errors (invalid parameters, etc.)
                    raise LlmValidationException(error_msg) from e
            else:
                # Unknown error type - re-raise as-is for debugging
                # This could be network errors, timeouts, or new OpenAI exception types
                raise

        # STEP 5: Convert langchain response back to Bedrock format
        # Langchain returns: AIMessage(content="...", tool_calls=[...], usage_metadata={...})
        # Bedrock expects: {"output": {"message": {...}}, "stopReason": "...", "usage": {...}}
        # This conversion is done in _convert_langchain_response_to_bedrock() (see base.py)
        return self._convert_langchain_response_to_bedrock(response)
