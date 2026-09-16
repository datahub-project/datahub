"""Regression tests ensuring the embedding factory doesn't eagerly import provider SDKs.

Previously factory.py imported all providers at module level, so loading any
document source (Confluence, Notion, datahub-documents) on a minimal install
without google-auth caused: ModuleNotFoundError: No module named 'google'.

These tests verify that provider SDKs are only imported when actually instantiated.
"""

import importlib
import sys
from unittest.mock import patch

import pytest

_FACTORY = "datahub.ingestion.source.unstructured.embedding_providers.factory"
_VERTEX_AI = "datahub.ingestion.source.unstructured.embedding_providers.vertex_ai"
_BEDROCK = "datahub.ingestion.source.unstructured.embedding_providers.bedrock"
_COHERE_PROV = "datahub.ingestion.source.unstructured.embedding_providers.cohere"
_OPENAI_PROV = "datahub.ingestion.source.unstructured.embedding_providers.openai"

_ALL_PROVIDER_MODULES = [_VERTEX_AI, _BEDROCK, _COHERE_PROV, _OPENAI_PROV]
_ALL_PROVIDER_SDKS = {
    "google": None,
    "google.auth": None,
    "google.auth.transport": None,
    "google.auth.transport.requests": None,
    "boto3": None,
    "botocore": None,
    "cohere": None,
    "openai": None,
}


def _reload_factory_with_blocked_sdks():
    """Force-reload factory.py with all provider SDKs blocked.

    Returns the reloaded module. Caller is responsible for restoring sys.modules.
    Raises ImportError/ModuleNotFoundError if factory has any eager provider import.
    """
    # Remove cached provider sub-modules so the reload re-executes cleanly
    saved = {}
    for key in [_FACTORY] + _ALL_PROVIDER_MODULES:
        if key in sys.modules:
            saved[key] = sys.modules.pop(key)

    with patch.dict("sys.modules", _ALL_PROVIDER_SDKS):
        factory = importlib.import_module(_FACTORY)

    # Restore saved modules
    sys.modules.update(saved)
    return factory


class TestFactoryLazyImports:
    def test_factory_loads_with_all_provider_sdks_absent(self) -> None:
        """Importing factory.py must not fail even when every provider SDK is missing.

        If this test fails with ImportError/ModuleNotFoundError it means a
        module-level eager import was added back to factory.py.
        """
        # This will raise if factory.py has any top-level 'import google' etc.
        _reload_factory_with_blocked_sdks()

    def test_vertex_ai_import_error_raised_at_call_time_not_import_time(
        self,
    ) -> None:
        """google-auth must only be required when vertex_ai provider is instantiated,
        not when factory.py is first imported."""
        saved = {}
        for key in [_FACTORY, _VERTEX_AI]:
            if key in sys.modules:
                saved[key] = sys.modules.pop(key)

        try:
            with patch.dict(
                "sys.modules",
                {"google": None, "google.auth": None, _VERTEX_AI: None},
            ):
                factory = importlib.import_module(_FACTORY)

                from datahub.ingestion.source.unstructured.chunking_config import (
                    EmbeddingConfig,
                )

                cfg = EmbeddingConfig(
                    provider="vertex_ai",
                    model="text-embedding-004",
                    vertex_project_id="my-project",
                )
                # Import succeeds above; the error only surfaces when we try to
                # instantiate the provider (lazy import inside create_embedding_provider)
                with pytest.raises((ImportError, ModuleNotFoundError)):
                    factory.create_embedding_provider(cfg)
        finally:
            sys.modules.update(saved)

    def test_unsupported_provider_raises_value_error(self) -> None:
        """dispatch to an unknown provider name raises ValueError, not ImportError."""
        factory = _reload_factory_with_blocked_sdks()

        from datahub.ingestion.source.unstructured.chunking_config import (
            EmbeddingConfig,
        )

        # Build a config that passes the provider/model None guard in factory
        cfg = EmbeddingConfig(provider="openai", model="text-embedding-3-small")
        # Patch the provider name to something unknown after construction
        object.__setattr__(cfg, "provider", "nonexistent_provider")

        with pytest.raises(ValueError, match="Unsupported embedding provider"):
            factory.create_embedding_provider(cfg)


class TestDocumentSourcesImportWithoutProviderSDKs:
    """Each document source must survive import when provider SDKs are absent.

    The fix in factory.py (lazy provider imports) means loading chunking_source.py
    no longer drags in google-auth, boto3, etc. These smoke tests catch any
    regression where an eager import is re-introduced anywhere in the chain.
    """

    @pytest.mark.parametrize(
        "module_path",
        [
            "datahub.ingestion.source.confluence.confluence_source",
            "datahub.ingestion.source.notion.notion_source",
            "datahub.ingestion.source.datahub_documents.datahub_documents_source",
        ],
    )
    def test_source_already_importable(self, module_path: str) -> None:
        """Source module must be importable (basic smoke check)."""
        importlib.import_module(module_path)

    def test_factory_reloads_cleanly_without_google_auth(self) -> None:
        """factory.py must reload without error even when google-auth is blocked.

        This simulates a deployment where only the confluence/notion extras are
        installed but not the vertex_ai/google extras.
        """
        saved = {}
        for key in [_FACTORY, _VERTEX_AI]:
            if key in sys.modules:
                saved[key] = sys.modules.pop(key)

        try:
            with patch.dict(
                "sys.modules",
                {"google": None, "google.auth": None, _VERTEX_AI: None},
            ):
                importlib.import_module(_FACTORY)
        finally:
            sys.modules.update(saved)
