"""Document entity builder for UnstructuredSource."""

import logging
import os
import re
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from datahub.ingestion.source.unstructured.config import (
    DocumentMappingConfig,
    IdNormalizationConfig,
    TitleExtractionConfig,
)
from datahub.metadata.schema_classes import DocumentStateClass
from datahub.sdk.document import Document

logger = logging.getLogger(__name__)


class DocumentIdGenerator:
    """Generates normalized document IDs from templates."""

    def __init__(
        self, pattern: str, normalization: IdNormalizationConfig, source_type: str
    ):
        self.pattern = pattern
        self.normalization = normalization
        self.source_type = source_type

    def generate_id(
        self,
        filename: str,
        directory: str = "",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Generate a normalized document ID from the template pattern."""
        basename = os.path.splitext(filename)[0]
        extension = os.path.splitext(filename)[1].lstrip(".")

        # Build replacement dictionary
        replacements = {
            "source_type": self.source_type,
            "directory": directory.strip("/"),
            "filename": filename,
            "basename": basename,
            "extension": extension,
            "date": datetime.now().strftime("%Y-%m-%d"),
            "uuid": str(uuid.uuid4())[:8],
        }

        # Apply pattern
        doc_id = self.pattern
        for key, value in replacements.items():
            doc_id = doc_id.replace(f"{{{key}}}", value)

        # Normalize
        return self._normalize_id(doc_id)

    def _normalize_id(self, doc_id: str) -> str:
        """Apply normalization rules to document ID."""
        if self.normalization.lowercase:
            doc_id = doc_id.lower()

        if self.normalization.replace_spaces_with:
            doc_id = doc_id.replace(" ", self.normalization.replace_spaces_with)

        if self.normalization.remove_special_chars:
            # Keep only alphanumeric, underscore, hyphen, and forward slash
            doc_id = re.sub(r"[^a-zA-Z0-9_\-/]", "", doc_id)

        # Remove consecutive separators
        doc_id = re.sub(r"[-_/]+", lambda m: m.group(0)[0], doc_id)

        # Trim to max length
        if len(doc_id) > self.normalization.max_length:
            doc_id = doc_id[: self.normalization.max_length]

        return doc_id


class TitleExtractor:
    """Extracts document titles from content or filenames."""

    def __init__(self, config: TitleExtractionConfig):
        self.config = config

    def _extract_title_from_notion_url(self, url: str) -> Optional[str]:
        """Extract human-readable title from Notion URL.

        Notion URLs have format:
        https://www.notion.so/Title-With-Dashes-{32-char-uuid}
        """
        if not url or "notion.so" not in url:
            return None

        # Extract the path after notion.so/
        # Example: "Patching-upsert-in-SDK-v2-2aafc6a6427780d1b553d4afb741bf22"
        match = re.search(r"notion\.so/(?:.*?/)?([^/?#]+)", url)
        if not match:
            return None

        path_segment = match.group(1)

        # Remove the UUID suffix (last 32 hex chars, might be preceded by hyphen)
        # Pattern: anything followed by a hyphen and 32 hex chars at the end
        title_match = re.match(r"^(.+?)-([a-f0-9]{32})$", path_segment, re.IGNORECASE)
        if title_match:
            title_with_dashes = title_match.group(1)
            # Replace hyphens with spaces
            title = title_with_dashes.replace("-", " ")
            return title.strip()

        return None

    def extract_title(
        self,
        elements: List[Dict[str, Any]],
        filename: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Optional[str]:
        """Extract document title using configured strategy."""
        title = None

        # Try Notion URL first if available
        if metadata:
            # Try data_source.url first (where source.py injects Notion URLs)
            data_source = metadata.get("data_source", {})
            url = data_source.get("url")

            # Fall back to additional_metadata.url if not in data_source
            if not url:
                additional_metadata = metadata.get("additional_metadata", {})
                url = additional_metadata.get("url")

            if url:
                title = self._extract_title_from_notion_url(url)
                if title:
                    logger.debug(f"Extracted title from Notion URL: {title}")

        # Then try content extraction
        if not title and self.config.extract_from_content:
            # Find first Title element
            for elem in elements:
                if elem.get("type") == "Title":
                    title = elem.get("text", "").strip()
                    if title:
                        break

        # Finally fall back to filename
        if not title and self.config.fallback_to_filename:
            # Use filename without extension
            title = os.path.splitext(filename)[0]

        if not title:
            title = "Untitled Document"

        # Truncate if too long
        if len(title) > self.config.max_length:
            title = title[: self.config.max_length - 3] + "..."

        return title


class ContentMapper:
    """Maps Unstructured elements to document text content."""

    def extract_text_content(self, elements: List[Dict[str, Any]]) -> str:
        """Extract full text content from all elements."""
        text_parts = []

        for elem in elements:
            elem_type = elem.get("type")
            text = elem.get("text", "").strip()

            if not text:
                continue

            # Add element type markers for structure
            if elem_type in ["Title", "Header"]:
                text_parts.append(f"\n\n## {text}\n")
            elif elem_type == "Table":
                text_parts.append(f"\n\n[TABLE]\n{text}\n[/TABLE]\n")
            else:
                text_parts.append(text)

        return "\n".join(text_parts)


class DocumentEntityBuilder:
    """Builds Document entities from Unstructured output."""

    def __init__(
        self,
        config: DocumentMappingConfig,
        source_type: str,
        actor: str = "urn:li:corpuser:datahub",
    ):
        self.config = config
        self.source_type = source_type
        self.actor = actor

        self.id_generator = DocumentIdGenerator(
            pattern=config.id_pattern,
            normalization=config.id_normalization,
            source_type=source_type,
        )
        self.title_extractor = TitleExtractor(config.title)
        self.content_mapper = ContentMapper()

    def build_document_entity(
        self,
        elements: List[Dict[str, Any]],
        metadata: Dict[str, Any],
        custom_properties: Dict[str, str],
        parent_urn: Optional[str] = None,
        related_assets: Optional[List[str]] = None,
        created_time: Optional[datetime] = None,
        last_modified_time: Optional[datetime] = None,
    ) -> Document:
        """Build a Document entity from Unstructured output."""
        filename = metadata.get("filename", "unknown")
        directory = metadata.get("file_directory", "")

        # Generate document ID
        doc_id = self.id_generator.generate_id(filename, directory, metadata)

        # Extract title
        title = self.title_extractor.extract_title(elements, filename, metadata)
        assert title is not None, "Title extraction should never return None"

        # Extract full text content
        text_content = self.content_mapper.extract_text_content(elements)

        # Get source URL - try url first, then fall back to record_locator path
        data_source = metadata.get("data_source", {})
        source_url = data_source.get("url")
        if not source_url and "record_locator" in data_source:
            # For local files, use the path as the URL
            record_locator = data_source["record_locator"]
            if isinstance(record_locator, dict) and "path" in record_locator:
                source_url = record_locator["path"]

        # Default to doc_id if no URL available (ensures external_url is never None)
        if not source_url:
            source_url = f"{self.source_type}://{doc_id}"

        source_id = source_url if self.config.source.include_external_id else None

        # Determine status
        status = (
            DocumentStateClass.PUBLISHED
            if self.config.status == "PUBLISHED"
            else DocumentStateClass.UNPUBLISHED
        )

        # Note: unstructured_elements are NO LONGER stored in customProperties.
        # Instead, they are processed inline by the source (e.g., NotionSource)
        # which calls DocumentChunkingSource.process_elements_inline() to generate
        # embeddings and emit SemanticContent aspects directly.

        # Create Document using SDK
        # Determine platform from source_type (e.g., "notion", "local", "s3")
        platform = self.source_type

        # Determine external_url based on config
        final_external_url: str
        if self.config.source.include_external_url:
            final_external_url = source_url
        else:
            # Still need to provide a URL (required parameter), use a placeholder
            final_external_url = f"{self.source_type}://{doc_id}"

        doc = Document.create_external_document(
            id=doc_id,
            title=title,
            platform=platform,
            external_url=final_external_url,
            external_id=source_id,
            text=text_content,
            status=status,
            custom_properties=custom_properties,
            parent_document=parent_urn,
            created_time=created_time,
            last_modified_time=last_modified_time,
        )

        return doc

    def build_folder_entity(
        self,
        folder_path: str,
        parent_urn: Optional[str] = None,
    ) -> Document:
        """Build a Document entity representing a folder."""
        folder_name = os.path.basename(folder_path.rstrip("/")) or "root"

        # Generate folder document ID
        doc_id = self.id_generator.generate_id(
            filename="", directory=folder_path, metadata=None
        )

        # Create Document for folder using SDK
        platform = self.source_type

        # Determine external_url based on config (folder_path is always a string)
        final_external_url: str = (
            folder_path
            if self.config.source.include_external_url
            else f"{self.source_type}://{doc_id}"
        )

        doc = Document.create_external_document(
            id=doc_id,
            title=folder_name,
            platform=platform,
            external_url=final_external_url,
            external_id=folder_path,
            text=f"Folder: {folder_name}",
            status=DocumentStateClass.PUBLISHED,
            custom_properties={
                "is_folder": "true",
                "source_type": self.source_type,
                "folder_path": folder_path,
            },
            parent_document=parent_urn,
        )

        return doc
