"""Unit tests for get_entities with Document entity support."""

from datahub_integrations.mcp.mcp_server import (
    DOCUMENT_CONTENT_CHAR_LIMIT,
    clean_get_entities_response,
)


class TestGetEntitiesDocuments:
    """Tests for Document entity handling in get_entities."""

    def test_document_content_not_truncated_when_small(self):
        """Test that small document content is not truncated."""
        raw_response = {
            "urn": "urn:li:document:doc1",
            "info": {
                "title": "Test Document",
                "contents": {"text": "This is a short document."},
            },
        }

        result = clean_get_entities_response(raw_response)

        assert result["info"]["contents"]["text"] == "This is a short document."
        assert "_truncated" not in result["info"]["contents"]

    def test_document_content_truncated_when_large(self):
        """Test that large document content is truncated with message."""
        large_content = "A" * (DOCUMENT_CONTENT_CHAR_LIMIT + 1000)
        raw_response = {
            "urn": "urn:li:document:doc1",
            "info": {"title": "Large Document", "contents": {"text": large_content}},
        }

        result = clean_get_entities_response(raw_response)

        # Should be truncated
        assert len(result["info"]["contents"]["text"]) < len(large_content)
        # Should end with truncation message including start_offset hint
        assert (
            "[Content truncated. Use grep_documents(start_offset="
            in result["info"]["contents"]["text"]
        )
        assert "to continue.]" in result["info"]["contents"]["text"]
        # Should have truncated flag, original length, and truncation position
        assert result["info"]["contents"]["_truncated"] is True
        assert result["info"]["contents"]["_originalLengthChars"] == len(large_content)
        assert (
            result["info"]["contents"]["_truncatedAtChar"]
            == DOCUMENT_CONTENT_CHAR_LIMIT
        )

    def test_document_content_exactly_at_limit(self):
        """Test document content exactly at limit is not truncated."""
        exact_content = "B" * DOCUMENT_CONTENT_CHAR_LIMIT
        raw_response = {
            "urn": "urn:li:document:doc1",
            "info": {
                "title": "Exact Limit Document",
                "contents": {"text": exact_content},
            },
        }

        result = clean_get_entities_response(raw_response)

        # Should not be truncated
        assert result["info"]["contents"]["text"] == exact_content
        assert "_truncated" not in result["info"]["contents"]

    def test_document_without_contents(self):
        """Test document without contents field."""
        raw_response = {
            "urn": "urn:li:document:doc1",
            "info": {"title": "Empty Document", "contents": None},
        }

        result = clean_get_entities_response(raw_response)

        # Should handle gracefully (contents becomes None after clean_gql_response)
        assert result["urn"] == "urn:li:document:doc1"

    def test_document_with_empty_text(self):
        """Test document with empty text content."""
        raw_response = {
            "urn": "urn:li:document:doc1",
            "info": {"title": "Empty Text Document", "contents": {"text": ""}},
        }

        result = clean_get_entities_response(raw_response)

        # Should handle gracefully
        assert result["urn"] == "urn:li:document:doc1"

    def test_non_document_entity_not_affected(self):
        """Test that non-document entities with 'info' field are not affected."""
        # Some entities have 'info' but not 'contents' - should not error
        raw_response = {
            "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,test,PROD)",
            "info": {"description": "A test dataset"},
        }

        result = clean_get_entities_response(raw_response)

        # Should not have truncation applied
        assert result["info"]["description"] == "A test dataset"

    def test_truncation_preserves_document_structure(self):
        """Test that truncation preserves other document fields."""
        large_content = "C" * (DOCUMENT_CONTENT_CHAR_LIMIT + 500)
        raw_response = {
            "urn": "urn:li:document:doc1",
            "subType": "Runbook",
            "platform": {"urn": "urn:li:dataPlatform:notion", "name": "Notion"},
            "info": {
                "title": "Important Runbook",
                "contents": {"text": large_content},
                "source": {
                    "sourceType": "EXTERNAL",
                    "externalUrl": "https://notion.so/doc1",
                },
            },
            "tags": {"tags": [{"tag": {"urn": "urn:li:tag:critical"}}]},
        }

        result = clean_get_entities_response(raw_response)

        # Content should be truncated
        assert result["info"]["contents"]["_truncated"] is True
        assert result["info"]["contents"]["_originalLengthChars"] == len(large_content)
        assert (
            result["info"]["contents"]["_truncatedAtChar"]
            == DOCUMENT_CONTENT_CHAR_LIMIT
        )

        # Other fields should be preserved
        assert result["subType"] == "Runbook"
        assert result["platform"]["name"] == "Notion"
        assert result["info"]["title"] == "Important Runbook"
        assert result["info"]["source"]["sourceType"] == "EXTERNAL"
        assert len(result["tags"]["tags"]) == 1
