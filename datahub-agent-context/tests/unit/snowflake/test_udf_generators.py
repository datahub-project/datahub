"""Tests for individual UDF generator functions."""

from typing import Callable

import pytest

from datahub_agent_context.snowflake.udfs.add_glossary_terms import (
    generate_add_glossary_terms_udf,
)
from datahub_agent_context.snowflake.udfs.add_owners import generate_add_owners_udf
from datahub_agent_context.snowflake.udfs.add_structured_properties import (
    generate_add_structured_properties_udf,
)
from datahub_agent_context.snowflake.udfs.add_tags import generate_add_tags_udf
from datahub_agent_context.snowflake.udfs.get_dataset_queries import (
    generate_get_dataset_queries_udf,
)
from datahub_agent_context.snowflake.udfs.get_entities import generate_get_entities_udf
from datahub_agent_context.snowflake.udfs.get_lineage import generate_get_lineage_udf
from datahub_agent_context.snowflake.udfs.get_lineage_paths_between import (
    generate_get_lineage_paths_between_udf,
)
from datahub_agent_context.snowflake.udfs.get_me import generate_get_me_udf
from datahub_agent_context.snowflake.udfs.grep_documents import (
    generate_grep_documents_udf,
)
from datahub_agent_context.snowflake.udfs.list_schema_fields import (
    generate_list_schema_fields_udf,
)
from datahub_agent_context.snowflake.udfs.remove_domains import (
    generate_remove_domains_udf,
)
from datahub_agent_context.snowflake.udfs.remove_glossary_terms import (
    generate_remove_glossary_terms_udf,
)
from datahub_agent_context.snowflake.udfs.remove_owners import (
    generate_remove_owners_udf,
)
from datahub_agent_context.snowflake.udfs.remove_structured_properties import (
    generate_remove_structured_properties_udf,
)
from datahub_agent_context.snowflake.udfs.remove_tags import generate_remove_tags_udf
from datahub_agent_context.snowflake.udfs.search_datahub import (
    generate_search_datahub_udf,
)
from datahub_agent_context.snowflake.udfs.search_documents import (
    generate_search_documents_udf,
)
from datahub_agent_context.snowflake.udfs.set_domains import generate_set_domains_udf
from datahub_agent_context.snowflake.udfs.update_description import (
    generate_update_description_udf,
)


class TestSearchDatahubUdf:
    """Tests for SEARCH_DATAHUB UDF generator."""

    def test_function_signature(self) -> None:
        """Test that SEARCH_DATAHUB has correct signature."""
        result = generate_search_datahub_udf()

        assert (
            "CREATE OR REPLACE FUNCTION SEARCH_DATAHUB(search_query STRING, entity_type STRING)"
            in result
        )
        assert "RETURNS VARIANT" in result

    def test_uses_search_tool(self) -> None:
        """Test that UDF uses datahub_agent_context.mcp_tools.search."""
        result = generate_search_datahub_udf()

        assert "from datahub_agent_context.mcp_tools import search" in result
        assert "return search(" in result

    def test_handles_entity_type_filter(self) -> None:
        """Test that entity_type filtering is implemented."""
        result = generate_search_datahub_udf()

        assert "if entity_type:" in result
        assert 'filters["entity_type"]' in result


class TestGetEntitiesUdf:
    """Tests for GET_ENTITIES UDF generator."""

    def test_function_signature(self) -> None:
        """Test that GET_ENTITIES has correct signature."""
        result = generate_get_entities_udf()

        assert "CREATE OR REPLACE FUNCTION GET_ENTITIES(entity_urn STRING)" in result
        assert "RETURNS VARIANT" in result

    def test_uses_get_entities_tool(self) -> None:
        """Test that UDF uses datahub_agent_context.mcp_tools.get_entities."""
        result = generate_get_entities_udf()

        assert "from datahub_agent_context.mcp_tools import get_entities" in result
        assert "return get_entities(" in result


class TestListSchemaFieldsUdf:
    """Tests for LIST_SCHEMA_FIELDS UDF generator."""

    def test_function_signature(self) -> None:
        """Test that LIST_SCHEMA_FIELDS has correct signature."""
        result = generate_list_schema_fields_udf()

        assert (
            "CREATE OR REPLACE FUNCTION LIST_SCHEMA_FIELDS(dataset_urn STRING, keywords STRING, limit NUMBER)"
            in result
        )
        assert "RETURNS VARIANT" in result

    def test_uses_list_schema_fields_tool(self) -> None:
        """Test that UDF uses datahub_agent_context.mcp_tools.list_schema_fields."""
        result = generate_list_schema_fields_udf()

        assert (
            "from datahub_agent_context.mcp_tools import list_schema_fields" in result
        )
        assert "return list_schema_fields(" in result


class TestGetLineageUdf:
    """Tests for GET_LINEAGE UDF generator."""

    def test_function_signature(self) -> None:
        """Test that GET_LINEAGE has correct signature."""
        result = generate_get_lineage_udf()

        assert (
            "CREATE OR REPLACE FUNCTION GET_LINEAGE(urn STRING, column_name STRING, upstream NUMBER, max_hops NUMBER, max_results NUMBER)"
            in result
        )
        assert "RETURNS VARIANT" in result

    def test_uses_get_lineage_tool(self) -> None:
        """Test that UDF uses datahub_agent_context.mcp_tools.get_lineage."""
        result = generate_get_lineage_udf()

        assert "from datahub_agent_context.mcp_tools import get_lineage" in result
        assert "return get_lineage(" in result


class TestGetLineagePathsBetweenUdf:
    """Tests for GET_LINEAGE_PATHS_BETWEEN UDF generator."""

    def test_function_signature(self) -> None:
        """Test that GET_LINEAGE_PATHS_BETWEEN has correct signature."""
        result = generate_get_lineage_paths_between_udf()

        assert (
            "CREATE OR REPLACE FUNCTION GET_LINEAGE_PATHS_BETWEEN(source_urn STRING, target_urn STRING, source_column STRING, target_column STRING)"
            in result
        )
        assert "RETURNS VARIANT" in result

    def test_uses_lineage_paths_tool(self) -> None:
        """Test that UDF uses datahub_agent_context.mcp_tools.get_lineage_paths_between."""
        result = generate_get_lineage_paths_between_udf()

        assert (
            "from datahub_agent_context.mcp_tools import get_lineage_paths_between"
            in result
        )
        assert "return get_lineage_paths_between(" in result


class TestGetDatasetQueriesUdf:
    """Tests for GET_DATASET_QUERIES UDF generator."""

    def test_function_signature(self) -> None:
        """Test that GET_DATASET_QUERIES has correct signature."""
        result = generate_get_dataset_queries_udf()

        assert (
            "CREATE OR REPLACE FUNCTION GET_DATASET_QUERIES(urn STRING, column_name STRING, source STRING, count NUMBER)"
            in result
        )
        assert "RETURNS VARIANT" in result

    def test_uses_get_dataset_queries_tool(self) -> None:
        """Test that UDF uses datahub_agent_context.mcp_tools.get_dataset_queries."""
        result = generate_get_dataset_queries_udf()

        assert (
            "from datahub_agent_context.mcp_tools import get_dataset_queries" in result
        )
        assert "return get_dataset_queries(" in result


class TestSearchDocumentsUdf:
    """Tests for SEARCH_DOCUMENTS UDF generator."""

    def test_function_signature(self) -> None:
        """Test that SEARCH_DOCUMENTS has correct signature."""
        result = generate_search_documents_udf()

        assert (
            "CREATE OR REPLACE FUNCTION SEARCH_DOCUMENTS(search_query STRING, num_results NUMBER)"
            in result
        )
        assert "RETURNS VARIANT" in result

    def test_uses_search_documents_tool(self) -> None:
        """Test that UDF uses datahub_agent_context.mcp_tools.search_documents."""
        result = generate_search_documents_udf()

        assert "from datahub_agent_context.mcp_tools import search_documents" in result
        assert "return search_documents(" in result


class TestGrepDocumentsUdf:
    """Tests for GREP_DOCUMENTS UDF generator."""

    def test_function_signature(self) -> None:
        """Test that GREP_DOCUMENTS has correct signature."""
        result = generate_grep_documents_udf()

        assert (
            "CREATE OR REPLACE FUNCTION GREP_DOCUMENTS(urns STRING, pattern STRING, context_chars NUMBER, max_matches_per_doc NUMBER)"
            in result
        )
        assert "RETURNS VARIANT" in result

    def test_uses_grep_documents_tool(self) -> None:
        """Test that UDF uses datahub_agent_context.mcp_tools.grep_documents."""
        result = generate_grep_documents_udf()

        assert "from datahub_agent_context.mcp_tools import grep_documents" in result
        assert "return grep_documents(" in result


class TestGetMeUdf:
    """Tests for GET_ME UDF generator."""

    def test_function_signature(self) -> None:
        """Test that GET_ME has correct signature."""
        result = generate_get_me_udf()

        assert "CREATE OR REPLACE FUNCTION GET_ME()" in result
        assert "RETURNS VARIANT" in result

    def test_uses_get_me_tool(self) -> None:
        """Test that UDF uses datahub_agent_context.mcp_tools.get_me."""
        result = generate_get_me_udf()

        assert "from datahub_agent_context.mcp_tools import get_me" in result
        assert "return get_me()" in result


class TestAddTagsUdf:
    """Tests for ADD_TAGS UDF generator."""

    def test_function_signature(self) -> None:
        """Test that ADD_TAGS has correct signature."""
        result = generate_add_tags_udf()

        assert (
            "CREATE OR REPLACE FUNCTION ADD_TAGS(tag_urns STRING, entity_urns STRING, column_paths STRING)"
            in result
        )
        assert "RETURNS VARIANT" in result

    def test_uses_add_tags_tool(self) -> None:
        """Test that UDF uses datahub_agent_context.mcp_tools.add_tags."""
        result = generate_add_tags_udf()

        assert "from datahub_agent_context.mcp_tools import add_tags" in result
        assert "return add_tags(" in result

    def test_handles_json_parsing(self) -> None:
        """Test that UDF handles JSON array parsing."""
        result = generate_add_tags_udf()

        assert "import json" in result
        assert "json.loads(tag_urns)" in result
        assert "json.loads(entity_urns)" in result


class TestRemoveTagsUdf:
    """Tests for REMOVE_TAGS UDF generator."""

    def test_function_signature(self) -> None:
        """Test that REMOVE_TAGS has correct signature."""
        result = generate_remove_tags_udf()

        assert (
            "CREATE OR REPLACE FUNCTION REMOVE_TAGS(tag_urns STRING, entity_urns STRING, column_paths STRING)"
            in result
        )
        assert "RETURNS VARIANT" in result

    def test_uses_remove_tags_tool(self) -> None:
        """Test that UDF uses datahub_agent_context.mcp_tools.remove_tags."""
        result = generate_remove_tags_udf()

        assert "from datahub_agent_context.mcp_tools import remove_tags" in result
        assert "return remove_tags(" in result


class TestUpdateDescriptionUdf:
    """Tests for UPDATE_DESCRIPTION UDF generator."""

    def test_function_signature(self) -> None:
        """Test that UPDATE_DESCRIPTION has correct signature."""
        result = generate_update_description_udf()

        assert (
            "CREATE OR REPLACE FUNCTION UPDATE_DESCRIPTION(entity_urn STRING, operation STRING, description STRING, column_path STRING)"
            in result
        )
        assert "RETURNS VARIANT" in result

    def test_uses_update_description_tool(self) -> None:
        """Test that UDF uses datahub_agent_context.mcp_tools.update_description."""
        result = generate_update_description_udf()

        assert (
            "from datahub_agent_context.mcp_tools import update_description" in result
        )
        assert "return update_description(" in result


class TestSetDomainsUdf:
    """Tests for SET_DOMAINS UDF generator."""

    def test_function_signature(self) -> None:
        """Test that SET_DOMAINS has correct signature."""
        result = generate_set_domains_udf()

        assert (
            "CREATE OR REPLACE FUNCTION SET_DOMAINS(domain_urn STRING, entity_urns STRING)"
            in result
        )
        assert "RETURNS VARIANT" in result

    def test_uses_set_domains_tool(self) -> None:
        """Test that UDF uses datahub_agent_context.mcp_tools.set_domains."""
        result = generate_set_domains_udf()

        assert "from datahub_agent_context.mcp_tools import set_domains" in result
        assert "return set_domains(" in result


class TestRemoveDomainsUdf:
    """Tests for REMOVE_DOMAINS UDF generator."""

    def test_function_signature(self) -> None:
        """Test that REMOVE_DOMAINS has correct signature."""
        result = generate_remove_domains_udf()

        assert "CREATE OR REPLACE FUNCTION REMOVE_DOMAINS(entity_urns STRING)" in result
        assert "RETURNS VARIANT" in result

    def test_uses_remove_domains_tool(self) -> None:
        """Test that UDF uses datahub_agent_context.mcp_tools.remove_domains."""
        result = generate_remove_domains_udf()

        assert "from datahub_agent_context.mcp_tools import remove_domains" in result
        assert "return remove_domains(" in result


class TestAddOwnersUdf:
    """Tests for ADD_OWNERS UDF generator."""

    def test_function_signature(self) -> None:
        """Test that ADD_OWNERS has correct signature."""
        result = generate_add_owners_udf()

        assert (
            "CREATE OR REPLACE FUNCTION ADD_OWNERS(owner_urns STRING, entity_urns STRING, ownership_type_urn STRING)"
            in result
        )
        assert "RETURNS VARIANT" in result

    def test_uses_add_owners_tool(self) -> None:
        """Test that UDF uses datahub_agent_context.mcp_tools.add_owners."""
        result = generate_add_owners_udf()

        assert "from datahub_agent_context.mcp_tools import add_owners" in result
        assert "return add_owners(" in result


class TestRemoveOwnersUdf:
    """Tests for REMOVE_OWNERS UDF generator."""

    def test_function_signature(self) -> None:
        """Test that REMOVE_OWNERS has correct signature."""
        result = generate_remove_owners_udf()

        assert (
            "CREATE OR REPLACE FUNCTION REMOVE_OWNERS(owner_urns STRING, entity_urns STRING, ownership_type_urn STRING)"
            in result
        )
        assert "RETURNS VARIANT" in result

    def test_uses_remove_owners_tool(self) -> None:
        """Test that UDF uses datahub_agent_context.mcp_tools.remove_owners."""
        result = generate_remove_owners_udf()

        assert "from datahub_agent_context.mcp_tools import remove_owners" in result
        assert "return remove_owners(" in result


class TestAddGlossaryTermsUdf:
    """Tests for ADD_GLOSSARY_TERMS UDF generator."""

    def test_function_signature(self) -> None:
        """Test that ADD_GLOSSARY_TERMS has correct signature."""
        result = generate_add_glossary_terms_udf()

        assert (
            "CREATE OR REPLACE FUNCTION ADD_GLOSSARY_TERMS(term_urns STRING, entity_urns STRING, column_paths STRING)"
            in result
        )
        assert "RETURNS VARIANT" in result

    def test_uses_add_glossary_terms_tool(self) -> None:
        """Test that UDF uses datahub_agent_context.mcp_tools.add_glossary_terms."""
        result = generate_add_glossary_terms_udf()

        assert (
            "from datahub_agent_context.mcp_tools import add_glossary_terms" in result
        )
        assert "return add_glossary_terms(" in result


class TestRemoveGlossaryTermsUdf:
    """Tests for REMOVE_GLOSSARY_TERMS UDF generator."""

    def test_function_signature(self) -> None:
        """Test that REMOVE_GLOSSARY_TERMS has correct signature."""
        result = generate_remove_glossary_terms_udf()

        assert (
            "CREATE OR REPLACE FUNCTION REMOVE_GLOSSARY_TERMS(term_urns STRING, entity_urns STRING, column_paths STRING)"
            in result
        )
        assert "RETURNS VARIANT" in result

    def test_uses_remove_glossary_terms_tool(self) -> None:
        """Test that UDF uses datahub_agent_context.mcp_tools.remove_glossary_terms."""
        result = generate_remove_glossary_terms_udf()

        assert (
            "from datahub_agent_context.mcp_tools import remove_glossary_terms"
            in result
        )
        assert "return remove_glossary_terms(" in result


class TestAddStructuredPropertiesUdf:
    """Tests for ADD_STRUCTURED_PROPERTIES UDF generator."""

    def test_function_signature(self) -> None:
        """Test that ADD_STRUCTURED_PROPERTIES has correct signature."""
        result = generate_add_structured_properties_udf()

        assert (
            "CREATE OR REPLACE FUNCTION ADD_STRUCTURED_PROPERTIES(property_values STRING, entity_urns STRING)"
            in result
        )
        assert "RETURNS VARIANT" in result

    def test_uses_add_structured_properties_tool(self) -> None:
        """Test that UDF uses datahub_agent_context.mcp_tools.add_structured_properties."""
        result = generate_add_structured_properties_udf()

        assert (
            "from datahub_agent_context.mcp_tools import add_structured_properties"
            in result
        )
        assert "return add_structured_properties(" in result


class TestRemoveStructuredPropertiesUdf:
    """Tests for REMOVE_STRUCTURED_PROPERTIES UDF generator."""

    def test_function_signature(self) -> None:
        """Test that REMOVE_STRUCTURED_PROPERTIES has correct signature."""
        result = generate_remove_structured_properties_udf()

        assert (
            "CREATE OR REPLACE FUNCTION REMOVE_STRUCTURED_PROPERTIES(property_urns STRING, entity_urns STRING)"
            in result
        )
        assert "RETURNS VARIANT" in result

    def test_uses_remove_structured_properties_tool(self) -> None:
        """Test that UDF uses datahub_agent_context.mcp_tools.remove_structured_properties."""
        result = generate_remove_structured_properties_udf()

        assert (
            "from datahub_agent_context.mcp_tools import remove_structured_properties"
            in result
        )
        assert "return remove_structured_properties(" in result


class TestAllUdfsHaveErrorHandling:
    """Tests that all UDFs have proper error handling."""

    @pytest.mark.parametrize(
        "generator_func",
        [
            generate_search_datahub_udf,
            generate_get_entities_udf,
            generate_list_schema_fields_udf,
            generate_get_lineage_udf,
            generate_get_lineage_paths_between_udf,
            generate_get_dataset_queries_udf,
            generate_search_documents_udf,
            generate_grep_documents_udf,
            generate_get_me_udf,
            generate_add_tags_udf,
            generate_remove_tags_udf,
            generate_update_description_udf,
            generate_set_domains_udf,
            generate_remove_domains_udf,
            generate_add_owners_udf,
            generate_remove_owners_udf,
            generate_add_glossary_terms_udf,
            generate_remove_glossary_terms_udf,
            generate_add_structured_properties_udf,
            generate_remove_structured_properties_udf,
        ],
    )
    def test_udf_has_exception_handling(
        self, generator_func: Callable[[], str]
    ) -> None:
        """Test that UDF has try/except error handling."""
        result = generator_func()

        assert "try:" in result
        assert "except Exception as e:" in result
        assert "'success': False" in result
        assert "'error': str(e)" in result


class TestAllUdfsUseDataHubContext:
    """Tests that all UDFs use DataHubContext properly."""

    @pytest.mark.parametrize(
        "generator_func",
        [
            generate_search_datahub_udf,
            generate_get_entities_udf,
            generate_list_schema_fields_udf,
            generate_get_lineage_udf,
            generate_get_lineage_paths_between_udf,
            generate_get_dataset_queries_udf,
            generate_search_documents_udf,
            generate_grep_documents_udf,
            generate_get_me_udf,
            generate_add_tags_udf,
            generate_remove_tags_udf,
            generate_update_description_udf,
            generate_set_domains_udf,
            generate_remove_domains_udf,
            generate_add_owners_udf,
            generate_remove_owners_udf,
            generate_add_glossary_terms_udf,
            generate_remove_glossary_terms_udf,
            generate_add_structured_properties_udf,
            generate_remove_structured_properties_udf,
        ],
    )
    def test_udf_uses_datahub_context(self, generator_func: Callable[[], str]) -> None:
        """Test that UDF uses DataHubContext for API calls."""
        result = generator_func()

        assert "_snowflake.get_generic_secret_string('datahub_url_secret')" in result
        assert "_snowflake.get_generic_secret_string('datahub_token_secret')" in result
        assert "DataHubClient(" in result
        assert "with DataHubContext(client):" in result
