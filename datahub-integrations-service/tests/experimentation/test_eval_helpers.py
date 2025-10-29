import pathlib

from datahub_integrations.chat.chat_history import (
    AssistantMessage,
    ChatHistory,
    HumanMessage,
    ReasoningMessage,
    ToolCallRequest,
    ToolResult,
)
from datahub_integrations.experimentation.chatbot.eval_helpers import (
    check_for_invalid_links,
    extract_full_datahub_links_from_response,
    extract_response_from_history,
    extract_valid_urns_from_history,
)
from datahub_integrations.experimentation.docs_generation.metrics import extract_links

test_data_dir = pathlib.Path(__file__).parent


def test_extract_response_simple_chat_history() -> None:
    history = ChatHistory(
        messages=[
            HumanMessage(text="Hello"),
            AssistantMessage(text="Hi there! How can I help you?"),
        ]
    )

    result = extract_response_from_history(history)
    assert result == "Hi there! How can I help you?"


def test_extract_response_from_assistant_message_fallback() -> None:
    """Test extraction when LLM uses fallback path (direct AssistantMessage with multiple messages)."""
    # Simulate a conversation with tool calls followed by direct assistant output
    history = ChatHistory(
        messages=[
            HumanMessage(text="What tables do we have?"),
            ReasoningMessage(text="<reasoning>Need to search for tables</reasoning>"),
            ToolCallRequest(
                tool_use_id="tool1", tool_name="search", tool_input={"query": "tables"}
            ),
            ToolResult(
                tool_request=ToolCallRequest(
                    tool_use_id="tool1",
                    tool_name="search",
                    tool_input={"query": "tables"},
                ),
                result={"matches": ["table1", "table2"]},
            ),
            AssistantMessage(text="I found two tables: table1 and table2."),
        ]
    )

    result = extract_response_from_history(history)
    assert result == "I found two tables: table1 and table2."


def test_extract_response_from_chat_history_with_tool_calls() -> None:
    valid_links_file = test_data_dir / "test_valid_links.json"

    history = ChatHistory.load_file(valid_links_file)

    result = extract_response_from_history(history)

    assert result is not None
    assert (
        result
        == "Here's information about the customer analytics resources:\n\n**Dashboard:** The [Customer Analytics Dashboard](https://company.acryl.io/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Atableau%2Ccustomer_analytics_dashboard%2CPROD%29) provides executive-level customer insights.\n\n**Team:** Maintained by the [Analytics Team](https://company.acryl.io/user/urn%3Ali%3Acorpuser%3Aanalytics.team%40company.com) who specializes in customer data analysis.\n\n**Domain:** This dashboard belongs to the [Customer Analytics Domain](https://company.acryl.io/domain/urn%3Ali%3Adomain%3Acustomer-analytics-domain-789) which manages all customer-related analytics assets.\n\n**Data Source:** The dashboard pulls data from the [customer_metrics](https://company.acryl.io/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Asnowflake%2Canalytics.customer_metrics%2CPROD%29) table in Snowflake."
    )


def test_extract_valid_urns_from_history() -> None:
    chat_history_path = test_data_dir / "test_valid_links.json"
    history = ChatHistory.load_file(chat_history_path)

    valid_urns = extract_valid_urns_from_history(history)

    expected_valid_urns = {
        "urn:li:dataset:(urn:li:dataPlatform:tableau,customer_analytics_dashboard,PROD)",
        "urn:li:corpuser:analytics.team@company.com",
        "urn:li:domain:customer-analytics-domain-789",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.customer_metrics,PROD)",
    }
    assert set(valid_urns) == expected_valid_urns


def test_extract_valid_urns_from_history_empty_history() -> None:
    history = ChatHistory(messages=[])

    valid_urns = extract_valid_urns_from_history(history)

    assert valid_urns == []


def test_extract_valid_urns_from_history_no_tool_results() -> None:
    history = ChatHistory(
        messages=[
            HumanMessage(text="Hello"),
            AssistantMessage(text="Hi there! How can I help you?"),
        ]
    )

    valid_urns = extract_valid_urns_from_history(history)

    assert valid_urns == []


def test_extract_links_from_response() -> None:
    response = "Here's the information you requested:\n\n**Dataset:** The [pet_details](https://company.acryl.io/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Asnowflake%2Clong_tail_companions.analytics.pet_details%2CPROD%29) table contains pet adoption information.\n\n**User:** [John Doe](https://company.acryl.io/user/urn%3Ali%3Acorpuser%3Ajohn.doe%40company.com) is a data analyst in our organization.\n\n**Domain:** The [Marketing Domain](https://company.acryl.io/domain/urn%3Ali%3Adomain%3Amarketing-domain-123) manages customer-facing data assets.\n\n**Lineage:** The pet_details dataset is derived from [raw_pets](https://company.acryl.io/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Amysql%2Craw_data.pets%2CPROD%29).\n\nAdditionally, here are some related links:\n- [Invalid Dataset](https://company.acryl.io/dataset/urn%3Ali%3Adataset%3A%28invalid-urn-not-in-history%29)\n- [Unknown User](https://company.acryl.io/user/urn%3Ali%3Acorpuser%3Aunknown.user%40company.com)\n- External link: https://external-site.com/some-page\n- Markdown format: [pet_details_complete](urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.analytics.pet_details_complete,PROD))"

    full_datahub_links = extract_full_datahub_links_from_response(response)

    # Should extract all URNs from the response, including invalid ones
    expected_urns = {
        (
            "pet_details",
            "https://company.acryl.io/dataset/urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.analytics.pet_details,PROD)",
        ),
        (
            "John Doe",
            "https://company.acryl.io/user/urn:li:corpuser:john.doe@company.com",
        ),
        (
            "Marketing Domain",
            "https://company.acryl.io/domain/urn:li:domain:marketing-domain-123",
        ),
        (
            "raw_pets",
            "https://company.acryl.io/dataset/urn:li:dataset:(urn:li:dataPlatform:mysql,raw_data.pets,PROD)",
        ),
        (
            "Invalid Dataset",
            "https://company.acryl.io/dataset/urn:li:dataset:(invalid-urn-not-in-history)",
        ),
        (
            "Unknown User",
            "https://company.acryl.io/user/urn:li:corpuser:unknown.user@company.com",
        ),
    }
    assert set(full_datahub_links) == expected_urns

    urn_only_links = extract_links(response)

    expected_urn_only_links = {
        (
            "pet_details_complete",
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.analytics.pet_details_complete,PROD)",
        ),
    }
    assert set(urn_only_links) == expected_urn_only_links


def test_all_valid_links() -> None:
    valid_links_path = test_data_dir / "test_valid_links.json"
    history = ChatHistory.load_file(valid_links_path)

    valid_urns = extract_valid_urns_from_history(history)

    response = extract_response_from_history(history)
    assert response is not None

    invalid_links = check_for_invalid_links(
        response, valid_urns, "https://company.acryl.io"
    )

    assert invalid_links == []


def test_invalid_links_in_response() -> None:
    valid_links_path = test_data_dir / "test_invalid_links.json"
    history = ChatHistory.load_file(valid_links_path)

    valid_urns = extract_valid_urns_from_history(history)

    response = extract_response_from_history(history)
    assert response is not None

    invalid_links = check_for_invalid_links(
        response, valid_urns, "https://company.acryl.io"
    )

    assert set(invalid_links) == {
        (
            "Unknown User",
            "https://company.acryl.io/user/urn:li:corpuser:unknown.user@company.com",
        ),
        (
            "Invalid Dataset",
            "https://company.acryl.io/dataset/urn:li:dataset:(invalid-urn-not-in-history)",
        ),
        (
            "pet_details_complete",
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.analytics.pet_details_complete,PROD)",
        ),
    }
