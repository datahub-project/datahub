import pathlib

from datahub_integrations.chat.chat_history import (
    AssistantMessage,
    ChatHistory,
    HumanMessage,
)
from datahub_integrations.experimentation.chatbot.eval_helpers import (
    extract_datahub_links_from_response,
    extract_response_from_history,
    extract_urns_from_history,
)

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


def test_extract_response_from_chat_history_with_tool_calls() -> None:
    valid_links_file = test_data_dir / "test_valid_links.json"

    history = ChatHistory.load_file(valid_links_file)

    result = extract_response_from_history(history)

    assert result is not None
    assert (
        result
        == "Here's information about the customer analytics resources:\n\n**Dashboard:** The [Customer Analytics Dashboard](https://company.acryl.io/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Atableau%2Ccustomer_analytics_dashboard%2CPROD%29) provides executive-level customer insights.\n\n**Team:** Maintained by the [Analytics Team](https://company.acryl.io/user/urn%3Ali%3Acorpuser%3Aanalytics.team%40company.com) who specializes in customer data analysis.\n\n**Domain:** This dashboard belongs to the [Customer Analytics Domain](https://company.acryl.io/domain/urn%3Ali%3Adomain%3Acustomer-analytics-domain-789) which manages all customer-related analytics assets.\n\n**Data Source:** The dashboard pulls data from the [customer_metrics](https://company.acryl.io/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Asnowflake%2Canalytics.customer_metrics%2CPROD%29) table in Snowflake."
    )


def test_mixed_valid_invalid_links() -> None:
    """Test response with mix of valid and invalid DataHub links across entity types."""
    # Load chat history with mixed URNs from different tool types
    chat_history_path = test_data_dir / "test_invalid_links.json"
    history = ChatHistory.load_file(chat_history_path)

    # Get response content from the JSON file
    response = extract_response_from_history(history)
    assert response is not None
    valid_urns = extract_urns_from_history(history)
    response_urns = extract_datahub_links_from_response(response)

    # Valid URNs should include all URNs from tool results
    expected_valid_urns = {
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.analytics.pet_details,PROD)",
        "urn:li:corpuser:john.doe@company.com",
        "urn:li:domain:marketing-domain-123",
        "urn:li:dataset:(urn:li:dataPlatform:mysql,raw_data.pets,PROD)",
    }
    assert set(valid_urns) == expected_valid_urns

    # Invalid URNs should include URNs in response that are not in history
    expected_invalid_urns = {
        "urn:li:dataset:(invalid-urn-not-in-history)",
        "urn:li:corpuser:unknown.user@company.com",
    }
    assert set(response_urns) - set(valid_urns) == expected_invalid_urns


def test_no_links_in_response() -> None:
    """Test response with no DataHub links."""
    # Load chat history
    chat_history_path = test_data_dir / "test_no_links.json"
    history = ChatHistory.load_file(chat_history_path)

    # Get response content from the JSON file
    response = extract_response_from_history(history)
    assert response is not None
    assert (
        response
        == "Data governance is a comprehensive framework that encompasses the policies, procedures, and standards for managing data assets throughout their lifecycle. It involves:\n\n**Key Components:**\n- Data quality management and validation\n- Access control and security policies\n- Data lineage and metadata management\n- Compliance with regulatory requirements\n- Data stewardship roles and responsibilities\n\n**Benefits:**\n- Improved data quality and consistency\n- Better regulatory compliance\n- Enhanced data security and privacy\n- Increased trust in data-driven decisions\n- Reduced operational risks\n\nData governance ensures that data is accurate, accessible, consistent, and secure across the organization while meeting business and regulatory requirements."
    )

    valid_urns = extract_urns_from_history(history)
    response_urns = extract_datahub_links_from_response(response)

    # Should have valid URNs from history but no invalid URNs since no links in response
    expected_valid_urns = {
        "urn:li:glossaryTerm:data-governance-term-456",
        "urn:li:corpuser:governance.admin@company.com",
    }
    assert set(valid_urns) == expected_valid_urns
    assert set(response_urns) == set()


def test_all_valid_links() -> None:
    """Test response where all DataHub links are valid."""
    # Load chat history
    chat_history_path = test_data_dir / "test_valid_links.json"
    history = ChatHistory.load_file(chat_history_path)

    # Get response content from the JSON file
    response = extract_response_from_history(history)
    assert response is not None

    valid_urns = extract_urns_from_history(history)
    response_urns = extract_datahub_links_from_response(response)

    # All URNs should be valid
    expected_valid_urns = {
        "urn:li:dataset:(urn:li:dataPlatform:tableau,customer_analytics_dashboard,PROD)",
        "urn:li:corpuser:analytics.team@company.com",
        "urn:li:domain:customer-analytics-domain-789",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.customer_metrics,PROD)",
    }
    assert set(valid_urns) == expected_valid_urns
    assert set(response_urns) - set(valid_urns) == set()
