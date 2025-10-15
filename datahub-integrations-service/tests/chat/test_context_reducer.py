from typing import List
from unittest.mock import Mock, patch

import pytest

from datahub_integrations.chat.chat_history import (
    AssistantMessage,
    ChatHistory,
    HumanMessage,
    Message,
    ReasoningMessage,
    SummaryMessage,
    ToolCallRequest,
    ToolResult,
)
from datahub_integrations.chat.context_reducer import (
    ContextReducerConfig,
    TokenCountEstimator,
)
from datahub_integrations.chat.reducers.conversation_summarizer import (
    ConversationSummarizer,
)
from datahub_integrations.chat.reducers.sliding_window_reducer import (
    SlidingWindowReducer,
)
from datahub_integrations.gen_ai.model_config import model_config


class TestConversationSummarizer:
    @pytest.fixture
    def token_estimator(self) -> Mock:
        estimator = Mock(spec=TokenCountEstimator)
        estimator.estimate_tokens = Mock(side_effect=lambda text: len(text) // 2)
        return estimator

    @pytest.fixture
    def config(self) -> ContextReducerConfig:
        return ContextReducerConfig(
            llm_token_limit=500,
            safety_buffer=100,
            system_message_tokens=50,
            tool_config_tokens=25,
        )

    @pytest.fixture
    def summarizer(
        self, token_estimator: Mock, config: ContextReducerConfig
    ) -> ConversationSummarizer:
        return ConversationSummarizer(
            token_estimator=token_estimator,
            config=config,
            num_recent_messages_to_keep=3,
            summarization_model=model_config.chat_assistant_ai.summary_model,
        )

    def test_split_at_context_fit_all_messages_fit(
        self, summarizer: ConversationSummarizer
    ) -> None:
        tool_request = ToolCallRequest(
            tool_name="search", tool_use_id="search_1", tool_input={"query": "test"}
        )
        tool_result = ToolResult(tool_request=tool_request, result="Search results")

        messages: List[Message] = [
            HumanMessage(
                text="Can you help me find information about data governance?"
            ),
            AssistantMessage(
                text="I'd be happy to help you with data governance information."
            ),
            tool_request,
            ReasoningMessage(text="Searching for data governance information..."),
            tool_result,
        ]

        split_index = summarizer.split_at_context_fit(messages)

        assert messages[:split_index] == messages
        assert messages[split_index:] == []

    def test_split_at_context_fit_no_messages_fit(
        self, summarizer: ConversationSummarizer
    ) -> None:
        tool_request = ToolCallRequest(
            tool_name="search",
            tool_use_id="search_1",
            tool_input={
                "query": "Can you provide a comprehensive analysis of data governance frameworks, including best practices, implementation strategies, regulatory compliance requirements, and case studies from Fortune 500 companies?"
            },
        )
        tool_result = ToolResult(
            tool_request=tool_request,
            result="Here's a detailed analysis of data governance frameworks including GDPR compliance, data lineage tracking, metadata management, and implementation roadmaps for enterprise organizations.",
        )

        long_messages: List[Message] = [
            HumanMessage(
                text="I need a comprehensive analysis of data governance frameworks including best practices, implementation strategies, regulatory compliance requirements, and case studies from Fortune 500 companies. Please provide detailed insights on GDPR compliance, data lineage tracking, metadata management, and implementation roadmaps for enterprise organizations. This should include detailed analysis of data classification, retention policies, access controls, audit trails, compliance monitoring, risk assessment frameworks, data quality standards, and integration with existing enterprise systems. Additionally, I need information about data privacy regulations, cross-border data transfers, data subject rights, consent management, data breach notification procedures, data retention schedules, data anonymization techniques, data masking strategies, data encryption standards, key management practices, data backup and recovery procedures, disaster recovery planning, business continuity management, data center security, network security protocols, identity and access management, multi-factor authentication, single sign-on implementation, role-based access control, data governance committees, data stewardship programs, data quality metrics, data lineage visualization, metadata cataloging, data discovery tools, data profiling techniques, data validation rules, data cleansing processes, data standardization procedures, master data management, reference data management, data integration patterns, ETL processes, real-time data processing, batch processing workflows, data streaming architectures, microservices data patterns, API management for data access, data versioning strategies, data archiving policies, data lifecycle management, data monetization strategies, data sharing agreements, data licensing models, data marketplace considerations, data ethics frameworks, algorithmic bias detection, fairness in AI systems, explainable AI requirements, model governance, machine learning operations, AI model lifecycle management, continuous learning systems, automated model retraining, model performance monitoring, drift detection, model validation procedures, and comprehensive documentation for all data governance processes."
            ),
            AssistantMessage(
                text="I'll help you with that comprehensive analysis of data governance frameworks, including detailed insights on GDPR compliance, data lineage tracking, metadata management, and implementation roadmaps for enterprise organizations."
            ),
            tool_request,
            ReasoningMessage(
                text="Processing comprehensive data governance analysis request with detailed requirements for enterprise implementation..."
            ),
            tool_result,
        ]

        split_index = summarizer.split_at_context_fit(long_messages)

        assert long_messages[:split_index] == []
        assert long_messages[split_index:] == long_messages

    def test_split_at_context_fit_partial_fit(
        self, summarizer: ConversationSummarizer
    ) -> None:
        tool_request1 = ToolCallRequest(
            tool_name="search",
            tool_use_id="search_1",
            tool_input={"query": "short query"},
        )
        tool_result1 = ToolResult(tool_request=tool_request1, result="Short results")

        tool_request2 = ToolCallRequest(
            tool_name="search",
            tool_use_id="search_2",
            tool_input={
                "query": "What are the latest trends in data quality management, including automated data profiling, real-time monitoring, and machine learning-based anomaly detection?"
            },
        )
        tool_result2 = ToolResult(
            tool_request=tool_request2,
            result="Latest trends include AI-powered data quality scoring, automated data lineage discovery, real-time data validation, and ML-based anomaly detection systems.",
        )

        messages: List[Message] = [
            HumanMessage(text="Can you help me with data quality?"),
            AssistantMessage(text="I'll search for data quality information."),
            tool_request1,
            ReasoningMessage(text="Searching for data quality info..."),
            tool_result1,
            HumanMessage(
                text="I need a comprehensive analysis of data quality management trends including automated data profiling, real-time monitoring systems, machine learning-based anomaly detection, and best practices for implementing data quality frameworks in enterprise environments."
            ),
            AssistantMessage(text="Let me help with that complex request."),
            tool_request2,
            ReasoningMessage(text="Processing complex request..."),
            tool_result2,
        ]

        split_index = summarizer.split_at_context_fit(messages)

        assert len(messages[:split_index]) == 5
        assert messages[:split_index] == messages[:5]
        assert messages[split_index:] == messages[5:]

    def test_split_at_context_fit_single_message_fits(
        self, summarizer: ConversationSummarizer
    ) -> None:
        messages: List[Message] = [HumanMessage(text="Hello")]

        split_index = summarizer.split_at_context_fit(messages)

        assert messages[:split_index] == messages
        assert messages[split_index:] == []

    def test_split_at_context_fit_single_message_doesnt_fit(
        self, summarizer: ConversationSummarizer
    ) -> None:
        messages: List[Message] = [
            HumanMessage(
                text="I need a comprehensive analysis of data governance frameworks including best practices, implementation strategies, regulatory compliance requirements, and case studies from Fortune 500 companies. Please provide detailed insights on GDPR compliance, data lineage tracking, metadata management, and implementation roadmaps for enterprise organizations. This should include detailed analysis of data classification, retention policies, access controls, audit trails, compliance monitoring, risk assessment frameworks, data quality standards, and integration with existing enterprise systems. Additionally, I need information about data privacy regulations, cross-border data transfers, data subject rights, consent management, data breach notification procedures, data retention schedules, data anonymization techniques, data masking strategies, data encryption standards, key management practices, data backup and recovery procedures, disaster recovery planning, business continuity management, data center security, network security protocols, identity and access management, multi-factor authentication, single sign-on implementation, role-based access control, data governance committees, data stewardship programs, data quality metrics, data lineage visualization, metadata cataloging, data discovery tools, data profiling techniques, data validation rules, data cleansing processes, data standardization procedures, master data management, reference data management, data integration patterns, ETL processes, real-time data processing, batch processing workflows, data streaming architectures, microservices data patterns, API management for data access, data versioning strategies, data archiving policies, data lifecycle management, data monetization strategies, data sharing agreements, data licensing models, data marketplace considerations, data ethics frameworks, algorithmic bias detection, fairness in AI systems, explainable AI requirements, model governance, machine learning operations, AI model lifecycle management, continuous learning systems, automated model retraining, model performance monitoring, drift detection, model validation procedures, and comprehensive documentation for all data governance processes."
            )
        ]

        split_index = summarizer.split_at_context_fit(messages)

        assert messages[:split_index] == []
        assert messages[split_index:] == messages

    def test_split_at_context_fit_preserves_order(
        self, summarizer: ConversationSummarizer
    ) -> None:
        tool_request1 = ToolCallRequest(
            tool_name="search",
            tool_use_id="search_1",
            tool_input={"query": "data lineage"},
        )
        tool_result1 = ToolResult(tool_request=tool_request1, result="Lineage results")

        tool_request2 = ToolCallRequest(
            tool_name="search",
            tool_use_id="search_2",
            tool_input={
                "query": "What are the best practices for implementing data lineage tracking in enterprise environments?"
            },
        )
        tool_result2 = ToolResult(
            tool_request=tool_request2,
            result="Best practices include automated lineage discovery, metadata-driven tracking, and integration with data governance frameworks.",
        )

        messages: List[Message] = [
            HumanMessage(text="Can you help me understand data lineage?"),
            AssistantMessage(text="I'll search for data lineage information."),
            tool_request1,
            ReasoningMessage(text="Searching for lineage info..."),
            tool_result1,
            HumanMessage(
                text="I need a comprehensive analysis of data lineage tracking including best practices for implementation in enterprise environments, automated discovery methods, metadata-driven approaches, and integration with data governance frameworks."
            ),
            AssistantMessage(text="Let me help with that complex request."),
            tool_request2,
            ReasoningMessage(text="Processing complex request..."),
            tool_result2,
        ]

        split_index = summarizer.split_at_context_fit(messages)

        assert messages[:split_index] == messages[:5]
        assert messages[split_index:] == messages[5:]

    def test_reduce_with_summarization(
        self, summarizer: ConversationSummarizer
    ) -> None:
        tool_request = ToolCallRequest(
            tool_name="search",
            tool_use_id="search_1",
            tool_input={"query": "data governance"},
        )
        tool_result = ToolResult(
            tool_request=tool_request, result="Data governance results"
        )

        messages: List[Message] = [
            HumanMessage(text="Can you help me with data governance?"),
            AssistantMessage(text="I'll search for data governance information."),
            tool_request,
            ReasoningMessage(text="Searching for data governance info..."),
            tool_result,
            HumanMessage(
                text="I need a comprehensive analysis of data governance frameworks including best practices, implementation strategies, regulatory compliance requirements, and case studies from Fortune 500 companies. Please provide detailed insights on GDPR compliance, data lineage tracking, metadata management, and implementation roadmaps for enterprise organizations."
            ),
            AssistantMessage(text="Let me help with that complex request."),
        ]

        history = ChatHistory(messages=messages)

        with patch.object(
            summarizer,
            "_create_summary_text",
            return_value="Summary of conversation",
        ) as mock_create_summary:
            summarizer.reduce(history)

            mock_create_summary.assert_called_once()
            call_args = mock_create_summary.call_args
            messages_to_summarize = call_args[0][0]
            assert len(messages_to_summarize) == 5
            assert history.reduced_history is not None
            # 3 messages preserved+ tool request adjusted + summary message
            assert len(history.reduced_history) == 5
            assert isinstance(history.reduced_history[0], SummaryMessage)
            assert history.reduced_history[0].text == "Summary of conversation"
            assert history.reduced_history[2:] == messages[4:]

    def test_reduce_no_reduction_needed(
        self, summarizer: ConversationSummarizer
    ) -> None:
        messages: List[Message] = [
            HumanMessage(text="Hello"),
            AssistantMessage(text="Hi there!"),
        ]

        history = ChatHistory(messages=messages)

        summarizer.reduce(history)

        assert history.reduced_history is None
        assert history.context_messages == messages

    def test_create_summary_for_simple_history(
        self, summarizer: ConversationSummarizer
    ) -> None:
        messages: List[Message] = [
            HumanMessage(text="Hello, I need help with data governance."),
            AssistantMessage(text="I'll help you with data governance."),
            HumanMessage(
                text="What are the best practices for data lineage tracking in enterprise environments? This is a very long message that should trigger reduction and require summarization of the conversation history. I need comprehensive information about data governance frameworks, implementation strategies, regulatory compliance requirements, and case studies from Fortune 500 companies. Please provide detailed insights on GDPR compliance, data lineage tracking, metadata management, and implementation roadmaps for enterprise organizations."
            ),
        ]

        history = ChatHistory(messages=messages)

        with patch.object(
            summarizer,
            "_create_summary_text",
            return_value="Summary of conversation",
        ):
            summarizer.reduce(history)

        assert history.reduced_history is not None
        assert len(history.reduced_history) >= 1
        assert isinstance(history.reduced_history[0], SummaryMessage)
        assert history.reduced_history[0].text == "Summary of conversation"
        assert len(history.messages) == len(messages)
        assert history.messages == messages
        assert history.context_messages == history.reduced_history

    def test_update_summary_for_reduced_history_with_summary(
        self, summarizer: ConversationSummarizer
    ) -> None:
        existing_summary = SummaryMessage(
            text="Previous conversation about data governance."
        )
        original_messages: List[Message] = [
            HumanMessage(text="Hello, I need help with data governance."),
            AssistantMessage(text="I'll help you with data governance."),
            HumanMessage(text="What are the best practices for data lineage tracking?"),
            AssistantMessage(
                text="Here are the key best practices for data lineage tracking."
            ),
        ]
        new_messages: List[Message] = [
            HumanMessage(text="Now I need help with data quality management."),
            AssistantMessage(text="I'll help you with data quality."),
            HumanMessage(
                text="What are the latest trends in automated data profiling and real-time monitoring systems for enterprise data quality management? This is a very long message that should trigger reduction. I need comprehensive information about data quality frameworks, implementation strategies, regulatory compliance requirements, and case studies from Fortune 500 companies. Please provide detailed insights on data quality metrics, monitoring systems, and implementation roadmaps for enterprise organizations."
            ),
        ]

        history = ChatHistory(messages=original_messages + new_messages)
        history.set_reduced_history(
            [existing_summary] + new_messages,
            {"reducer_name": "ConversationSummarizer"},
        )

        with patch.object(
            summarizer,
            "_update_summary_text",
            return_value="Summary of conversation",
        ):
            summarizer.reduce(history)

        assert history.reduced_history is not None
        assert len(history.reduced_history) >= 1
        assert isinstance(history.reduced_history[0], SummaryMessage)
        assert history.reduced_history[0].text == "Summary of conversation"

        all_original_messages = original_messages + new_messages
        assert len(history.messages) == len(all_original_messages)
        assert history.messages == all_original_messages
        assert history.context_messages == history.reduced_history

    def test_create_summary_for_reduced_history_without_summary(
        self, summarizer: ConversationSummarizer
    ) -> None:
        messages: List[Message] = [
            HumanMessage(
                text="I need comprehensive help with data governance implementation in our enterprise environment."
            ),
            AssistantMessage(
                text="I'll help you with data governance implementation strategies."
            ),
            HumanMessage(
                text="What are the best practices for data lineage tracking and metadata management in enterprise environments? This is a very long message that should trigger reduction and require summarization of the conversation history. I need comprehensive information about data governance frameworks, implementation strategies, regulatory compliance requirements, and case studies from Fortune 500 companies."
            ),
            AssistantMessage(
                text="Here are the key best practices for data lineage and metadata management."
            ),
            HumanMessage(
                text="Can you provide a comprehensive analysis of GDPR compliance requirements for data processing? This is another very long message that should trigger reduction. I need detailed insights on GDPR compliance, data lineage tracking, metadata management, and implementation roadmaps for enterprise organizations."
            ),
        ]

        history = ChatHistory(messages=messages)

        with patch.object(
            summarizer,
            "_create_summary_text",
            return_value="Summary of conversation",
        ):
            summarizer.reduce(history)

        assert history.reduced_history is not None
        assert len(history.reduced_history) >= 1
        assert isinstance(history.reduced_history[0], SummaryMessage)
        assert history.reduced_history[0].text == "Summary of conversation"

        assert len(history.messages) == len(messages)
        assert history.messages == messages
        assert history.context_messages == history.reduced_history


class TestSlidingWindowReducer:
    @pytest.fixture
    def token_estimator(self) -> Mock:
        estimator = Mock(spec=TokenCountEstimator)
        estimator.estimate_tokens = Mock(side_effect=lambda text: len(text) // 2)
        return estimator

    @pytest.fixture
    def config(self) -> ContextReducerConfig:
        return ContextReducerConfig(
            llm_token_limit=500,
            safety_buffer=100,
            system_message_tokens=50,
            tool_config_tokens=25,
        )

    @pytest.fixture
    def reducer(
        self, token_estimator: Mock, config: ContextReducerConfig
    ) -> SlidingWindowReducer:
        return SlidingWindowReducer(
            token_estimator=token_estimator,
            config=config,
            max_messages=5,
        )

    def test_reduce_with_many_real_messages(
        self, reducer: SlidingWindowReducer
    ) -> None:
        messages: List[Message] = [
            HumanMessage(
                text="I need help with data governance implementation in our enterprise environment."
            ),
            AssistantMessage(
                text="I'll help you with data governance implementation strategies."
            ),
            HumanMessage(
                text="What are the best practices for data lineage tracking and metadata management?"
            ),
            AssistantMessage(
                text="Here are the key best practices for data lineage and metadata management."
            ),
            HumanMessage(
                text="Can you provide a comprehensive analysis of GDPR compliance requirements for data processing?"
            ),
            AssistantMessage(
                text="I'll provide detailed information about GDPR compliance requirements."
            ),
            HumanMessage(
                text="What are the latest trends in data quality management and automated monitoring systems?"
            ),
            AssistantMessage(
                text="Let me share the latest trends in data quality management and monitoring."
            ),
            HumanMessage(
                text="I need guidance on implementing data catalog solutions for enterprise data discovery."
            ),
            AssistantMessage(
                text="I'll help you with data catalog implementation strategies."
            ),
            HumanMessage(
                text="What are the security considerations for data sharing across different business units?"
            ),
            AssistantMessage(
                text="Here are the key security considerations for cross-business unit data sharing."
            ),
        ]

        history = ChatHistory(messages=messages)

        reducer.reduce(history)

        expected_messages = messages[-5:]
        assert history.reduced_history is not None
        assert len(history.reduced_history) == 5
        assert history.reduced_history == expected_messages

    def test_reduce_with_tool_request_adjustment(
        self, reducer: SlidingWindowReducer
    ) -> None:
        messages: List[Message] = [
            HumanMessage(
                text="I need help with data governance implementation in our enterprise environment."
            ),
            AssistantMessage(
                text="I'll help you with data governance implementation strategies."
            ),
            ReasoningMessage(
                text="Analyzing data governance requirements and best practices."
            ),
            ToolCallRequest(
                tool_name="search",
                tool_use_id="search_1",
                tool_input={
                    "query": "data governance best practices enterprise implementation"
                },
            ),
            ToolResult(
                tool_request=ToolCallRequest(
                    tool_name="search",
                    tool_use_id="search_1",
                    tool_input={
                        "query": "data governance best practices enterprise implementation"
                    },
                ),
                result="Here are the key data governance best practices for enterprise implementation.",
            ),
            ToolCallRequest(
                tool_name="search",
                tool_use_id="search_2",
                tool_input={
                    "query": "data governance best practices enterprise implementation"
                },
            ),
            ReasoningMessage(
                text="Analyzing data governance requirements and best practices."
            ),
            ToolResult(
                tool_request=ToolCallRequest(
                    tool_name="search",
                    tool_use_id="search_2",
                    tool_input={
                        "query": "data governance best practices enterprise implementation"
                    },
                ),
                result="Here are the key data governance best practices for enterprise implementation.",
            ),
            AssistantMessage(
                text="Here are the key best practices for data lineage and metadata management."
            ),
        ]

        history = ChatHistory(messages=messages)

        reducer.reduce(history)

        assert history.reduced_history is not None and len(history.reduced_history) == 7

    def test_reduce_no_reduction_needed(self, reducer: SlidingWindowReducer) -> None:
        messages: List[Message] = [
            HumanMessage(text="Hello"),
            AssistantMessage(text="Hi there!"),
        ]

        history = ChatHistory(messages=messages)

        reducer.reduce(history)

        assert history.reduced_history is None
        assert history.context_messages == messages
