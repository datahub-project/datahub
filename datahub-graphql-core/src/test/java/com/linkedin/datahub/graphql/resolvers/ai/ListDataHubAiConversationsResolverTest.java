package com.linkedin.datahub.graphql.resolvers.ai;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.conversation.DataHubAiConversationInfo;
import com.linkedin.conversation.DataHubAiConversationMessageArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataHubAiConversationConnection;
import com.linkedin.metadata.service.DataHubAiConversationService;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class ListDataHubAiConversationsResolverTest {

  private static final Urn TEST_USER_URN = UrnUtils.getUrn("urn:li:corpuser:testUser");
  private static final Urn TEST_CONVERSATION_URN_1 =
      UrnUtils.getUrn("urn:li:dataHubConversation:12345");
  private static final Urn TEST_CONVERSATION_URN_2 =
      UrnUtils.getUrn("urn:li:dataHubConversation:67890");

  @Test
  public void testListConversationsSuccess() throws Exception {
    // Create mock service
    DataHubAiConversationService mockService = mock(DataHubAiConversationService.class);
    when(mockService.listConversations(
            any(OperationContext.class), any(Urn.class), any(Integer.class), any(Integer.class)))
        .thenReturn(
            new DataHubAiConversationService.ConversationListResult(
                ImmutableList.of(
                    new DataHubAiConversationService.ConversationResult(
                        TEST_CONVERSATION_URN_1, createMockConversationInfo()),
                    new DataHubAiConversationService.ConversationResult(
                        TEST_CONVERSATION_URN_2, createMockConversationInfo())),
                2));

    // Create resolver
    ListDataHubAiConversationsResolver resolver =
        new ListDataHubAiConversationsResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext(TEST_USER_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("count"))).thenReturn(20);
    Mockito.when(mockEnv.getArgument(Mockito.eq("start"))).thenReturn(0);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    DataHubAiConversationConnection result = resolver.get(mockEnv).get();

    // Verify result
    assertNotNull(result);
    assertEquals(result.getConversations().size(), 2);
    assertEquals(result.getTotal(), 2);

    // Verify service was called
    verify(mockService, times(1))
        .listConversations(any(OperationContext.class), eq(TEST_USER_URN), eq(20), eq(0));
  }

  @Test
  public void testListConversationsEmpty() throws Exception {
    // Create mock service
    DataHubAiConversationService mockService = mock(DataHubAiConversationService.class);
    when(mockService.listConversations(
            any(OperationContext.class), any(Urn.class), any(Integer.class), any(Integer.class)))
        .thenReturn(new DataHubAiConversationService.ConversationListResult(ImmutableList.of(), 0));

    // Create resolver
    ListDataHubAiConversationsResolver resolver =
        new ListDataHubAiConversationsResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext(TEST_USER_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("count"))).thenReturn(20);
    Mockito.when(mockEnv.getArgument(Mockito.eq("start"))).thenReturn(0);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    DataHubAiConversationConnection result = resolver.get(mockEnv).get();

    // Verify result
    assertNotNull(result);
    assertEquals(result.getConversations().size(), 0);
    assertEquals(result.getTotal(), 0);
  }

  @Test
  public void testListConversationsDefaultPagination() throws Exception {
    // Create mock service
    DataHubAiConversationService mockService = mock(DataHubAiConversationService.class);
    when(mockService.listConversations(
            any(OperationContext.class), any(Urn.class), any(Integer.class), any(Integer.class)))
        .thenReturn(new DataHubAiConversationService.ConversationListResult(ImmutableList.of(), 0));

    // Create resolver
    ListDataHubAiConversationsResolver resolver =
        new ListDataHubAiConversationsResolver(mockService);

    // Execute resolver with no pagination params
    QueryContext mockContext = getMockAllowContext(TEST_USER_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("count"))).thenReturn(null);
    Mockito.when(mockEnv.getArgument(Mockito.eq("start"))).thenReturn(null);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    resolver.get(mockEnv).get();

    // Verify service was called with defaults
    verify(mockService, times(1))
        .listConversations(any(OperationContext.class), eq(TEST_USER_URN), eq(20), eq(0));
  }

  private DataHubAiConversationInfo createMockConversationInfo() {
    DataHubAiConversationInfo info = new DataHubAiConversationInfo();
    info.setMessages(new DataHubAiConversationMessageArray());
    return info;
  }
}
