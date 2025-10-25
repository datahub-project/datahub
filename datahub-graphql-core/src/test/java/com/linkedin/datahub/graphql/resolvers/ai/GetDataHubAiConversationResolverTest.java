package com.linkedin.datahub.graphql.resolvers.ai;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.conversation.DataHubAiConversationInfo;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataHubAiConversation;
import com.linkedin.metadata.service.DataHubAiConversationService;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class GetDataHubAiConversationResolverTest {

  private static final Urn TEST_USER_URN = UrnUtils.getUrn("urn:li:corpuser:testUser");
  private static final Urn TEST_CONVERSATION_URN =
      UrnUtils.getUrn("urn:li:dataHubAiConversation:12345");
  private static final String TEST_CONVERSATION_URN_STRING = TEST_CONVERSATION_URN.toString();

  @Test
  public void testGetConversationSuccess() throws Exception {
    // Create mock service
    DataHubAiConversationService mockService = mock(DataHubAiConversationService.class);
    DataHubAiConversationInfo mockConversationInfo = createMockConversationInfo();

    when(mockService.canAccessConversation(
            any(OperationContext.class), any(Urn.class), any(Urn.class)))
        .thenReturn(true);
    when(mockService.getConversation(any(OperationContext.class), any(Urn.class)))
        .thenReturn(mockConversationInfo);

    // Create resolver
    GetDataHubAiConversationResolver resolver = new GetDataHubAiConversationResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext(TEST_USER_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_CONVERSATION_URN_STRING);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    DataHubAiConversation result = resolver.get(mockEnv).get();

    // Verify result is not null
    assertNotNull(result);

    // Verify service methods were called
    verify(mockService, times(1))
        .canAccessConversation(
            any(OperationContext.class), eq(TEST_CONVERSATION_URN), eq(TEST_USER_URN));
    verify(mockService, times(1))
        .getConversation(any(OperationContext.class), eq(TEST_CONVERSATION_URN));
  }

  private DataHubAiConversationInfo createMockConversationInfo() {
    DataHubAiConversationInfo mockInfo = mock(DataHubAiConversationInfo.class);

    // Mock origin type
    com.linkedin.conversation.DataHubAiConversationOriginType mockOriginType =
        mock(com.linkedin.conversation.DataHubAiConversationOriginType.class);
    when(mockOriginType.toString()).thenReturn("DATAHUB_UI");
    when(mockInfo.getOriginType()).thenReturn(mockOriginType);

    // Mock title
    when(mockInfo.hasTitle()).thenReturn(true);
    when(mockInfo.getTitle()).thenReturn("Test Conversation");

    // Mock created audit stamp
    com.linkedin.common.AuditStamp mockAuditStamp = mock(com.linkedin.common.AuditStamp.class);
    when(mockAuditStamp.getTime()).thenReturn(System.currentTimeMillis());
    when(mockAuditStamp.getActor()).thenReturn(TEST_USER_URN);
    when(mockInfo.hasCreated()).thenReturn(true);
    when(mockInfo.getCreated()).thenReturn(mockAuditStamp);

    // Mock messages
    when(mockInfo.hasMessages()).thenReturn(false);

    return mockInfo;
  }

  @Test
  public void testGetConversationUnauthorized() throws Exception {
    // Create mock service
    DataHubAiConversationService mockService = mock(DataHubAiConversationService.class);
    when(mockService.canAccessConversation(
            any(OperationContext.class), any(Urn.class), any(Urn.class)))
        .thenReturn(false);

    // Create resolver
    GetDataHubAiConversationResolver resolver = new GetDataHubAiConversationResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext(TEST_USER_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_CONVERSATION_URN_STRING);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    DataHubAiConversation result = resolver.get(mockEnv).get();

    // Verify result is null for unauthorized access
    assertNull(result);

    // Verify canAccessConversation was called but getConversation was not
    verify(mockService, times(1))
        .canAccessConversation(
            any(OperationContext.class), eq(TEST_CONVERSATION_URN), eq(TEST_USER_URN));
    verify(mockService, times(0)).getConversation(any(OperationContext.class), any(Urn.class));
  }

  @Test
  public void testGetConversationInvalidUrn() throws Exception {
    // Create mock service
    DataHubAiConversationService mockService = mock(DataHubAiConversationService.class);

    // Create resolver
    GetDataHubAiConversationResolver resolver = new GetDataHubAiConversationResolver(mockService);

    // Execute resolver with invalid URN
    QueryContext mockContext = getMockAllowContext(TEST_USER_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn("invalid-urn-format");
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    DataHubAiConversation result = resolver.get(mockEnv).get();

    // Verify result is null for invalid URN
    assertNull(result);

    // Verify service methods were not called
    verify(mockService, times(0))
        .canAccessConversation(any(OperationContext.class), any(Urn.class), any(Urn.class));
    verify(mockService, times(0)).getConversation(any(OperationContext.class), any(Urn.class));
  }

  @Test
  public void testGetConversationWrongEntityType() throws Exception {
    // Create mock service
    DataHubAiConversationService mockService = mock(DataHubAiConversationService.class);

    // Create resolver
    GetDataHubAiConversationResolver resolver = new GetDataHubAiConversationResolver(mockService);

    // Execute resolver with wrong entity type (dataset instead of dataHubAiConversation)
    QueryContext mockContext = getMockAllowContext(TEST_USER_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn("urn:li:dataset:test");
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    DataHubAiConversation result = resolver.get(mockEnv).get();

    // Verify result is null for wrong entity type
    assertNull(result);

    // Verify service methods were not called
    verify(mockService, times(0))
        .canAccessConversation(any(OperationContext.class), any(Urn.class), any(Urn.class));
    verify(mockService, times(0)).getConversation(any(OperationContext.class), any(Urn.class));
  }

  @Test
  public void testGetConversationNotFound() throws Exception {
    // Create mock service
    DataHubAiConversationService mockService = mock(DataHubAiConversationService.class);

    when(mockService.canAccessConversation(
            any(OperationContext.class), any(Urn.class), any(Urn.class)))
        .thenReturn(true);
    when(mockService.getConversation(any(OperationContext.class), any(Urn.class))).thenReturn(null);

    // Create resolver
    GetDataHubAiConversationResolver resolver = new GetDataHubAiConversationResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext(TEST_USER_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_CONVERSATION_URN_STRING);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    DataHubAiConversation result = resolver.get(mockEnv).get();

    // Verify result is null when conversation is not found
    assertNull(result);

    // Verify both service methods were called
    verify(mockService, times(1))
        .canAccessConversation(
            any(OperationContext.class), eq(TEST_CONVERSATION_URN), eq(TEST_USER_URN));
    verify(mockService, times(1))
        .getConversation(any(OperationContext.class), eq(TEST_CONVERSATION_URN));
  }

  @Test
  public void testGetConversationServiceException() throws Exception {
    // Create mock service
    DataHubAiConversationService mockService = mock(DataHubAiConversationService.class);

    when(mockService.canAccessConversation(
            any(OperationContext.class), any(Urn.class), any(Urn.class)))
        .thenReturn(true);
    when(mockService.getConversation(any(OperationContext.class), any(Urn.class)))
        .thenThrow(new RuntimeException("Service error"));

    // Create resolver
    GetDataHubAiConversationResolver resolver = new GetDataHubAiConversationResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext(TEST_USER_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_CONVERSATION_URN_STRING);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    try {
      resolver.get(mockEnv).get();
      fail("Expected RuntimeException to be thrown");
    } catch (Exception e) {
      // Expected - verify the exception was thrown
      assertNotNull(e);
      assertTrue(
          e.getMessage().contains("Failed to get agent conversation")
              || e.getCause().getMessage().contains("Service error"));
    }

    // Verify service methods were called
    verify(mockService, times(1))
        .canAccessConversation(
            any(OperationContext.class), eq(TEST_CONVERSATION_URN), eq(TEST_USER_URN));
    verify(mockService, times(1))
        .getConversation(any(OperationContext.class), eq(TEST_CONVERSATION_URN));
  }

  @Test
  public void testGetConversationNullUrnArgument() throws Exception {
    // Create mock service
    DataHubAiConversationService mockService = mock(DataHubAiConversationService.class);

    // Create resolver
    GetDataHubAiConversationResolver resolver = new GetDataHubAiConversationResolver(mockService);

    // Execute resolver with null URN
    QueryContext mockContext = getMockAllowContext(TEST_USER_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(null);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    try {
      resolver.get(mockEnv).get();
      // If it doesn't throw, result should be null
    } catch (Exception e) {
      // Expected - null URN should cause an error
      assertNotNull(e);
    }

    // Verify service methods were not called
    verify(mockService, times(0))
        .canAccessConversation(any(OperationContext.class), any(Urn.class), any(Urn.class));
    verify(mockService, times(0)).getConversation(any(OperationContext.class), any(Urn.class));
  }
}
