package com.linkedin.datahub.graphql.resolvers.ai;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.metadata.service.DataHubAiConversationService;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class DeleteDataHubAiConversationResolverTest {

  private static final Urn TEST_USER_URN = UrnUtils.getUrn("urn:li:corpuser:testUser");
  private static final Urn TEST_CONVERSATION_URN =
      UrnUtils.getUrn("urn:li:dataHubAiConversation:12345");

  @Test
  public void testDeleteConversationSuccess() throws Exception {
    // Create mock service
    DataHubAiConversationService mockService = mock(DataHubAiConversationService.class);
    when(mockService.canAccessConversation(
            any(OperationContext.class), any(Urn.class), any(Urn.class)))
        .thenReturn(true);
    doNothing().when(mockService).deleteConversation(any(OperationContext.class), any(Urn.class));

    // Create resolver
    DeleteDataHubAiConversationResolver resolver =
        new DeleteDataHubAiConversationResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext(TEST_USER_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn")))
        .thenReturn(TEST_CONVERSATION_URN.toString());
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Boolean result = resolver.get(mockEnv).get();

    // Verify result
    assertTrue(result);

    // Verify service was called
    verify(mockService, times(1))
        .canAccessConversation(
            any(OperationContext.class), eq(TEST_CONVERSATION_URN), eq(TEST_USER_URN));
    verify(mockService, times(1))
        .deleteConversation(any(OperationContext.class), eq(TEST_CONVERSATION_URN));
  }

  @Test
  public void testDeleteConversationUnauthorized() throws Exception {
    // Create mock service
    DataHubAiConversationService mockService = mock(DataHubAiConversationService.class);
    when(mockService.canAccessConversation(
            any(OperationContext.class), any(Urn.class), any(Urn.class)))
        .thenReturn(false);

    // Create resolver
    DeleteDataHubAiConversationResolver resolver =
        new DeleteDataHubAiConversationResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext(TEST_USER_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn")))
        .thenReturn(TEST_CONVERSATION_URN.toString());
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    try {
      resolver.get(mockEnv).get();
      fail("Expected RuntimeException for unauthorized access");
    } catch (Exception e) {
      // Expected - any exception is fine as long as access was denied
      assertNotNull(e);
    }

    // Verify delete was not called
    verify(mockService, times(0)).deleteConversation(any(OperationContext.class), any(Urn.class));
  }

  @Test
  public void testDeleteConversationInvalidUrn() throws Exception {
    // Create mock service
    DataHubAiConversationService mockService = mock(DataHubAiConversationService.class);

    // Create resolver
    DeleteDataHubAiConversationResolver resolver =
        new DeleteDataHubAiConversationResolver(mockService);

    // Execute resolver with invalid URN
    QueryContext mockContext = getMockAllowContext(TEST_USER_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(null);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    try {
      resolver.get(mockEnv).get();
      fail("Expected IllegalArgumentException");
    } catch (Exception e) {
      // Expected - any exception is fine for invalid input
      assertNotNull(e);
    }
  }

  @Test
  public void testDeleteConversationWrongEntityType() throws Exception {
    // Create mock service
    DataHubAiConversationService mockService = mock(DataHubAiConversationService.class);

    // Create resolver
    DeleteDataHubAiConversationResolver resolver =
        new DeleteDataHubAiConversationResolver(mockService);

    // Execute resolver with wrong entity type
    QueryContext mockContext = getMockAllowContext(TEST_USER_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn("urn:li:dataset:test");
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    try {
      resolver.get(mockEnv).get();
      fail("Expected IllegalArgumentException");
    } catch (Exception e) {
      // Expected - any exception is fine for wrong entity type
      assertNotNull(e);
    }
  }
}
