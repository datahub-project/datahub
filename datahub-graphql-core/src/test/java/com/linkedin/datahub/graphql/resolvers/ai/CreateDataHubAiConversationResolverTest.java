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
import com.linkedin.conversation.DataHubAiConversationMessageArray;
import com.linkedin.conversation.DataHubAiConversationOriginType;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataHubAiConversation;
import com.linkedin.metadata.service.DataHubAiConversationService;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.HashMap;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class CreateDataHubAiConversationResolverTest {

  private static final Urn TEST_USER_URN = UrnUtils.getUrn("urn:li:corpuser:testUser");
  private static final Urn TEST_CONVERSATION_URN =
      UrnUtils.getUrn("urn:li:dataHubAiConversation:12345");

  @Test
  public void testCreateConversationSuccess() throws Exception {
    // Create mock service
    DataHubAiConversationService mockService = mock(DataHubAiConversationService.class);
    when(mockService.createConversation(
            any(OperationContext.class),
            any(),
            any(Urn.class),
            any(DataHubAiConversationOriginType.class)))
        .thenReturn(TEST_CONVERSATION_URN);
    when(mockService.getConversation(any(OperationContext.class), any(Urn.class)))
        .thenReturn(createMockConversationInfo());

    // Create resolver
    CreateDataHubAiConversationResolver resolver =
        new CreateDataHubAiConversationResolver(mockService);

    // Execute resolver with input object
    QueryContext mockContext = getMockAllowContext(TEST_USER_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Map<String, Object> input = new HashMap<>();
    input.put("title", "Test Conversation");
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    DataHubAiConversation result = resolver.get(mockEnv).get();

    // Verify result
    assertNotNull(result);
    assertEquals(result.getUrn(), TEST_CONVERSATION_URN.toString());

    // Verify service was called
    verify(mockService, times(1))
        .createConversation(
            any(OperationContext.class),
            eq("Test Conversation"),
            eq(TEST_USER_URN),
            any(DataHubAiConversationOriginType.class));
    verify(mockService, times(1))
        .getConversation(any(OperationContext.class), eq(TEST_CONVERSATION_URN));
  }

  @Test
  public void testCreateConversationWithNullTitle() throws Exception {
    // Create mock service
    DataHubAiConversationService mockService = mock(DataHubAiConversationService.class);
    when(mockService.createConversation(
            any(OperationContext.class),
            any(),
            any(Urn.class),
            any(DataHubAiConversationOriginType.class)))
        .thenReturn(TEST_CONVERSATION_URN);
    when(mockService.getConversation(any(OperationContext.class), any(Urn.class)))
        .thenReturn(createMockConversationInfo());

    // Create resolver
    CreateDataHubAiConversationResolver resolver =
        new CreateDataHubAiConversationResolver(mockService);

    // Execute resolver with input object containing null title
    QueryContext mockContext = getMockAllowContext(TEST_USER_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Map<String, Object> input = new HashMap<>();
    input.put("title", null);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    DataHubAiConversation result = resolver.get(mockEnv).get();

    // Verify result
    assertNotNull(result);

    // Verify service was called with null title
    verify(mockService, times(1))
        .createConversation(
            any(OperationContext.class),
            eq(null),
            eq(TEST_USER_URN),
            any(DataHubAiConversationOriginType.class));
  }

  @Test
  public void testCreateConversationNoUser() throws Exception {
    // Create mock service
    DataHubAiConversationService mockService = mock(DataHubAiConversationService.class);

    // Create resolver
    CreateDataHubAiConversationResolver resolver =
        new CreateDataHubAiConversationResolver(mockService);

    // Execute resolver with no user
    QueryContext mockContext = mock(QueryContext.class);
    when(mockContext.getActorUrn()).thenReturn(null);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Map<String, Object> input = new HashMap<>();
    input.put("title", "Test");
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    try {
      resolver.get(mockEnv).get();
      fail("Expected RuntimeException");
    } catch (Exception e) {
      // Expected - any exception is fine when no user is present
      assertNotNull(e);
    }

    // Verify service was not called
    verify(mockService, times(0))
        .createConversation(
            any(OperationContext.class),
            any(),
            any(Urn.class),
            any(DataHubAiConversationOriginType.class));
  }

  @Test
  public void testCreateConversationWithOriginType() throws Exception {
    // Create mock service
    DataHubAiConversationService mockService = mock(DataHubAiConversationService.class);
    when(mockService.createConversation(
            any(OperationContext.class),
            any(),
            any(Urn.class),
            any(DataHubAiConversationOriginType.class)))
        .thenReturn(TEST_CONVERSATION_URN);
    when(mockService.getConversation(any(OperationContext.class), any(Urn.class)))
        .thenReturn(createMockConversationInfo());

    // Create resolver
    CreateDataHubAiConversationResolver resolver =
        new CreateDataHubAiConversationResolver(mockService);

    // Execute resolver with originType in input object
    QueryContext mockContext = getMockAllowContext(TEST_USER_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Map<String, Object> input = new HashMap<>();
    input.put("title", "Test Conversation");
    input.put("originType", "DATAHUB_UI");
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    DataHubAiConversation result = resolver.get(mockEnv).get();

    // Verify result
    assertNotNull(result);
    assertEquals(result.getUrn(), TEST_CONVERSATION_URN.toString());

    // Verify service was called with correct originType
    verify(mockService, times(1))
        .createConversation(
            any(OperationContext.class),
            eq("Test Conversation"),
            eq(TEST_USER_URN),
            eq(DataHubAiConversationOriginType.DATAHUB_UI));
  }

  @Test
  public void testCreateConversationWithNullOriginType() throws Exception {
    // Create mock service
    DataHubAiConversationService mockService = mock(DataHubAiConversationService.class);
    when(mockService.createConversation(
            any(OperationContext.class),
            any(),
            any(Urn.class),
            any(DataHubAiConversationOriginType.class)))
        .thenReturn(TEST_CONVERSATION_URN);
    when(mockService.getConversation(any(OperationContext.class), any(Urn.class)))
        .thenReturn(createMockConversationInfo());

    // Create resolver
    CreateDataHubAiConversationResolver resolver =
        new CreateDataHubAiConversationResolver(mockService);

    // Execute resolver with null originType in input (should default to DATAHUB_UI)
    QueryContext mockContext = getMockAllowContext(TEST_USER_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Map<String, Object> input = new HashMap<>();
    input.put("title", "Test Conversation");
    input.put("originType", null);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    DataHubAiConversation result = resolver.get(mockEnv).get();

    // Verify result
    assertNotNull(result);
    assertEquals(result.getUrn(), TEST_CONVERSATION_URN.toString());

    // Verify service was called with default originType (DATAHUB_UI)
    verify(mockService, times(1))
        .createConversation(
            any(OperationContext.class),
            eq("Test Conversation"),
            eq(TEST_USER_URN),
            eq(DataHubAiConversationOriginType.DATAHUB_UI));
  }

  private DataHubAiConversationInfo createMockConversationInfo() {
    DataHubAiConversationInfo info = new DataHubAiConversationInfo();
    info.setMessages(new DataHubAiConversationMessageArray());
    return info;
  }
}
