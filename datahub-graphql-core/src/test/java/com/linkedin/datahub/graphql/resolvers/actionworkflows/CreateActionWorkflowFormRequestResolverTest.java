package com.linkedin.datahub.graphql.resolvers.actionworkflows;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.*;
import com.linkedin.metadata.service.ActionWorkflowService;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class CreateActionWorkflowFormRequestResolverTest {

  private static final String TEST_WORKFLOW_URN = "urn:li:actionRequestWorkflow:testWorkflow";
  private static final String TEST_ENTITY_URN =
      "urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)";
  private static final String TEST_ACTION_REQUEST_URN = "urn:li:actionRequest:test123";
  private static final String TEST_DESCRIPTION = "Test workflow request";
  private static final Long TEST_EXPIRES_AT = 1234567890L;

  @Mock private ActionWorkflowService mockWorkflowService;
  @Mock private DataFetchingEnvironment mockEnvironment;
  @Mock private QueryContext mockContext;
  @Mock private OperationContext mockOperationContext;

  private CreateActionWorkflowFormRequestResolver resolver;
  private AutoCloseable mocks;

  @BeforeMethod
  public void setup() {
    mocks = MockitoAnnotations.openMocks(this);
    resolver = new CreateActionWorkflowFormRequestResolver(mockWorkflowService);

    when(mockEnvironment.getContext()).thenReturn(mockContext);
    when(mockContext.getOperationContext()).thenReturn(mockOperationContext);
  }

  @AfterMethod
  public void teardown() throws Exception {
    mocks.close();
  }

  @Test
  public void testCreateActionRequestWorkflowRequestSuccess() throws Exception {
    // Arrange
    CreateActionWorkflowFormRequestInput input = createTestInput();
    when(mockEnvironment.getArgument("input")).thenReturn(input);

    Urn testActionRequestUrn = UrnUtils.getUrn(TEST_ACTION_REQUEST_URN);

    when(mockWorkflowService.createActionWorkflowFormRequest(
            any(Urn.class),
            any(Urn.class),
            anyString(),
            any(List.class),
            any(Long.class),
            any(OperationContext.class)))
        .thenReturn(testActionRequestUrn);

    // Act
    CompletableFuture<String> result = resolver.get(mockEnvironment);

    // Assert
    assertNotNull(result);
    String actionRequestUrn = result.get();
    assertEquals(actionRequestUrn, TEST_ACTION_REQUEST_URN);
  }

  @Test
  public void testCreateActionRequestWorkflowRequestWithoutEntityUrn() throws Exception {
    // Arrange
    CreateActionWorkflowFormRequestInput input = createTestInputWithoutEntity();
    when(mockEnvironment.getArgument("input")).thenReturn(input);

    Urn testActionRequestUrn = UrnUtils.getUrn(TEST_ACTION_REQUEST_URN);

    when(mockWorkflowService.createActionWorkflowFormRequest(
            any(Urn.class),
            isNull(),
            anyString(),
            any(List.class),
            isNull(),
            any(OperationContext.class)))
        .thenReturn(testActionRequestUrn);

    // Act
    CompletableFuture<String> result = resolver.get(mockEnvironment);

    // Assert
    assertNotNull(result);
    String actionRequestUrn = result.get();
    assertEquals(actionRequestUrn, TEST_ACTION_REQUEST_URN);
  }

  @Test
  public void testCreateActionRequestWorkflowRequestWithServiceException() throws Exception {
    // Arrange
    CreateActionWorkflowFormRequestInput input = createTestInput();
    when(mockEnvironment.getArgument("input")).thenReturn(input);

    when(mockWorkflowService.createActionWorkflowFormRequest(
            any(Urn.class),
            any(Urn.class),
            anyString(),
            any(List.class),
            any(Long.class),
            any(OperationContext.class)))
        .thenThrow(new RuntimeException("Service error"));

    // Act
    CompletableFuture<String> result = resolver.get(mockEnvironment);

    // Assert
    assertNotNull(result);
    try {
      result.get();
      fail("Expected exception but none was thrown");
    } catch (Exception e) {
      assertTrue(e.getCause() instanceof RuntimeException);
      assertEquals(e.getCause().getMessage(), "Failed to create action workflow request");
    }
  }

  private CreateActionWorkflowFormRequestInput createTestInput() {
    CreateActionWorkflowFormRequestInput input = new CreateActionWorkflowFormRequestInput();
    input.setWorkflowUrn(TEST_WORKFLOW_URN);
    input.setEntityUrn(TEST_ENTITY_URN);
    input.setDescription(TEST_DESCRIPTION);

    // Create fields
    ActionWorkflowFormRequestFieldInput field1 = new ActionWorkflowFormRequestFieldInput();
    field1.setId("field1");
    PropertyValueInput value1 = new PropertyValueInput();
    value1.setStringValue("test value");
    field1.setValues(ImmutableList.of(value1));

    ActionWorkflowFormRequestFieldInput field2 = new ActionWorkflowFormRequestFieldInput();
    field2.setId("field2");
    PropertyValueInput value2 = new PropertyValueInput();
    value2.setNumberValue(42.0f);
    field2.setValues(ImmutableList.of(value2));

    input.setFields(ImmutableList.of(field1, field2));

    // Create access
    CreateAccessWorkflowRequestInput access = new CreateAccessWorkflowRequestInput();
    access.setExpiresAt(TEST_EXPIRES_AT);
    input.setAccess(access);

    return input;
  }

  private CreateActionWorkflowFormRequestInput createTestInputWithoutEntity() {
    CreateActionWorkflowFormRequestInput input = new CreateActionWorkflowFormRequestInput();
    input.setWorkflowUrn(TEST_WORKFLOW_URN);
    input.setDescription(TEST_DESCRIPTION);

    // Create minimal fields
    ActionWorkflowFormRequestFieldInput field1 = new ActionWorkflowFormRequestFieldInput();
    field1.setId("field1");
    PropertyValueInput value1 = new PropertyValueInput();
    value1.setStringValue("test value");
    field1.setValues(ImmutableList.of(value1));

    input.setFields(ImmutableList.of(field1));

    return input;
  }

  private CreateActionWorkflowFormRequestInput createMinimalInput() {
    CreateActionWorkflowFormRequestInput input = new CreateActionWorkflowFormRequestInput();
    input.setWorkflowUrn(TEST_WORKFLOW_URN);

    // Create minimal fields
    ActionWorkflowFormRequestFieldInput field1 = new ActionWorkflowFormRequestFieldInput();
    field1.setId("field1");
    PropertyValueInput value1 = new PropertyValueInput();
    value1.setStringValue("test value");
    field1.setValues(ImmutableList.of(value1));

    input.setFields(ImmutableList.of(field1));

    return input;
  }
}
