package com.linkedin.datahub.graphql.resolvers.actionworkflows;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.datahub.graphql.TestUtils.getMockDenyContext;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.linkedin.actionworkflow.ActionWorkflowInfo;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.*;
import com.linkedin.metadata.service.ActionWorkflowService;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class UpsertActionWorkflowResolverTest {

  private static final String TEST_USER_URN = "urn:li:corpuser:test";
  private static final String TEST_WORKFLOW_URN = "urn:li:actionRequestWorkflow:123";

  private ActionWorkflowService mockActionWorkflowService;
  private DataFetchingEnvironment mockDataFetchingEnvironment;
  private UpsertActionWorkflowResolver resolver;

  @BeforeMethod
  public void setup() {
    mockActionWorkflowService = Mockito.mock(ActionWorkflowService.class);
    mockDataFetchingEnvironment = Mockito.mock(DataFetchingEnvironment.class);
    resolver = new UpsertActionWorkflowResolver(mockActionWorkflowService);
  }

  @Test
  public void testUpsertWorkflowCreateSuccess() throws Exception {
    // GIVEN
    ActionWorkflowFieldInput fieldInput = new ActionWorkflowFieldInput();
    fieldInput.setId("reason");
    fieldInput.setName("Reason");
    fieldInput.setValueType(ActionWorkflowFieldValueType.STRING);
    fieldInput.setCardinality(PropertyCardinality.SINGLE);
    fieldInput.setRequired(true);

    UpsertActionWorkflowInput input = new UpsertActionWorkflowInput();
    input.setName("Test Workflow");
    input.setCategory(ActionWorkflowCategory.ACCESS);

    ActionWorkflowTriggerInput triggerInput = new ActionWorkflowTriggerInput();
    triggerInput.setType(ActionWorkflowTriggerType.FORM_SUBMITTED);

    ActionWorkflowFormInput inputForm = new ActionWorkflowFormInput();
    inputForm.setFields(Collections.singletonList(fieldInput));
    inputForm.setEntrypoints(Collections.emptyList());
    triggerInput.setForm(inputForm);
    input.setTrigger(triggerInput);

    input.setSteps(Collections.emptyList());

    ActionWorkflowInfo mockWorkflowInfo = new ActionWorkflowInfo();
    mockWorkflowInfo.setName("Test Workflow");
    mockWorkflowInfo.setCategory(com.linkedin.actionworkflow.ActionWorkflowCategory.ACCESS);
    mockWorkflowInfo.setTrigger(
        new com.linkedin.actionworkflow.ActionWorkflowTrigger()
            .setType(com.linkedin.actionworkflow.ActionWorkflowTriggerType.FORM_SUBMITTED)
            .setForm(
                new com.linkedin.actionworkflow.ActionWorkflowForm()
                    .setFields(new com.linkedin.actionworkflow.ActionWorkflowFieldArray())
                    .setEntrypoints(
                        new com.linkedin.actionworkflow.ActionWorkflowEntrypointArray())));
    mockWorkflowInfo.setSteps(new com.linkedin.actionworkflow.ActionWorkflowStepArray());

    // Add required audit stamps
    AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(UrnUtils.getUrn(TEST_USER_URN));
    auditStamp.setTime(System.currentTimeMillis());
    mockWorkflowInfo.setCreated(auditStamp);
    mockWorkflowInfo.setLastModified(auditStamp);

    // Mock environment and service behaviors
    QueryContext mockContext = getMockAllowContext(TEST_USER_URN);
    when(mockDataFetchingEnvironment.getArgument("input")).thenReturn(input);
    when(mockDataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(mockActionWorkflowService.upsertActionWorkflow(
            any(OperationContext.class), nullable(Urn.class), any(ActionWorkflowInfo.class)))
        .thenReturn(UrnUtils.getUrn(TEST_WORKFLOW_URN));
    when(mockActionWorkflowService.getActionWorkflow(any(OperationContext.class), any(Urn.class)))
        .thenReturn(mockWorkflowInfo);

    // WHEN
    CompletableFuture<ActionWorkflow> futureResult = resolver.get(mockDataFetchingEnvironment);
    ActionWorkflow result = futureResult.get();

    // THEN
    Assert.assertNotNull(result);
    Assert.assertEquals(result.getName(), "Test Workflow");
  }

  @Test(expectedExceptions = java.util.concurrent.ExecutionException.class)
  public void testUpsertWorkflowAuthorizationFailure() throws Exception {
    // GIVEN
    UpsertActionWorkflowInput input = new UpsertActionWorkflowInput();
    input.setName("Test Workflow");
    input.setCategory(ActionWorkflowCategory.ACCESS);

    ActionWorkflowTriggerInput triggerInput = new ActionWorkflowTriggerInput();
    triggerInput.setType(ActionWorkflowTriggerType.FORM_SUBMITTED);

    ActionWorkflowFormInput inputForm = new ActionWorkflowFormInput();
    inputForm.setFields(Collections.emptyList());
    inputForm.setEntrypoints(Collections.emptyList());
    triggerInput.setForm(inputForm);
    input.setTrigger(triggerInput);
    input.setSteps(Collections.emptyList());

    // Mock environment and service behaviors
    QueryContext mockContext = getMockDenyContext(TEST_USER_URN);
    when(mockDataFetchingEnvironment.getArgument("input")).thenReturn(input);
    when(mockDataFetchingEnvironment.getContext()).thenReturn(mockContext);

    // WHEN
    CompletableFuture<ActionWorkflow> futureResult = resolver.get(mockDataFetchingEnvironment);
    futureResult.get(); // This should throw ExecutionException wrapping AuthorizationException
  }
}
