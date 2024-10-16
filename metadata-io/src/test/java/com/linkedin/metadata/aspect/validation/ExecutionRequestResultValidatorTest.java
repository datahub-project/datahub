package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_ENTITY_NAME;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_RESULT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_STATUS_ABORTED;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_STATUS_CANCELLED;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_STATUS_DUPLICATE;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_STATUS_FAILURE;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_STATUS_RUNNING;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_STATUS_SUCCESS;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_STATUS_TIMEOUT;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.execution.ExecutionRequestResult;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.utils.AuditStampUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.testng.annotations.Test;

public class ExecutionRequestResultValidatorTest {
  private static final OperationContext TEST_CONTEXT =
      TestOperationContexts.systemContextNoSearchAuthorization();
  private static final AspectPluginConfig TEST_PLUGIN_CONFIG =
      AspectPluginConfig.builder()
          .className(ExecutionRequestResultValidator.class.getName())
          .enabled(true)
          .supportedOperations(List.of("UPSERT"))
          .supportedEntityAspectNames(
              List.of(
                  AspectPluginConfig.EntityAspectName.builder()
                      .entityName(EXECUTION_REQUEST_ENTITY_NAME)
                      .aspectName(EXECUTION_REQUEST_RESULT_ASPECT_NAME)
                      .build()))
          .build();
  private static final Urn TEST_URN = UrnUtils.getUrn("urn:li:dataHubExecutionRequest:xyz");

  @Test
  public void testAllowed() {
    ExecutionRequestResultValidator test = new ExecutionRequestResultValidator();
    test.setConfig(TEST_PLUGIN_CONFIG);

    Set<String> allowedUpdateStates =
        Set.of(
            EXECUTION_REQUEST_STATUS_RUNNING,
            EXECUTION_REQUEST_STATUS_FAILURE,
            EXECUTION_REQUEST_STATUS_TIMEOUT);
    Set<String> destinationStates = new HashSet<>(allowedUpdateStates);
    destinationStates.addAll(
        Set.of(
            EXECUTION_REQUEST_STATUS_ABORTED,
            EXECUTION_REQUEST_STATUS_CANCELLED,
            EXECUTION_REQUEST_STATUS_SUCCESS,
            EXECUTION_REQUEST_STATUS_DUPLICATE));

    List<ChangeMCP> testItems =
        new ArrayList<>(
            // Tests with previous state
            allowedUpdateStates.stream()
                .flatMap(
                    prevState ->
                        destinationStates.stream()
                            .map(
                                destState -> {
                                  SystemAspect prevData = mock(SystemAspect.class);
                                  when(prevData.getRecordTemplate())
                                      .thenReturn(
                                          new ExecutionRequestResult().setStatus(prevState));
                                  return ChangeItemImpl.builder()
                                      .changeType(ChangeType.UPSERT)
                                      .urn(TEST_URN)
                                      .aspectName(EXECUTION_REQUEST_RESULT_ASPECT_NAME)
                                      .recordTemplate(
                                          new ExecutionRequestResult().setStatus(destState))
                                      .previousSystemAspect(prevData)
                                      .auditStamp(AuditStampUtils.createDefaultAuditStamp())
                                      .build(TEST_CONTEXT.getAspectRetriever());
                                }))
                .toList());
    // Tests with no previous
    testItems.addAll(
        destinationStates.stream()
            .map(
                destState ->
                    ChangeItemImpl.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(TEST_URN)
                        .aspectName(EXECUTION_REQUEST_RESULT_ASPECT_NAME)
                        .recordTemplate(new ExecutionRequestResult().setStatus(destState))
                        .auditStamp(AuditStampUtils.createDefaultAuditStamp())
                        .build(TEST_CONTEXT.getAspectRetriever()))
            .toList());

    List<AspectValidationException> result =
        test.validatePreCommitAspects(testItems, mock(RetrieverContext.class)).toList();

    assertTrue(result.isEmpty(), "Did not expect any validation errors.");
  }

  @Test
  public void testDenied() {
    ExecutionRequestResultValidator test = new ExecutionRequestResultValidator();
    test.setConfig(TEST_PLUGIN_CONFIG);

    Set<String> deniedUpdateStates =
        Set.of(
            EXECUTION_REQUEST_STATUS_ABORTED,
            EXECUTION_REQUEST_STATUS_CANCELLED,
            EXECUTION_REQUEST_STATUS_SUCCESS,
            EXECUTION_REQUEST_STATUS_DUPLICATE);
    Set<String> destinationStates = new HashSet<>(deniedUpdateStates);
    destinationStates.addAll(
        Set.of(
            EXECUTION_REQUEST_STATUS_RUNNING,
            EXECUTION_REQUEST_STATUS_FAILURE,
            EXECUTION_REQUEST_STATUS_TIMEOUT));

    List<ChangeMCP> testItems =
        new ArrayList<>(
            // Tests with previous state
            deniedUpdateStates.stream()
                .flatMap(
                    prevState ->
                        destinationStates.stream()
                            .map(
                                destState -> {
                                  SystemAspect prevData = mock(SystemAspect.class);
                                  when(prevData.getRecordTemplate())
                                      .thenReturn(
                                          new ExecutionRequestResult().setStatus(prevState));
                                  return ChangeItemImpl.builder()
                                      .changeType(ChangeType.UPSERT)
                                      .urn(TEST_URN)
                                      .aspectName(EXECUTION_REQUEST_RESULT_ASPECT_NAME)
                                      .recordTemplate(
                                          new ExecutionRequestResult().setStatus(destState))
                                      .previousSystemAspect(prevData)
                                      .auditStamp(AuditStampUtils.createDefaultAuditStamp())
                                      .build(TEST_CONTEXT.getAspectRetriever());
                                }))
                .toList());

    List<AspectValidationException> result =
        test.validatePreCommitAspects(testItems, mock(RetrieverContext.class)).toList();

    assertEquals(
        result.size(),
        deniedUpdateStates.size() * destinationStates.size(),
        "Expected ALL items to be denied.");
  }
}
