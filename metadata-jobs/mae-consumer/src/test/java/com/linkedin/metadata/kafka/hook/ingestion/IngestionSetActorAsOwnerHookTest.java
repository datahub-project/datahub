package com.linkedin.metadata.kafka.hook.ingestion;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.linkedin.common.*;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.execution.ExecutionRequestInput;
import com.linkedin.execution.ExecutionRequestSource;
import com.linkedin.metadata.service.OwnerService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class IngestionSetActorAsOwnerHookTest {
  private static final Urn TEST_EXECUTION_RUN_URN =
      UrnUtils.getUrn("urn:li:dataHubExecutionRequest:test");

  private static final Urn TEST_INGESTION_SOURCE_URN =
      UrnUtils.getUrn("urn:li:dataHubIngestionSource:test");

  private static final Urn TEST_USER_URN = UrnUtils.getUrn("urn:li:corpuser:test");

  @Test
  public void testGetSuccess() throws Exception {
    OwnerService ownerService = getOwnerService(TEST_USER_URN, null, TEST_INGESTION_SOURCE_URN);

    IngestionSetActorAsOwnerHook hook = new IngestionSetActorAsOwnerHook(true, ownerService);
    hook.init(TestOperationContexts.systemContextNoSearchAuthorization());
    hook.invoke(
        buildMetadataChangeLog(
            TEST_EXECUTION_RUN_URN,
            EXECUTION_REQUEST_INPUT_ASPECT_NAME,
            ChangeType.UPSERT,
            getAuditStamp(TEST_USER_URN),
            getExecutionRequestInput(
                TEST_INGESTION_SOURCE_URN, EXECUTION_REQUEST_SOURCE_CLI_INGESTION_SOURCE)));

    ensureActorWasIngestedAsOwner(ownerService, TEST_USER_URN, TEST_INGESTION_SOURCE_URN);
  }

  @Test
  public void testGetSuccessWhenActorIsAlreadyOwner() throws Exception {
    OwnerService ownerService =
        getOwnerService(TEST_USER_URN, TEST_USER_URN, TEST_INGESTION_SOURCE_URN);

    IngestionSetActorAsOwnerHook hook = new IngestionSetActorAsOwnerHook(true, ownerService);
    hook.init(TestOperationContexts.systemContextNoSearchAuthorization());
    hook.invoke(
        buildMetadataChangeLog(
            TEST_EXECUTION_RUN_URN,
            EXECUTION_REQUEST_INPUT_ASPECT_NAME,
            ChangeType.UPSERT,
            getAuditStamp(TEST_USER_URN),
            getExecutionRequestInput(
                TEST_INGESTION_SOURCE_URN, EXECUTION_REQUEST_SOURCE_CLI_INGESTION_SOURCE)));

    verify(ownerService, never())
        .addOwners(any(OperationContext.class), eq(TEST_INGESTION_SOURCE_URN), any());
  }

  @Test
  public void testErrorWhenNoActorInEvent() throws Exception {
    OwnerService ownerService = getOwnerService(TEST_USER_URN, null, TEST_INGESTION_SOURCE_URN);

    IngestionSetActorAsOwnerHook hook = new IngestionSetActorAsOwnerHook(true, ownerService);
    hook.init(TestOperationContexts.systemContextNoSearchAuthorization());

    Assert.assertThrows(
        () -> {
          hook.invoke(
              buildMetadataChangeLog(
                  TEST_EXECUTION_RUN_URN,
                  EXECUTION_REQUEST_INPUT_ASPECT_NAME,
                  ChangeType.UPSERT,
                  null,
                  getExecutionRequestInput(
                      TEST_INGESTION_SOURCE_URN, EXECUTION_REQUEST_SOURCE_CLI_INGESTION_SOURCE)));
        });
  }

  @Test
  public void testErrorWhenNoSourceInEvent() throws Exception {
    OwnerService ownerService = getOwnerService(TEST_USER_URN, null, TEST_INGESTION_SOURCE_URN);

    IngestionSetActorAsOwnerHook hook = new IngestionSetActorAsOwnerHook(true, ownerService);
    hook.init(TestOperationContexts.systemContextNoSearchAuthorization());

    Assert.assertThrows(
        () -> {
          hook.invoke(
              buildMetadataChangeLog(
                  TEST_EXECUTION_RUN_URN,
                  EXECUTION_REQUEST_INPUT_ASPECT_NAME,
                  ChangeType.UPSERT,
                  getAuditStamp(TEST_USER_URN),
                  getExecutionRequestInput(null, EXECUTION_REQUEST_SOURCE_CLI_INGESTION_SOURCE)));
        });
  }

  @Test
  public void testErrorWhenNoAspectInEvent() throws Exception {
    OwnerService ownerService = getOwnerService(TEST_USER_URN, null, TEST_INGESTION_SOURCE_URN);

    IngestionSetActorAsOwnerHook hook = new IngestionSetActorAsOwnerHook(true, ownerService);
    hook.init(TestOperationContexts.systemContextNoSearchAuthorization());

    Assert.assertThrows(
        () -> {
          hook.invoke(
              buildMetadataChangeLog(
                  TEST_EXECUTION_RUN_URN,
                  EXECUTION_REQUEST_INPUT_ASPECT_NAME,
                  ChangeType.UPSERT,
                  getAuditStamp(TEST_USER_URN),
                  null));
        });
  }

  @Test
  public void testInvokeNotEnabled() throws Exception {
    OwnerService ownerService = getOwnerService(TEST_USER_URN, null, TEST_INGESTION_SOURCE_URN);

    IngestionSetActorAsOwnerHook hook = new IngestionSetActorAsOwnerHook(false, ownerService);
    hook.invoke(
        buildMetadataChangeLog(
            TEST_EXECUTION_RUN_URN,
            EXECUTION_REQUEST_INPUT_ASPECT_NAME,
            ChangeType.UPSERT,
            getAuditStamp(TEST_USER_URN),
            getExecutionRequestInput(
                TEST_INGESTION_SOURCE_URN, EXECUTION_REQUEST_SOURCE_CLI_INGESTION_SOURCE)));

    Mockito.verifyNoInteractions(ownerService);
  }

  @Test
  public void testIgnoreManualIngestionSourceType() throws Exception {
    OwnerService ownerService = getOwnerService(TEST_USER_URN, null, TEST_INGESTION_SOURCE_URN);

    IngestionSetActorAsOwnerHook hook = new IngestionSetActorAsOwnerHook(true, ownerService);
    hook.invoke(
        buildMetadataChangeLog(
            TEST_EXECUTION_RUN_URN,
            EXECUTION_REQUEST_INPUT_ASPECT_NAME,
            ChangeType.UPSERT,
            getAuditStamp(TEST_USER_URN),
            getExecutionRequestInput(
                TEST_INGESTION_SOURCE_URN, EXECUTION_REQUEST_SOURCE_MANUAL_INGESTION_SOURCE)));

    Mockito.verifyNoInteractions(ownerService);
  }

  private MetadataChangeLog buildMetadataChangeLog(
      Urn urn,
      String aspectName,
      ChangeType changeType,
      AuditStamp created,
      RecordTemplate aspect) {
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityUrn(urn);
    event.setEntityType(EXECUTION_REQUEST_ENTITY_NAME);
    event.setAspectName(aspectName);
    event.setChangeType(changeType);
    if (aspect != null) {
      event.setAspect(GenericRecordUtils.serializeAspect(aspect));
    }
    if (created != null) {
      event.setCreated(created);
    }
    return event;
  }

  private AuditStamp getAuditStamp(Urn actorUrn) {
    AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(actorUrn);
    return auditStamp;
  }

  private ExecutionRequestInput getExecutionRequestInput(
      @Nullable Urn ingestionSourceUrn, @Nullable String ingestionSourceType) {
    ExecutionRequestInput executionRequestInput = new ExecutionRequestInput();
    ExecutionRequestSource executionRequestSource = new ExecutionRequestSource();
    if (ingestionSourceUrn != null) {
      executionRequestSource.setIngestionSource(ingestionSourceUrn);
    }
    if (ingestionSourceType != null) {
      executionRequestSource.setType(ingestionSourceType);
    }
    executionRequestInput.setSource(executionRequestSource);

    return executionRequestInput;
  }

  private void ensureActorWasIngestedAsOwner(
      OwnerService ownerService, Urn actorUrn, Urn ingestionSourceUrn) throws Exception {
    ArgumentCaptor<List<Owner>> argument = ArgumentCaptor.forClass((Class) List.class);
    Mockito.verify(ownerService, times(1))
        .addOwners(any(OperationContext.class), eq(ingestionSourceUrn), argument.capture());

    Urn owner = argument.getValue().get(0).getOwner();

    // ensure that the correct owner was added
    Assert.assertEquals(owner, actorUrn);
  }

  private OwnerService getOwnerService(
      Urn actorUrn, @Nullable Urn existingOwner, Urn ingestionSourceUrn) throws Exception {
    OwnerService ownerService = Mockito.mock(OwnerService.class);

    List<Owner> owners =
        existingOwner != null ? List.of(getOwner(existingOwner)) : Collections.emptyList();

    when(ownerService.getEntityOwners(any(OperationContext.class), eq(ingestionSourceUrn)))
        .thenReturn(owners);

    doNothing()
        .when(ownerService)
        .addOwners(
            any(OperationContext.class), eq(ingestionSourceUrn), eq(List.of(getOwner(actorUrn))));

    return ownerService;
  }

  private Owner getOwner(Urn urn) {
    return new Owner()
        .setType(com.linkedin.common.OwnershipType.TECHNICAL_OWNER)
        .setSource(new OwnershipSource().setType(OwnershipSourceType.OTHER))
        .setOwner(urn);
  }
}
