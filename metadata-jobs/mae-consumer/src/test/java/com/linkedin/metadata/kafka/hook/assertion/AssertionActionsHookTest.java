package com.linkedin.metadata.kafka.hook.assertion;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.kafka.hook.EntityRegistryTestUtil.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.assertion.AssertionAction;
import com.linkedin.assertion.AssertionActionArray;
import com.linkedin.assertion.AssertionActionType;
import com.linkedin.assertion.AssertionActions;
import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionResult;
import com.linkedin.assertion.AssertionResultType;
import com.linkedin.assertion.AssertionRunEvent;
import com.linkedin.assertion.AssertionRunStatus;
import com.linkedin.assertion.AssertionSource;
import com.linkedin.assertion.AssertionSourceType;
import com.linkedin.assertion.AssertionStdOperator;
import com.linkedin.assertion.AssertionType;
import com.linkedin.assertion.DatasetAssertionInfo;
import com.linkedin.assertion.DatasetAssertionScope;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.FabricType;
import com.linkedin.common.Status;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetFieldUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.incident.IncidentInfo;
import com.linkedin.incident.IncidentSource;
import com.linkedin.incident.IncidentSourceType;
import com.linkedin.incident.IncidentState;
import com.linkedin.incident.IncidentStatus;
import com.linkedin.incident.IncidentType;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.service.util.AssertionUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.openapi.client.OpenApiClient;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class AssertionActionsHookTest {
  private static final Urn TEST_ASSERTION_URN = UrnUtils.getUrn("urn:li:assertion:test");
  private static final Urn TEST_INCIDENT_URN = UrnUtils.getUrn("urn:li:incident:test-1");
  private static final Urn TEST_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,name,PROD)");
  private static final ObjectMapper objectMapper = new ObjectMapper();

  private static OperationContext mockOperationContext() {
    return TestOperationContexts.userContextNoSearchAuthorization(ENTITY_REGISTRY);
  }

  @Test
  public void testInvokeNotEnabled() throws Exception {
    SystemEntityClient entityClient = mock(SystemEntityClient.class);

    final AssertionActionsHook hook =
        new AssertionActionsHook(
            entityClient,
            Mockito.mock(GraphClient.class),
            false,
            Mockito.mock(OpenApiClient.class),
            objectMapper);

    final MetadataChangeLog event =
        buildMetadataChangeLog(
            TEST_ASSERTION_URN,
            ASSERTION_RUN_EVENT_ASPECT_NAME,
            ChangeType.UPSERT,
            buildAssertionRunEvent(
                TEST_ASSERTION_URN, AssertionRunStatus.COMPLETE, AssertionResultType.SUCCESS));
    hook.invoke(event);

    Mockito.verify(entityClient, Mockito.times(0))
        .getV2(
            any(OperationContext.class),
            Mockito.anyString(),
            Mockito.any(Urn.class),
            Mockito.anySet());
  }

  @Test
  public void testInvokeNotEligibleChange() throws Exception {
    SystemEntityClient entityClient = mock(SystemEntityClient.class);

    final AssertionActionsHook hook =
        new AssertionActionsHook(
                entityClient,
                Mockito.mock(GraphClient.class),
                true,
                Mockito.mock(OpenApiClient.class),
                objectMapper)
            .init(mockOperationContext());

    // Case 1: Incorrect aspect --- Assertion Info
    MetadataChangeLog event =
        buildMetadataChangeLog(
            TEST_ASSERTION_URN, ASSERTION_INFO_ASPECT_NAME, ChangeType.UPSERT, new AssertionInfo());
    hook.invoke(event);
    Mockito.verify(entityClient, Mockito.times(0))
        .search(any(), anyString(), anyString(), any(), any(), anyInt(), anyInt());
    Mockito.verify(entityClient, Mockito.times(0))
        .getV2(
            any(OperationContext.class),
            Mockito.anyString(),
            Mockito.any(Urn.class),
            Mockito.anySet());

    // Case 2: Run Event But Delete
    event =
        buildMetadataChangeLog(
            TEST_ASSERTION_URN,
            ASSERTION_RUN_EVENT_ASPECT_NAME,
            ChangeType.DELETE,
            buildAssertionRunEvent(
                TEST_ASSERTION_URN, AssertionRunStatus.COMPLETE, AssertionResultType.SUCCESS));
    hook.invoke(event);
    Mockito.verify(entityClient, Mockito.times(0))
        .search(any(), anyString(), anyString(), any(), any(), anyInt(), anyInt());
    Mockito.verify(entityClient, Mockito.times(0))
        .getV2(
            any(OperationContext.class),
            Mockito.anyString(),
            Mockito.any(Urn.class),
            Mockito.anySet());

    // Case 3: Status aspect but for the wrong entity type
    event =
        buildMetadataChangeLog(
            TEST_DATASET_URN, STATUS_ASPECT_NAME, ChangeType.UPSERT, new Status().setRemoved(true));
    hook.invoke(event);
    Mockito.verify(entityClient, Mockito.times(0))
        .search(any(), anyString(), anyString(), any(), any(), anyInt(), anyInt());
    Mockito.verify(entityClient, Mockito.times(0))
        .getV2(
            any(OperationContext.class),
            Mockito.anyString(),
            Mockito.any(Urn.class),
            Mockito.anySet());

    // Case 4: Run event but not complete
    event =
        buildMetadataChangeLog(
            TEST_ASSERTION_URN,
            ASSERTION_RUN_EVENT_ASPECT_NAME,
            ChangeType.UPSERT,
            buildAssertionRunEvent(
                TEST_ASSERTION_URN, AssertionRunStatus.$UNKNOWN, AssertionResultType.SUCCESS));
    hook.invoke(event);
    Mockito.verify(entityClient, Mockito.times(0))
        .search(any(), anyString(), anyString(), any(), any(), anyInt(), anyInt());
    Mockito.verify(entityClient, Mockito.times(0))
        .getV2(
            any(OperationContext.class),
            Mockito.anyString(),
            Mockito.any(Urn.class),
            Mockito.anySet());
  }

  @Test
  public void testInvokeAssertionRunEventSuccessNoActions() throws Exception {
    SystemEntityClient entityClient =
        mockSystemEntityClient(
            null,
            null,
            TEST_ASSERTION_URN,
            new AssertionInfo()
                .setType(AssertionType.DATASET)
                .setDatasetAssertion(
                    new DatasetAssertionInfo()
                        .setDataset(TEST_DATASET_URN)
                        .setScope(DatasetAssertionScope.DATASET_COLUMN)),
            new AssertionActions()
                .setOnFailure(new AssertionActionArray())
                .setOnSuccess(new AssertionActionArray()));

    final AssertionActionsHook hook =
        new AssertionActionsHook(
                entityClient,
                Mockito.mock(GraphClient.class),
                true,
                Mockito.mock(OpenApiClient.class),
                objectMapper)
            .init(mockOperationContext());

    MetadataChangeLog event =
        buildMetadataChangeLog(
            TEST_ASSERTION_URN,
            ASSERTION_RUN_EVENT_ASPECT_NAME,
            ChangeType.UPSERT,
            buildAssertionRunEvent(
                TEST_ASSERTION_URN, AssertionRunStatus.COMPLETE, AssertionResultType.SUCCESS));

    hook.invoke(event);

    // Ensure that we looked up the assertion actions correctly.
    Mockito.verify(entityClient, Mockito.times(2))
        .getV2(
            any(OperationContext.class),
            Mockito.eq(ASSERTION_ENTITY_NAME),
            Mockito.eq(TEST_ASSERTION_URN),
            Mockito.eq(
                ImmutableSet.of(
                    ASSERTION_INFO_ASPECT_NAME,
                    ASSERTION_ACTIONS_ASPECT_NAME,
                    DATA_PLATFORM_INSTANCE_ASPECT_NAME,
                    GLOBAL_TAGS_ASPECT_NAME)));

    // Ensure that we did not apply any actions or look up anything for incidents.
    Mockito.verify(entityClient, Mockito.times(0))
        .search(
            Mockito.any(),
            Mockito.eq(INCIDENT_ENTITY_NAME),
            Mockito.eq("*"),
            Mockito.any(Filter.class),
            Mockito.any(List.class),
            Mockito.anyInt(),
            Mockito.anyInt());

    // No ingestion to perform
    Mockito.verify(entityClient, Mockito.times(0))
        .ingestProposal(
            any(OperationContext.class),
            Mockito.any(MetadataChangeProposal.class),
            Mockito.anyBoolean());
  }

  @Test
  public void testInvokeAssertionRunEventSuccessActionsNoIncident() throws Exception {

    SystemEntityClient entityClient =
        mockSystemEntityClient(
            null,
            null,
            TEST_ASSERTION_URN,
            new AssertionInfo()
                .setType(AssertionType.DATASET)
                .setDatasetAssertion(
                    new DatasetAssertionInfo()
                        .setDataset(TEST_DATASET_URN)
                        .setScope(DatasetAssertionScope.DATASET_COLUMN)),
            new AssertionActions()
                .setOnFailure(new AssertionActionArray())
                .setOnSuccess(
                    new AssertionActionArray(
                        ImmutableList.of(
                            new AssertionAction().setType(AssertionActionType.RESOLVE_INCIDENT)))));

    final AssertionActionsHook hook =
        new AssertionActionsHook(
                entityClient,
                Mockito.mock(GraphClient.class),
                true,
                Mockito.mock(OpenApiClient.class),
                objectMapper)
            .init(mockOperationContext());

    MetadataChangeLog event =
        buildMetadataChangeLog(
            TEST_ASSERTION_URN,
            ASSERTION_RUN_EVENT_ASPECT_NAME,
            ChangeType.UPSERT,
            buildAssertionRunEvent(
                TEST_ASSERTION_URN, AssertionRunStatus.COMPLETE, AssertionResultType.SUCCESS));

    hook.invoke(event);

    // Ensure that we looked up the assertion actions correctly.
    Mockito.verify(entityClient, Mockito.times(2))
        .getV2(
            any(OperationContext.class),
            Mockito.eq(ASSERTION_ENTITY_NAME),
            Mockito.eq(TEST_ASSERTION_URN),
            Mockito.eq(
                ImmutableSet.of(
                    ASSERTION_INFO_ASPECT_NAME,
                    ASSERTION_ACTIONS_ASPECT_NAME,
                    DATA_PLATFORM_INSTANCE_ASPECT_NAME,
                    GLOBAL_TAGS_ASPECT_NAME)));

    // Ensure that we searched for the active incidents associated with the assertion..
    Mockito.verify(entityClient, Mockito.times(1))
        .search(
            Mockito.any(),
            Mockito.eq(INCIDENT_ENTITY_NAME),
            Mockito.eq("*"),
            Mockito.any(Filter.class),
            Mockito.eq(null),
            Mockito.anyInt(),
            Mockito.anyInt());

    // Verify that nothing was ingested in this case
    Mockito.verify(entityClient, Mockito.times(0))
        .ingestProposal(
            any(OperationContext.class),
            Mockito.any(MetadataChangeProposal.class),
            Mockito.anyBoolean());
  }

  @Test
  public void testInvokeAssertionRunEventSuccessActionsActiveIncident() throws Exception {
    IncidentInfo activeIncidentInfo =
        new IncidentInfo()
            .setType(IncidentType.CUSTOM)
            .setPriority(2)
            .setTitle("Test Title")
            .setDescription("Test description")
            .setEntities(new UrnArray(ImmutableList.of(TEST_DATASET_URN)))
            .setStatus(
                new IncidentStatus()
                    .setState(IncidentState.ACTIVE)
                    .setLastUpdated(
                        new AuditStamp().setTime(0L).setActor(UrnUtils.getUrn(SYSTEM_ACTOR))))
            .setSource(
                new IncidentSource()
                    .setSourceUrn(TEST_ASSERTION_URN)
                    .setType(IncidentSourceType.ASSERTION_FAILURE))
            .setCreated(new AuditStamp().setActor(UrnUtils.getUrn(SYSTEM_ACTOR)).setTime(0L));

    SystemEntityClient entityClient =
        mockSystemEntityClient(
            TEST_INCIDENT_URN,
            activeIncidentInfo,
            TEST_ASSERTION_URN,
            new AssertionInfo()
                .setType(AssertionType.DATASET)
                .setDatasetAssertion(
                    new DatasetAssertionInfo()
                        .setDataset(TEST_DATASET_URN)
                        .setScope(DatasetAssertionScope.DATASET_COLUMN)),
            new AssertionActions()
                .setOnFailure(new AssertionActionArray())
                .setOnSuccess(
                    new AssertionActionArray(
                        ImmutableList.of(
                            new AssertionAction().setType(AssertionActionType.RESOLVE_INCIDENT)))));

    final AssertionActionsHook hook =
        new AssertionActionsHook(
                entityClient,
                Mockito.mock(GraphClient.class),
                true,
                Mockito.mock(OpenApiClient.class),
                objectMapper)
            .init(mockOperationContext());

    MetadataChangeLog event =
        buildMetadataChangeLog(
            TEST_ASSERTION_URN,
            ASSERTION_RUN_EVENT_ASPECT_NAME,
            ChangeType.UPSERT,
            buildAssertionRunEvent(
                TEST_ASSERTION_URN, AssertionRunStatus.COMPLETE, AssertionResultType.SUCCESS));

    hook.invoke(event);

    // Ensure that we looked up the assertion info correctly.
    Mockito.verify(entityClient, Mockito.times(2))
        .getV2(
            nullable(OperationContext.class),
            Mockito.eq(ASSERTION_ENTITY_NAME),
            Mockito.eq(TEST_ASSERTION_URN),
            Mockito.eq(
                ImmutableSet.of(
                    ASSERTION_INFO_ASPECT_NAME,
                    ASSERTION_ACTIONS_ASPECT_NAME,
                    DATA_PLATFORM_INSTANCE_ASPECT_NAME,
                    GLOBAL_TAGS_ASPECT_NAME)));

    // Ensure that we searched for the active incidents associated with the assertion..
    Mockito.verify(entityClient, Mockito.times(1))
        .search(
            Mockito.nullable(OperationContext.class),
            Mockito.eq(INCIDENT_ENTITY_NAME),
            Mockito.eq("*"),
            Mockito.any(Filter.class),
            Mockito.eq(null),
            Mockito.anyInt(),
            Mockito.anyInt());

    IncidentInfo expectedInfo = new IncidentInfo(activeIncidentInfo.data());
    expectedInfo.setStatus(
        new IncidentStatus()
            .setState(IncidentState.RESOLVED)
            .setMessage(
                "Auto-Resolved: The failing assertion which generated this incident is now passing.")
            .setLastUpdated(new AuditStamp().setActor(UrnUtils.getUrn(SYSTEM_ACTOR)).setTime(0L)));

    Mockito.verify(entityClient, Mockito.times(1))
        .ingestProposal(
            any(OperationContext.class),
            Mockito.argThat(
                new AssertionActionsHookIncidentInfoMatcher(
                    AspectUtils.buildMetadataChangeProposal(
                        TEST_INCIDENT_URN, INCIDENT_INFO_ASPECT_NAME, expectedInfo))),
            Mockito.anyBoolean());
  }

  @Test
  public void testInvokeInferredAssertionRunEventSuccess() throws Exception {
    SystemEntityClient entityClient =
        mockSystemEntityClient(
            TEST_ASSERTION_URN,
            new AssertionInfo()
                .setType(AssertionType.DATASET)
                .setSource(
                    new AssertionSource()
                        .setType(AssertionSourceType.INFERRED)) // Inferred Assertion!
                .setDatasetAssertion(
                    new DatasetAssertionInfo()
                        .setDataset(TEST_DATASET_URN)
                        .setScope(DatasetAssertionScope.DATASET_COLUMN)));

    final AssertionActionsHook hook =
        new AssertionActionsHook(
                entityClient,
                Mockito.mock(GraphClient.class),
                true,
                Mockito.mock(OpenApiClient.class),
                objectMapper)
            .init(mockOperationContext());

    MetadataChangeLog event =
        buildMetadataChangeLog(
            TEST_ASSERTION_URN,
            ASSERTION_RUN_EVENT_ASPECT_NAME,
            ChangeType.UPSERT,
            buildAssertionRunEvent(
                TEST_ASSERTION_URN, AssertionRunStatus.COMPLETE, AssertionResultType.SUCCESS));

    hook.invoke(event);

    // Ensure that we looked up the assertion actions correctly.
    Mockito.verify(entityClient, Mockito.times(2))
        .getV2(
            any(OperationContext.class),
            Mockito.eq(ASSERTION_ENTITY_NAME),
            Mockito.eq(TEST_ASSERTION_URN),
            Mockito.eq(
                ImmutableSet.of(
                    ASSERTION_INFO_ASPECT_NAME,
                    ASSERTION_ACTIONS_ASPECT_NAME,
                    DATA_PLATFORM_INSTANCE_ASPECT_NAME,
                    GLOBAL_TAGS_ASPECT_NAME)));

    // Verify that nothing was ingested in this case -- no anomalies to ingest since it was a
    // success.
    Mockito.verify(entityClient, Mockito.times(0))
        .ingestProposal(
            any(OperationContext.class),
            Mockito.any(MetadataChangeProposal.class),
            Mockito.anyBoolean());
  }

  @Test
  public void testInvokeAssertionRunEventFailureNoActions() throws Exception {
    SystemEntityClient entityClient =
        mockSystemEntityClient(
            null,
            null,
            TEST_ASSERTION_URN,
            new AssertionInfo()
                .setType(AssertionType.DATASET)
                .setDatasetAssertion(
                    new DatasetAssertionInfo()
                        .setDataset(TEST_DATASET_URN)
                        .setScope(DatasetAssertionScope.DATASET_COLUMN)),
            new AssertionActions()
                .setOnFailure(new AssertionActionArray())
                .setOnSuccess(new AssertionActionArray()));

    final AssertionActionsHook hook =
        new AssertionActionsHook(
                entityClient,
                Mockito.mock(GraphClient.class),
                true,
                Mockito.mock(OpenApiClient.class),
                objectMapper)
            .init(mockOperationContext());

    MetadataChangeLog event =
        buildMetadataChangeLog(
            TEST_ASSERTION_URN,
            ASSERTION_RUN_EVENT_ASPECT_NAME,
            ChangeType.UPSERT,
            buildAssertionRunEvent(
                TEST_ASSERTION_URN, AssertionRunStatus.COMPLETE, AssertionResultType.FAILURE));

    hook.invoke(event);

    // Ensure that we looked up the assertion actions correctly.
    Mockito.verify(entityClient, Mockito.times(2))
        .getV2(
            any(OperationContext.class),
            Mockito.eq(ASSERTION_ENTITY_NAME),
            Mockito.eq(TEST_ASSERTION_URN),
            Mockito.eq(
                ImmutableSet.of(
                    ASSERTION_INFO_ASPECT_NAME,
                    ASSERTION_ACTIONS_ASPECT_NAME,
                    DATA_PLATFORM_INSTANCE_ASPECT_NAME,
                    GLOBAL_TAGS_ASPECT_NAME)));

    // Ensure that we did not apply any actions or look up anything for incidents.
    Mockito.verify(entityClient, Mockito.times(0))
        .search(
            Mockito.any(),
            Mockito.eq(INCIDENT_ENTITY_NAME),
            Mockito.eq("*"),
            Mockito.any(Filter.class),
            Mockito.any(List.class),
            Mockito.anyInt(),
            Mockito.anyInt());

    // No ingestion to perform
    Mockito.verify(entityClient, Mockito.times(0))
        .ingestProposal(
            any(OperationContext.class),
            Mockito.any(MetadataChangeProposal.class),
            Mockito.anyBoolean());
  }

  @Test
  public void testInvokeAssertionRunEventFailureActionsNoIncident() throws Exception {
    AssertionInfo assertionInfo =
        new AssertionInfo()
            .setType(AssertionType.DATASET)
            .setDatasetAssertion(
                new DatasetAssertionInfo()
                    .setDataset(TEST_DATASET_URN)
                    .setScope(DatasetAssertionScope.DATASET_COLUMN)
                    .setOperator(AssertionStdOperator.NOT_NULL)
                    .setFields(
                        new UrnArray(
                            new DatasetFieldUrn(
                                new DatasetUrn(
                                    new DataPlatformUrn("hive"), "name", FabricType.PROD),
                                "id"))));

    SystemEntityClient entityClient =
        mockSystemEntityClient(
            null,
            null,
            TEST_ASSERTION_URN,
            assertionInfo,
            new AssertionActions()
                .setOnSuccess(new AssertionActionArray())
                .setOnFailure(
                    new AssertionActionArray(
                        ImmutableList.of(
                            new AssertionAction().setType(AssertionActionType.RAISE_INCIDENT)))));

    final AssertionActionsHook hook =
        new AssertionActionsHook(
                entityClient,
                Mockito.mock(GraphClient.class),
                true,
                Mockito.mock(OpenApiClient.class),
                objectMapper)
            .init(mockOperationContext());

    AssertionRunEvent testEvent =
        buildAssertionRunEvent(
            TEST_ASSERTION_URN, AssertionRunStatus.COMPLETE, AssertionResultType.FAILURE);

    MetadataChangeLog event =
        buildMetadataChangeLog(
            TEST_ASSERTION_URN, ASSERTION_RUN_EVENT_ASPECT_NAME, ChangeType.UPSERT, testEvent);

    hook.invoke(event);

    // Ensure that we looked up the assertion actions correctly.
    Mockito.verify(entityClient, Mockito.times(2))
        .getV2(
            any(OperationContext.class),
            Mockito.eq(ASSERTION_ENTITY_NAME),
            Mockito.eq(TEST_ASSERTION_URN),
            Mockito.eq(
                ImmutableSet.of(
                    ASSERTION_INFO_ASPECT_NAME,
                    ASSERTION_ACTIONS_ASPECT_NAME,
                    DATA_PLATFORM_INSTANCE_ASPECT_NAME,
                    GLOBAL_TAGS_ASPECT_NAME)));

    // Ensure that we searched for the active incidents associated with the assertion..
    Mockito.verify(entityClient, Mockito.times(1))
        .search(
            Mockito.any(),
            Mockito.eq(INCIDENT_ENTITY_NAME),
            Mockito.eq("*"),
            Mockito.any(Filter.class),
            Mockito.eq(null),
            Mockito.anyInt(),
            Mockito.anyInt());

    IncidentInfo expectedInfo = new IncidentInfo();
    expectedInfo.setType(IncidentType.CUSTOM);
    expectedInfo.setTitle(
        String.format(
            "%s Assertion `%s` has failed",
            AssertionUtils.getAssertionTypeName(assertionInfo.getType().toString()),
            AssertionUtils.buildAssertionDescription(TEST_ASSERTION_URN, assertionInfo)));
    expectedInfo.setDescription(
        AssertionUtils.buildAssertionResultReason(TEST_ASSERTION_URN, assertionInfo, testEvent));
    expectedInfo.setEntities(new UrnArray(ImmutableList.of(TEST_DATASET_URN)));
    expectedInfo.setSource(
        new IncidentSource()
            .setType(IncidentSourceType.ASSERTION_FAILURE)
            .setSourceUrn(TEST_ASSERTION_URN));
    expectedInfo.setStatus(
        new IncidentStatus()
            .setState(IncidentState.ACTIVE)
            .setMessage(
                "Auto-Raised: This incident was automatically generated by a failing assertion.")
            .setLastUpdated(new AuditStamp().setActor(UrnUtils.getUrn(SYSTEM_ACTOR)).setTime(0L)));
    expectedInfo.setCreated(new AuditStamp().setActor(UrnUtils.getUrn(SYSTEM_ACTOR)).setTime(0L));

    // Verify that a new assertion was created
    Mockito.verify(entityClient, Mockito.times(1))
        .ingestProposal(
            any(OperationContext.class),
            Mockito.argThat(
                new AssertionActionsHookIncidentInfoMatcher(
                    AspectUtils.buildMetadataChangeProposal(
                        TEST_INCIDENT_URN, INCIDENT_INFO_ASPECT_NAME, expectedInfo))),
            Mockito.anyBoolean());
  }

  @Test
  public void testInvokeAssertionRunEventFailureActionsWithIncident() throws Exception {
    IncidentInfo activeIncidentInfo =
        new IncidentInfo()
            .setEntities(new UrnArray(ImmutableList.of(TEST_DATASET_URN)))
            .setStatus(
                new IncidentStatus()
                    .setState(IncidentState.ACTIVE)
                    .setLastUpdated(
                        new AuditStamp().setTime(0L).setActor(UrnUtils.getUrn(SYSTEM_ACTOR))))
            .setSource(
                new IncidentSource()
                    .setSourceUrn(TEST_ASSERTION_URN)
                    .setType(IncidentSourceType.ASSERTION_FAILURE))
            .setType(IncidentType.CUSTOM);

    SystemEntityClient entityClient =
        mockSystemEntityClient(
            TEST_INCIDENT_URN,
            activeIncidentInfo,
            TEST_ASSERTION_URN,
            new AssertionInfo()
                .setType(AssertionType.DATASET)
                .setDatasetAssertion(
                    new DatasetAssertionInfo()
                        .setDataset(TEST_DATASET_URN)
                        .setScope(DatasetAssertionScope.DATASET_COLUMN)),
            new AssertionActions()
                .setOnSuccess(new AssertionActionArray())
                .setOnFailure(
                    new AssertionActionArray(
                        ImmutableList.of(
                            new AssertionAction().setType(AssertionActionType.RAISE_INCIDENT)))));

    final AssertionActionsHook hook =
        new AssertionActionsHook(
                entityClient,
                Mockito.mock(GraphClient.class),
                true,
                Mockito.mock(OpenApiClient.class),
                objectMapper)
            .init(mockOperationContext());

    MetadataChangeLog event =
        buildMetadataChangeLog(
            TEST_ASSERTION_URN,
            ASSERTION_RUN_EVENT_ASPECT_NAME,
            ChangeType.UPSERT,
            buildAssertionRunEvent(
                TEST_ASSERTION_URN, AssertionRunStatus.COMPLETE, AssertionResultType.FAILURE));

    hook.invoke(event);

    // Ensure that we looked up the assertion info correctly.
    Mockito.verify(entityClient, Mockito.times(2))
        .getV2(
            any(OperationContext.class),
            Mockito.eq(ASSERTION_ENTITY_NAME),
            Mockito.eq(TEST_ASSERTION_URN),
            Mockito.eq(
                ImmutableSet.of(
                    ASSERTION_INFO_ASPECT_NAME,
                    ASSERTION_ACTIONS_ASPECT_NAME,
                    DATA_PLATFORM_INSTANCE_ASPECT_NAME,
                    GLOBAL_TAGS_ASPECT_NAME)));

    // Ensure that we searched for the active incidents associated with the assertion..
    Mockito.verify(entityClient, Mockito.times(1))
        .search(
            Mockito.any(),
            Mockito.eq(INCIDENT_ENTITY_NAME),
            Mockito.eq("*"),
            Mockito.any(Filter.class),
            Mockito.eq(null),
            Mockito.anyInt(),
            Mockito.anyInt());

    // Verify that nothing was ingested due to existing active incident
    Mockito.verify(entityClient, Mockito.times(0))
        .ingestProposal(
            any(OperationContext.class),
            Mockito.any(MetadataChangeProposal.class),
            Mockito.anyBoolean());
  }

  @Test
  public void testInvokeInferredAssertionRunEventFailure() throws Exception {

    SystemEntityClient entityClient =
        mockSystemEntityClient(
            TEST_ASSERTION_URN,
            new AssertionInfo()
                .setType(AssertionType.DATASET)
                .setSource(
                    new AssertionSource()
                        .setType(AssertionSourceType.INFERRED)) // Inferred Assertion!
                .setDatasetAssertion(
                    new DatasetAssertionInfo()
                        .setDataset(TEST_DATASET_URN)
                        .setScope(DatasetAssertionScope.DATASET_COLUMN)));

    final AssertionActionsHook hook =
        new AssertionActionsHook(
                entityClient,
                Mockito.mock(GraphClient.class),
                true,
                Mockito.mock(OpenApiClient.class),
                objectMapper)
            .init(mockOperationContext());

    MetadataChangeLog event =
        buildMetadataChangeLog(
            TEST_ASSERTION_URN,
            ASSERTION_RUN_EVENT_ASPECT_NAME,
            ChangeType.UPSERT,
            buildAssertionRunEvent(
                TEST_ASSERTION_URN, AssertionRunStatus.COMPLETE, AssertionResultType.FAILURE));

    hook.invoke(event);

    // Ensure that we looked up the assertion info correctly.
    Mockito.verify(entityClient, Mockito.times(2))
        .getV2(
            any(OperationContext.class),
            Mockito.eq(ASSERTION_ENTITY_NAME),
            Mockito.eq(TEST_ASSERTION_URN),
            Mockito.eq(
                ImmutableSet.of(
                    ASSERTION_INFO_ASPECT_NAME,
                    ASSERTION_ACTIONS_ASPECT_NAME,
                    DATA_PLATFORM_INSTANCE_ASPECT_NAME,
                    GLOBAL_TAGS_ASPECT_NAME)));
  }

  @Test
  public void testInvokeAssertionSoftDeletedActiveIncident() throws Exception {
    // If assertion is soft deleted, then any active incidents resulting from it should be removed.

    IncidentInfo activeIncidentInfo =
        new IncidentInfo()
            .setEntities(new UrnArray(ImmutableList.of(TEST_DATASET_URN)))
            .setStatus(
                new IncidentStatus()
                    .setState(IncidentState.ACTIVE)
                    .setLastUpdated(
                        new AuditStamp().setTime(0L).setActor(UrnUtils.getUrn(SYSTEM_ACTOR))))
            .setSource(
                new IncidentSource()
                    .setSourceUrn(TEST_ASSERTION_URN)
                    .setType(IncidentSourceType.ASSERTION_FAILURE))
            .setType(IncidentType.CUSTOM);

    SystemEntityClient entityClient =
        mockSystemEntityClient(
            TEST_INCIDENT_URN,
            activeIncidentInfo,
            TEST_ASSERTION_URN,
            new AssertionInfo()
                .setType(AssertionType.DATASET)
                .setDatasetAssertion(
                    new DatasetAssertionInfo()
                        .setDataset(TEST_DATASET_URN)
                        .setScope(DatasetAssertionScope.DATASET_COLUMN)),
            new AssertionActions()
                .setOnSuccess(new AssertionActionArray())
                .setOnFailure(new AssertionActionArray()));

    final AssertionActionsHook hook =
        new AssertionActionsHook(
                entityClient,
                Mockito.mock(GraphClient.class),
                true,
                Mockito.mock(OpenApiClient.class),
                objectMapper)
            .init(mockOperationContext());

    MetadataChangeLog event =
        buildMetadataChangeLog(
            TEST_ASSERTION_URN, STATUS_ASPECT_NAME, ChangeType.UPSERT, buildStatus(true));

    hook.invoke(event);

    Mockito.verify(entityClient, Mockito.times(0))
        .getV2(
            any(OperationContext.class),
            Mockito.eq(ASSERTION_ENTITY_NAME),
            Mockito.eq(TEST_ASSERTION_URN),
            Mockito.eq(
                ImmutableSet.of(
                    ASSERTION_INFO_ASPECT_NAME,
                    ASSERTION_ACTIONS_ASPECT_NAME,
                    DATA_PLATFORM_INSTANCE_ASPECT_NAME,
                    GLOBAL_TAGS_ASPECT_NAME)));

    // Ensure that we searched for the active incidents associated with the assertion..
    Mockito.verify(entityClient, Mockito.times(1))
        .search(
            Mockito.any(),
            Mockito.eq(INCIDENT_ENTITY_NAME),
            Mockito.eq("*"),
            Mockito.any(Filter.class),
            Mockito.eq(null),
            Mockito.anyInt(),
            Mockito.anyInt());

    // Verify that we've deleted the active incidents.
    Mockito.verify(entityClient, Mockito.times(1))
        .deleteEntity(any(OperationContext.class), Mockito.eq(TEST_INCIDENT_URN));
    Mockito.verify(entityClient, Mockito.times(1))
        .deleteEntityReferences(any(OperationContext.class), Mockito.eq(TEST_INCIDENT_URN));
  }

  @Test
  public void testInvokeAssertionHardDeletedActiveIncident() throws Exception {
    // If assertion is hard deleted, then any active incidents resulting from it should be removed.

    IncidentInfo activeIncidentInfo =
        new IncidentInfo()
            .setEntities(new UrnArray(ImmutableList.of(TEST_DATASET_URN)))
            .setStatus(
                new IncidentStatus()
                    .setState(IncidentState.ACTIVE)
                    .setLastUpdated(
                        new AuditStamp().setTime(0L).setActor(UrnUtils.getUrn(SYSTEM_ACTOR))))
            .setSource(
                new IncidentSource()
                    .setSourceUrn(TEST_ASSERTION_URN)
                    .setType(IncidentSourceType.ASSERTION_FAILURE))
            .setType(IncidentType.CUSTOM);

    SystemEntityClient entityClient =
        mockSystemEntityClient(
            TEST_INCIDENT_URN,
            activeIncidentInfo,
            TEST_ASSERTION_URN,
            new AssertionInfo()
                .setType(AssertionType.DATASET)
                .setDatasetAssertion(
                    new DatasetAssertionInfo()
                        .setDataset(TEST_DATASET_URN)
                        .setScope(DatasetAssertionScope.DATASET_COLUMN)),
            new AssertionActions()
                .setOnSuccess(new AssertionActionArray())
                .setOnFailure(new AssertionActionArray()));

    final AssertionActionsHook hook =
        new AssertionActionsHook(
                entityClient,
                Mockito.mock(GraphClient.class),
                true,
                Mockito.mock(OpenApiClient.class),
                objectMapper)
            .init(mockOperationContext());

    MetadataChangeLog event =
        buildMetadataChangeLog(
            TEST_ASSERTION_URN, ASSERTION_KEY_ASPECT_NAME, ChangeType.DELETE, null);

    hook.invoke(event);

    Mockito.verify(entityClient, Mockito.times(0))
        .getV2(
            any(OperationContext.class),
            Mockito.eq(ASSERTION_ENTITY_NAME),
            Mockito.eq(TEST_ASSERTION_URN),
            Mockito.eq(
                ImmutableSet.of(
                    ASSERTION_INFO_ASPECT_NAME,
                    ASSERTION_ACTIONS_ASPECT_NAME,
                    DATA_PLATFORM_INSTANCE_ASPECT_NAME,
                    GLOBAL_TAGS_ASPECT_NAME)));

    // Ensure that we searched for the active incidents associated with the assertion..
    Mockito.verify(entityClient, Mockito.times(1))
        .search(
            Mockito.any(),
            Mockito.eq(INCIDENT_ENTITY_NAME),
            Mockito.eq("*"),
            Mockito.any(Filter.class),
            Mockito.eq(null),
            Mockito.anyInt(),
            Mockito.anyInt());

    // Verify that we've deleted the active incidents.
    Mockito.verify(entityClient, Mockito.times(1))
        .deleteEntity(any(OperationContext.class), Mockito.eq(TEST_INCIDENT_URN));
    Mockito.verify(entityClient, Mockito.times(1))
        .deleteEntityReferences(any(OperationContext.class), Mockito.eq(TEST_INCIDENT_URN));
  }

  private SystemEntityClient mockSystemEntityClient(
      Urn incidentUrn,
      IncidentInfo incidentInfo,
      Urn assertionUrn,
      AssertionInfo assertionInfo,
      AssertionActions assertionActions)
      throws Exception {
    SystemEntityClient mockClient = mock(SystemEntityClient.class);

    if (incidentUrn != null) {
      when(mockClient.getV2(
              any(OperationContext.class),
              Mockito.eq(INCIDENT_ENTITY_NAME),
              Mockito.eq(incidentUrn),
              Mockito.eq(ImmutableSet.of(INCIDENT_INFO_ASPECT_NAME))))
          .thenReturn(
              new EntityResponse()
                  .setUrn(incidentUrn)
                  .setEntityName(INCIDENT_ENTITY_NAME)
                  .setAspects(
                      new EnvelopedAspectMap(
                          ImmutableMap.of(
                              INCIDENT_INFO_ASPECT_NAME,
                              new EnvelopedAspect()
                                  .setName(INCIDENT_INFO_ASPECT_NAME)
                                  .setValue(new Aspect(incidentInfo.data()))))));
    }

    if (assertionUrn != null) {
      when(mockClient.getV2(
              any(OperationContext.class),
              Mockito.eq(ASSERTION_ENTITY_NAME),
              Mockito.eq(assertionUrn),
              Mockito.eq(
                  ImmutableSet.of(
                      ASSERTION_INFO_ASPECT_NAME,
                      ASSERTION_ACTIONS_ASPECT_NAME,
                      DATA_PLATFORM_INSTANCE_ASPECT_NAME,
                      GLOBAL_TAGS_ASPECT_NAME))))
          .thenReturn(
              new EntityResponse()
                  .setUrn(assertionUrn)
                  .setEntityName(ASSERTION_ENTITY_NAME)
                  .setAspects(
                      new EnvelopedAspectMap(
                          ImmutableMap.of(
                              ASSERTION_INFO_ASPECT_NAME,
                              new EnvelopedAspect()
                                  .setName(ASSERTION_INFO_ASPECT_NAME)
                                  .setValue(new Aspect(assertionInfo.data())),
                              ASSERTION_ACTIONS_ASPECT_NAME,
                              new EnvelopedAspect()
                                  .setName(ASSERTION_ACTIONS_ASPECT_NAME)
                                  .setValue(new Aspect(assertionActions.data()))))));
    }

    SearchEntityArray searchEntities =
        incidentUrn == null
            ? new SearchEntityArray(Collections.emptyList())
            : new SearchEntityArray(ImmutableList.of(new SearchEntity().setEntity(incidentUrn)));

    when(mockClient.search(
            Mockito.any(),
            Mockito.eq(INCIDENT_ENTITY_NAME),
            Mockito.eq("*"),
            Mockito.any(Filter.class),
            Mockito.eq(null),
            Mockito.anyInt(),
            Mockito.anyInt()))
        .thenReturn(
            new SearchResult()
                .setNumEntities(1)
                .setPageSize(1)
                .setFrom(0)
                .setEntities(searchEntities));

    return mockClient;
  }

  private SystemEntityClient mockSystemEntityClient(Urn assertionUrn, AssertionInfo assertionInfo)
      throws Exception {
    SystemEntityClient mockClient = mock(SystemEntityClient.class);

    if (assertionUrn != null) {
      when(mockClient.getV2(
              any(OperationContext.class),
              Mockito.eq(ASSERTION_ENTITY_NAME),
              Mockito.eq(assertionUrn),
              Mockito.eq(
                  ImmutableSet.of(
                      ASSERTION_INFO_ASPECT_NAME,
                      ASSERTION_ACTIONS_ASPECT_NAME,
                      DATA_PLATFORM_INSTANCE_ASPECT_NAME,
                      GLOBAL_TAGS_ASPECT_NAME))))
          .thenReturn(
              new EntityResponse()
                  .setUrn(assertionUrn)
                  .setEntityName(ASSERTION_ENTITY_NAME)
                  .setAspects(
                      new EnvelopedAspectMap(
                          ImmutableMap.of(
                              ASSERTION_INFO_ASPECT_NAME,
                              new EnvelopedAspect()
                                  .setName(ASSERTION_INFO_ASPECT_NAME)
                                  .setValue(new Aspect(assertionInfo.data()))))));
    }

    return mockClient;
  }

  @Nonnull
  private Status buildStatus(final boolean removed) {
    final Status result = new Status();
    result.setRemoved(removed);
    return result;
  }

  private AssertionRunEvent buildAssertionRunEvent(
      final Urn urn, final AssertionRunStatus status, final AssertionResultType resultType) {
    AssertionRunEvent event = new AssertionRunEvent();
    event.setTimestampMillis(1L);
    event.setAssertionUrn(urn);
    event.setStatus(status);
    event.setResult(new AssertionResult().setType(resultType).setRowCount(0L));
    return event;
  }

  private MetadataChangeLog buildMetadataChangeLog(
      Urn urn, String aspectName, ChangeType changeType, RecordTemplate aspect) throws Exception {
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityUrn(urn);
    event.setEntityType(urn.getEntityType());
    event.setAspectName(aspectName);
    event.setChangeType(changeType);
    if (aspect != null) {
      event.setAspect(GenericRecordUtils.serializeAspect(aspect));
    }
    return event;
  }
}
