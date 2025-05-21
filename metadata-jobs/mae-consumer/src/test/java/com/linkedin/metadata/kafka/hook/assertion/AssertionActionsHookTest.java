package com.linkedin.metadata.kafka.hook.assertion;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.kafka.hook.EntityRegistryTestUtil.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.anomaly.AnomalySource;
import com.linkedin.anomaly.AnomalySourceType;
import com.linkedin.anomaly.MonitorAnomalyEvent;
import com.linkedin.assertion.AssertionAction;
import com.linkedin.assertion.AssertionActionArray;
import com.linkedin.assertion.AssertionActionType;
import com.linkedin.assertion.AssertionActions;
import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionMetric;
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
import com.linkedin.common.EntityRelationship;
import com.linkedin.common.EntityRelationshipArray;
import com.linkedin.common.EntityRelationships;
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
import com.linkedin.metadata.query.filter.RelationshipDirection;
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
import org.mockito.ArgumentCaptor;
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
                    GLOBAL_TAGS_ASPECT_NAME,
                    ASSERTION_RUN_SUMMARY_ASPECT_NAME)));

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
                    GLOBAL_TAGS_ASPECT_NAME,
                    ASSERTION_RUN_SUMMARY_ASPECT_NAME)));

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
                    GLOBAL_TAGS_ASPECT_NAME,
                    ASSERTION_RUN_SUMMARY_ASPECT_NAME)));

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
                    GLOBAL_TAGS_ASPECT_NAME,
                    ASSERTION_RUN_SUMMARY_ASPECT_NAME)));

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
                    GLOBAL_TAGS_ASPECT_NAME,
                    ASSERTION_RUN_SUMMARY_ASPECT_NAME)));

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
                    GLOBAL_TAGS_ASPECT_NAME,
                    ASSERTION_RUN_SUMMARY_ASPECT_NAME)));

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
                    GLOBAL_TAGS_ASPECT_NAME,
                    ASSERTION_RUN_SUMMARY_ASPECT_NAME)));

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
                    GLOBAL_TAGS_ASPECT_NAME,
                    ASSERTION_RUN_SUMMARY_ASPECT_NAME)));
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

  @Test
  public void testInvokeInferredAssertionRunEventFailureProducesAnomalyEvent() throws Exception {
    // Mock a monitor URN that should be returned when looking for the monitor associated with the
    // assertion
    final Urn testMonitorUrn = UrnUtils.getUrn("urn:li:dataMonitor:test-monitor");

    // Create a mock entity client
    SystemEntityClient entityClient = mock(SystemEntityClient.class);

    // Setup the mock entity client to return assertion info for the test assertion
    AssertionInfo assertionInfo =
        new AssertionInfo()
            .setType(AssertionType.DATASET)
            .setSource(
                new AssertionSource().setType(AssertionSourceType.INFERRED)) // Inferred Assertion!
            .setDatasetAssertion(
                new DatasetAssertionInfo()
                    .setDataset(TEST_DATASET_URN)
                    .setScope(DatasetAssertionScope.DATASET_COLUMN));

    when(entityClient.getV2(
            any(OperationContext.class),
            eq(ASSERTION_ENTITY_NAME),
            eq(TEST_ASSERTION_URN),
            eq(
                ImmutableSet.of(
                    ASSERTION_INFO_ASPECT_NAME,
                    ASSERTION_ACTIONS_ASPECT_NAME,
                    DATA_PLATFORM_INSTANCE_ASPECT_NAME,
                    GLOBAL_TAGS_ASPECT_NAME,
                    ASSERTION_RUN_SUMMARY_ASPECT_NAME))))
        .thenReturn(
            new EntityResponse()
                .setUrn(TEST_ASSERTION_URN)
                .setEntityName(ASSERTION_ENTITY_NAME)
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            ASSERTION_INFO_ASPECT_NAME,
                            new EnvelopedAspect()
                                .setName(ASSERTION_INFO_ASPECT_NAME)
                                .setValue(new Aspect(assertionInfo.data()))))));

    // Create a mock graph client that will return our test monitor URN
    GraphClient mockGraphClient = mock(GraphClient.class);

    // Create entities relationship result with a monitor URN
    EntityRelationships relationships = new EntityRelationships();
    EntityRelationship relationship = new EntityRelationship();
    relationship.setEntity(testMonitorUrn);
    relationships.setRelationships(new EntityRelationshipArray(ImmutableList.of(relationship)));

    // Setup mock to return the relationship when queried
    when(mockGraphClient.getRelatedEntities(
            eq(TEST_ASSERTION_URN.toString()),
            eq(ImmutableSet.of("Evaluates")),
            eq(RelationshipDirection.INCOMING),
            eq(0),
            eq(1),
            anyString()))
        .thenReturn(relationships);

    // Create an instance of the hook with our mocked GraphClient
    final AssertionActionsHook hook =
        new AssertionActionsHook(
                entityClient, mockGraphClient, true, mock(OpenApiClient.class), objectMapper)
            .init(mockOperationContext());

    // Create a failure event for the inferred assertion with a metric value
    AssertionRunEvent testEvent =
        buildAssertionRunEvent(
            TEST_ASSERTION_URN, AssertionRunStatus.COMPLETE, AssertionResultType.FAILURE);
    AssertionMetric assertionMetric = new AssertionMetric().setValue(1.0f).setTimestampMs(1);
    testEvent.getResult().setMetric(assertionMetric); // Set a metric value to test its propagation

    MetadataChangeLog event =
        buildMetadataChangeLog(
            TEST_ASSERTION_URN, ASSERTION_RUN_EVENT_ASPECT_NAME, ChangeType.UPSERT, testEvent);

    // Invoke the hook with our event
    hook.invoke(event);

    // Verify that the graph client was called to find related entities
    verify(mockGraphClient, times(1))
        .getRelatedEntities(
            eq(TEST_ASSERTION_URN.toString()),
            eq(ImmutableSet.of("Evaluates")),
            eq(RelationshipDirection.INCOMING),
            eq(0),
            eq(1),
            anyString());

    // Verify that a monitor anomaly event was created
    ArgumentCaptor<MetadataChangeProposal> proposalCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(entityClient, times(1))
        .ingestProposal(any(OperationContext.class), proposalCaptor.capture(), eq(true));

    // Validate the monitor anomaly event properties
    MetadataChangeProposal proposal = proposalCaptor.getValue();
    assertEquals(testMonitorUrn, proposal.getEntityUrn());
    assertEquals(MONITOR_ANOMALY_EVENT_ASPECT_NAME, proposal.getAspectName());

    // Extract and validate the anomaly event
    MonitorAnomalyEvent anomalyEvent =
        GenericRecordUtils.deserializeAspect(
            proposal.getAspect().getValue(),
            proposal.getAspect().getContentType(),
            MonitorAnomalyEvent.class);

    assertEquals(testEvent.getTimestampMillis(), anomalyEvent.getTimestampMillis());
    assertEquals(testEvent.getTimestampMillis(), anomalyEvent.getCreated().getTime());
    assertEquals(testEvent.getTimestampMillis(), anomalyEvent.getLastUpdated().getTime());

    // Check the source properties
    AnomalySource source = anomalyEvent.getSource();
    assertEquals(TEST_ASSERTION_URN, source.getSourceUrn());
    assertEquals(AnomalySourceType.INFERRED_ASSERTION_FAILURE, source.getType());
    assertTrue(source.hasProperties());
    assertEquals(
        assertionMetric.getTimestampMs(),
        source.getProperties().getAssertionMetric().getTimestampMs());
    assertEquals(
        assertionMetric.getValue(), source.getProperties().getAssertionMetric().getValue());
  }

  @Test
  public void testInvokeNonInferredAssertionRunEventFailureDoesNotProduceAnomalyEvent()
      throws Exception {
    // Create a mock entity client
    SystemEntityClient entityClient = mock(SystemEntityClient.class);

    // Setup the mock entity client to return assertion info without inferred source
    AssertionInfo assertionInfo =
        new AssertionInfo()
            .setType(AssertionType.DATASET)
            // Note: No source means it's not an inferred assertion
            .setDatasetAssertion(
                new DatasetAssertionInfo()
                    .setDataset(TEST_DATASET_URN)
                    .setScope(DatasetAssertionScope.DATASET_COLUMN));

    when(entityClient.getV2(
            any(OperationContext.class),
            eq(ASSERTION_ENTITY_NAME),
            eq(TEST_ASSERTION_URN),
            eq(
                ImmutableSet.of(
                    ASSERTION_INFO_ASPECT_NAME,
                    ASSERTION_ACTIONS_ASPECT_NAME,
                    DATA_PLATFORM_INSTANCE_ASPECT_NAME,
                    GLOBAL_TAGS_ASPECT_NAME,
                    ASSERTION_RUN_SUMMARY_ASPECT_NAME))))
        .thenReturn(
            new EntityResponse()
                .setUrn(TEST_ASSERTION_URN)
                .setEntityName(ASSERTION_ENTITY_NAME)
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            ASSERTION_INFO_ASPECT_NAME,
                            new EnvelopedAspect()
                                .setName(ASSERTION_INFO_ASPECT_NAME)
                                .setValue(new Aspect(assertionInfo.data()))))));

    // Mock graph client but it should not be used for non-inferred assertions
    GraphClient mockGraphClient = mock(GraphClient.class);

    final AssertionActionsHook hook =
        new AssertionActionsHook(
                entityClient, mockGraphClient, true, mock(OpenApiClient.class), objectMapper)
            .init(mockOperationContext());

    // Create a failure event for the non-inferred assertion
    AssertionRunEvent testEvent =
        buildAssertionRunEvent(
            TEST_ASSERTION_URN, AssertionRunStatus.COMPLETE, AssertionResultType.FAILURE);

    MetadataChangeLog event =
        buildMetadataChangeLog(
            TEST_ASSERTION_URN, ASSERTION_RUN_EVENT_ASPECT_NAME, ChangeType.UPSERT, testEvent);

    // Invoke the hook with our event
    hook.invoke(event);

    // Verify that we looked up the assertion info
    verify(entityClient, times(2))
        .getV2(
            any(OperationContext.class),
            eq(ASSERTION_ENTITY_NAME),
            eq(TEST_ASSERTION_URN),
            eq(
                ImmutableSet.of(
                    ASSERTION_INFO_ASPECT_NAME,
                    ASSERTION_ACTIONS_ASPECT_NAME,
                    DATA_PLATFORM_INSTANCE_ASPECT_NAME,
                    GLOBAL_TAGS_ASPECT_NAME,
                    ASSERTION_RUN_SUMMARY_ASPECT_NAME)));

    // Verify that the graph client was NOT called since this is not an inferred assertion
    verify(mockGraphClient, times(0))
        .getRelatedEntities(
            anyString(),
            anySet(),
            any(RelationshipDirection.class),
            anyInt(),
            anyInt(),
            anyString());

    // Verify that NO monitor anomaly event was created
    verify(entityClient, times(0))
        .ingestProposal(
            any(OperationContext.class),
            argThat(
                proposal ->
                    proposal != null
                        && MONITOR_ANOMALY_EVENT_ASPECT_NAME.equals(proposal.getAspectName())),
            anyBoolean());
  }

  @Test
  public void testMonitorUrnNotFound() throws Exception {
    // Create a mock entity client
    SystemEntityClient entityClient = mock(SystemEntityClient.class);

    // Setup the mock entity client to return assertion info for the test assertion
    AssertionInfo assertionInfo =
        new AssertionInfo()
            .setType(AssertionType.DATASET)
            .setSource(new AssertionSource().setType(AssertionSourceType.INFERRED))
            .setDatasetAssertion(
                new DatasetAssertionInfo()
                    .setDataset(TEST_DATASET_URN)
                    .setScope(DatasetAssertionScope.DATASET_COLUMN));

    when(entityClient.getV2(
            any(OperationContext.class),
            eq(ASSERTION_ENTITY_NAME),
            eq(TEST_ASSERTION_URN),
            eq(
                ImmutableSet.of(
                    ASSERTION_INFO_ASPECT_NAME,
                    ASSERTION_ACTIONS_ASPECT_NAME,
                    DATA_PLATFORM_INSTANCE_ASPECT_NAME,
                    GLOBAL_TAGS_ASPECT_NAME,
                    ASSERTION_RUN_SUMMARY_ASPECT_NAME))))
        .thenReturn(
            new EntityResponse()
                .setUrn(TEST_ASSERTION_URN)
                .setEntityName(ASSERTION_ENTITY_NAME)
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            ASSERTION_INFO_ASPECT_NAME,
                            new EnvelopedAspect()
                                .setName(ASSERTION_INFO_ASPECT_NAME)
                                .setValue(new Aspect(assertionInfo.data()))))));

    // Create a mock graph client that will return an empty relationship list
    GraphClient mockGraphClient = mock(GraphClient.class);

    // Return empty relationships to simulate no monitor found
    EntityRelationships emptyRelationships = new EntityRelationships();
    emptyRelationships.setRelationships(new EntityRelationshipArray());

    when(mockGraphClient.getRelatedEntities(
            eq(TEST_ASSERTION_URN.toString()),
            eq(ImmutableSet.of("Evaluates")),
            eq(RelationshipDirection.INCOMING),
            eq(0),
            eq(1),
            anyString()))
        .thenReturn(emptyRelationships);

    final AssertionActionsHook hook =
        new AssertionActionsHook(
                entityClient, mockGraphClient, true, mock(OpenApiClient.class), objectMapper)
            .init(mockOperationContext());

    // Create a failure event for the inferred assertion
    AssertionRunEvent testEvent =
        buildAssertionRunEvent(
            TEST_ASSERTION_URN, AssertionRunStatus.COMPLETE, AssertionResultType.FAILURE);

    MetadataChangeLog event =
        buildMetadataChangeLog(
            TEST_ASSERTION_URN, ASSERTION_RUN_EVENT_ASPECT_NAME, ChangeType.UPSERT, testEvent);

    // Invoke the hook with our event
    hook.invoke(event);

    // Verify that the graph client was called to search for relationships
    verify(mockGraphClient, times(1))
        .getRelatedEntities(
            eq(TEST_ASSERTION_URN.toString()),
            eq(ImmutableSet.of("Evaluates")),
            eq(RelationshipDirection.INCOMING),
            eq(0),
            eq(1),
            anyString());

    // Verify that NO monitor anomaly event was created since no monitor was found
    verify(entityClient, times(0))
        .ingestProposal(
            any(OperationContext.class), any(MetadataChangeProposal.class), anyBoolean());
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
                      GLOBAL_TAGS_ASPECT_NAME,
                      ASSERTION_RUN_SUMMARY_ASPECT_NAME))))
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
                      GLOBAL_TAGS_ASPECT_NAME,
                      ASSERTION_RUN_SUMMARY_ASPECT_NAME))))
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
