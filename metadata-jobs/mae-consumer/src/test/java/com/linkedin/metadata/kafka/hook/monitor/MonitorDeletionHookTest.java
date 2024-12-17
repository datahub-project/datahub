package com.linkedin.metadata.kafka.hook.monitor;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.kafka.hook.EntityRegistryTestUtil.*;
import static org.mockito.Mockito.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.anomaly.AnomalyInfo;
import com.linkedin.assertion.AssertionActions;
import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionResult;
import com.linkedin.assertion.AssertionResultType;
import com.linkedin.assertion.AssertionRunEvent;
import com.linkedin.assertion.AssertionRunStatus;
import com.linkedin.common.EntityRelationship;
import com.linkedin.common.EntityRelationshipArray;
import com.linkedin.common.EntityRelationships;
import com.linkedin.common.FabricType;
import com.linkedin.common.Status;
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
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.key.AssertionKey;
import com.linkedin.metadata.key.DatasetKey;
import com.linkedin.metadata.key.MonitorKey;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.net.URISyntaxException;
import java.util.Collections;
import javax.annotation.Nonnull;
import org.mockito.Mockito;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class MonitorDeletionHookTest {
  private static final Urn TEST_ASSERTION_URN = UrnUtils.getUrn("urn:li:assertion:test");
  private static final Urn TEST_MONITOR_URN =
      UrnUtils.getUrn("urn:li:monitor:(urn:li:dataset:(urn:li:dataPlatform:hive,name,PROD),test)");
  private static final Urn TEST_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,name,PROD)");
  private OperationContext opContext;

  @BeforeTest
  public void setup() {
    opContext = TestOperationContexts.systemContextNoSearchAuthorization();
  }

  @Test
  public void testInvokeNotEnabled() throws Exception {
    SystemEntityClient entityClient = mock(SystemEntityClient.class);
    GraphClient graphClient = mock(GraphClient.class);

    final MonitorDeletionHook hook =
        new MonitorDeletionHook(entityClient, graphClient, false).init(opContext);

    final MetadataChangeLog event =
        buildMetadataChangeLog(
            TEST_ASSERTION_URN,
            ASSERTION_KEY_ASPECT_NAME,
            ChangeType.DELETE,
            buildAssertionKey(TEST_ASSERTION_URN));
    hook.invoke(event);

    Mockito.verify(graphClient, Mockito.times(0))
        .getRelatedEntities(
            Mockito.anyString(),
            Mockito.anyList(),
            Mockito.any(RelationshipDirection.class),
            Mockito.anyInt(),
            Mockito.anyInt(),
            Mockito.anyString());

    Mockito.verify(entityClient, Mockito.times(0))
        .deleteEntity(any(OperationContext.class), Mockito.any(Urn.class));
  }

  @Test
  public void testInvokeNotEligibleChange() throws Exception {
    SystemEntityClient entityClient = mock(SystemEntityClient.class);
    GraphClient graphClient = mock(GraphClient.class);

    final MonitorDeletionHook hook =
        new MonitorDeletionHook(entityClient, graphClient, true).init(opContext);

    // Case 1: Incorrect aspect --- Assertion Info
    MetadataChangeLog event =
        buildMetadataChangeLog(
            TEST_ASSERTION_URN, ASSERTION_INFO_ASPECT_NAME, ChangeType.UPSERT, new AssertionInfo());
    hook.invoke(event);

    Mockito.verify(graphClient, Mockito.times(0))
        .getRelatedEntities(
            Mockito.anyString(),
            Mockito.anyList(),
            Mockito.any(RelationshipDirection.class),
            Mockito.anyInt(),
            Mockito.anyInt(),
            Mockito.anyString());
    Mockito.verify(entityClient, Mockito.times(0))
        .deleteEntity(any(OperationContext.class), Mockito.any(Urn.class));

    // Case 2: Incorrect aspect - But Delete
    event =
        buildMetadataChangeLog(
            TEST_ASSERTION_URN,
            ASSERTION_RUN_EVENT_ASPECT_NAME,
            ChangeType.DELETE,
            buildAssertionRunEvent(
                TEST_ASSERTION_URN, AssertionRunStatus.COMPLETE, AssertionResultType.SUCCESS));
    hook.invoke(event);

    Mockito.verify(graphClient, Mockito.times(0))
        .getRelatedEntities(
            Mockito.anyString(),
            Mockito.anyList(),
            Mockito.any(RelationshipDirection.class),
            Mockito.anyInt(),
            Mockito.anyInt(),
            Mockito.anyString());
    Mockito.verify(entityClient, Mockito.times(0))
        .getV2(
            any(OperationContext.class),
            Mockito.anyString(),
            Mockito.any(Urn.class),
            Mockito.anySet());

    // Case 3: Incorrect aspect - Soft delete of assertion
    event =
        buildMetadataChangeLog(
            TEST_ASSERTION_URN, STATUS_ASPECT_NAME, ChangeType.UPSERT, buildStatus(true));
    hook.invoke(event);
    Mockito.verify(graphClient, Mockito.times(0))
        .getRelatedEntities(
            Mockito.anyString(),
            Mockito.anyList(),
            Mockito.any(RelationshipDirection.class),
            Mockito.anyInt(),
            Mockito.anyInt(),
            Mockito.anyString());
    Mockito.verify(entityClient, Mockito.times(0))
        .getV2(
            any(OperationContext.class),
            Mockito.anyString(),
            Mockito.any(Urn.class),
            Mockito.anySet());
  }

  @Test
  public void testInvokeMonitorDeleteFollowsAssertionHardDelete() throws Exception {
    SystemEntityClient entityClient = mock(SystemEntityClient.class);
    GraphClient graphClient = initGraphClient();

    final MonitorDeletionHook hook =
        new MonitorDeletionHook(entityClient, graphClient, true).init(opContext);

    MetadataChangeLog event =
        buildMetadataChangeLog(
            TEST_ASSERTION_URN,
            ASSERTION_KEY_ASPECT_NAME,
            ChangeType.DELETE,
            buildAssertionKey(TEST_ASSERTION_URN));
    hook.invoke(event);
    Mockito.verify(entityClient, Mockito.times(1))
        .deleteEntity(any(OperationContext.class), Mockito.eq(TEST_MONITOR_URN));
  }

  @Test
  public void testInvokeAssertionDeleteFollowsMonitorHardDelete() throws Exception {
    SystemEntityClient entityClient = mock(SystemEntityClient.class);
    GraphClient graphClient = initGraphClient();

    final MonitorDeletionHook hook =
        new MonitorDeletionHook(entityClient, graphClient, true).init(opContext);

    MetadataChangeLog event =
        buildMetadataChangeLog(
            TEST_MONITOR_URN,
            MONITOR_KEY_ASPECT_NAME,
            ChangeType.DELETE,
            buildMonitorKey(TEST_MONITOR_URN));
    hook.invoke(event);
    Mockito.verify(entityClient, Mockito.times(1))
        .deleteEntity(any(OperationContext.class), Mockito.eq(TEST_ASSERTION_URN));
  }

  @Test
  public void testInvokeNoActionFollowsAssertionHardDeleteIfNoLinkedMonitor() throws Exception {
    SystemEntityClient entityClient = mock(SystemEntityClient.class);
    GraphClient graphClient = mock(GraphClient.class);
    Mockito.when(
            graphClient.getRelatedEntities(
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any()))
        .thenReturn(new EntityRelationships().setRelationships(new EntityRelationshipArray()));

    final MonitorDeletionHook hook =
        new MonitorDeletionHook(entityClient, graphClient, true).init(opContext);

    MetadataChangeLog event =
        buildMetadataChangeLog(
            TEST_ASSERTION_URN,
            ASSERTION_KEY_ASPECT_NAME,
            ChangeType.DELETE,
            buildAssertionKey(TEST_ASSERTION_URN));
    hook.invoke(event);
    Mockito.verify(entityClient, Mockito.times(0))
        .deleteEntity(any(OperationContext.class), Mockito.any());
  }

  @Test
  public void testInvokeNoActionFollowsMonitorHardDeleteIfNoLinkedAssertion() throws Exception {
    SystemEntityClient entityClient = mock(SystemEntityClient.class);
    GraphClient graphClient = Mockito.mock(GraphClient.class);
    Mockito.when(
            graphClient.getRelatedEntities(
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any()))
        .thenReturn(new EntityRelationships().setRelationships(new EntityRelationshipArray()));

    final MonitorDeletionHook hook =
        new MonitorDeletionHook(entityClient, graphClient, true).init(opContext);

    MetadataChangeLog event =
        buildMetadataChangeLog(
            TEST_MONITOR_URN,
            MONITOR_KEY_ASPECT_NAME,
            ChangeType.DELETE,
            buildMonitorKey(TEST_MONITOR_URN));
    hook.invoke(event);
    Mockito.verify(entityClient, Mockito.times(0))
        .deleteEntity(any(OperationContext.class), Mockito.any());
  }

  @Test
  public void testInvokeMonitorDeleteFollowsDatasetHardDelete() throws Exception {
    SystemEntityClient entityClient = mock(SystemEntityClient.class);
    GraphClient graphClient = initGraphClient();

    final MonitorDeletionHook hook =
        new MonitorDeletionHook(entityClient, graphClient, true).init(opContext);

    MetadataChangeLog event =
        buildMetadataChangeLog(
            TEST_DATASET_URN,
            DATASET_KEY_ASPECT_NAME,
            ChangeType.DELETE,
            buildDatasetKey(TEST_ASSERTION_URN));
    hook.invoke(event);
    Mockito.verify(entityClient, Mockito.times(1))
        .deleteEntity(any(OperationContext.class), Mockito.eq(TEST_MONITOR_URN));
  }

  private static GraphClient initGraphClient() {
    GraphClient graphClient = mock(GraphClient.class);
    Mockito.when(
            graphClient.getRelatedEntities(
                Mockito.eq(TEST_ASSERTION_URN.toString()),
                Mockito.anyList(),
                Mockito.eq(RelationshipDirection.INCOMING),
                Mockito.any(),
                Mockito.any(),
                Mockito.any()))
        .thenReturn(
            new EntityRelationships()
                .setRelationships(
                    new EntityRelationshipArray(
                        ImmutableList.of(new EntityRelationship().setEntity(TEST_MONITOR_URN)))));

    Mockito.when(
            graphClient.getRelatedEntities(
                Mockito.eq(TEST_MONITOR_URN.toString()),
                Mockito.anyList(),
                Mockito.eq(RelationshipDirection.OUTGOING),
                Mockito.any(),
                Mockito.any(),
                Mockito.any()))
        .thenReturn(
            new EntityRelationships()
                .setRelationships(
                    new EntityRelationshipArray(
                        ImmutableList.of(new EntityRelationship().setEntity(TEST_ASSERTION_URN)))));

    Mockito.when(
            graphClient.getRelatedEntities(
                Mockito.eq(TEST_DATASET_URN.toString()),
                Mockito.eq(ImmutableList.of("Monitors")),
                Mockito.eq(RelationshipDirection.INCOMING),
                Mockito.any(),
                Mockito.any(),
                Mockito.any()))
        .thenReturn(
            new EntityRelationships()
                .setRelationships(
                    new EntityRelationshipArray(
                        ImmutableList.of(new EntityRelationship().setEntity(TEST_MONITOR_URN)))));
    return graphClient;
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
                      DATA_PLATFORM_INSTANCE_ASPECT_NAME))))
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

  private SystemEntityClient mockSystemEntityClient(
      Urn anomalyUrn, AnomalyInfo anomalyInfo, Urn assertionUrn, AssertionInfo assertionInfo)
      throws Exception {
    SystemEntityClient mockClient = mock(SystemEntityClient.class);

    if (anomalyUrn != null) {
      when(mockClient.getV2(
              any(OperationContext.class),
              Mockito.eq(ANOMALY_ENTITY_NAME),
              Mockito.eq(anomalyUrn),
              Mockito.eq(ImmutableSet.of(ANOMALY_INFO_ASPECT_NAME))))
          .thenReturn(
              new EntityResponse()
                  .setUrn(anomalyUrn)
                  .setEntityName(ANOMALY_ENTITY_NAME)
                  .setAspects(
                      new EnvelopedAspectMap(
                          ImmutableMap.of(
                              ANOMALY_INFO_ASPECT_NAME,
                              new EnvelopedAspect()
                                  .setName(ANOMALY_INFO_ASPECT_NAME)
                                  .setValue(new Aspect(anomalyInfo.data()))))));
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
                      DATA_PLATFORM_INSTANCE_ASPECT_NAME))))
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

    SearchEntityArray searchEntities =
        anomalyUrn == null
            ? new SearchEntityArray(Collections.emptyList())
            : new SearchEntityArray(ImmutableList.of(new SearchEntity().setEntity(anomalyUrn)));

    when(mockClient.search(
            Mockito.any(),
            Mockito.eq(ANOMALY_ENTITY_NAME),
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

  private AssertionKey buildAssertionKey(final Urn urn) {
    AssertionKey event = new AssertionKey();
    event.setAssertionId(urn.getId());
    return event;
  }

  private MonitorKey buildMonitorKey(final Urn urn) throws URISyntaxException {
    MonitorKey event = new MonitorKey();
    event.setEntity(Urn.createFromString(urn.getEntityKey().get(0)));
    event.setId(urn.getEntityKey().get(1));
    return event;
  }

  private DatasetKey buildDatasetKey(final Urn urn) {
    DatasetKey event = new DatasetKey();
    event.setName("test");
    event.setOrigin(FabricType.DEV);
    event.setPlatform(UrnUtils.getUrn("urn:li:dataPlatform:snowflake"));
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
