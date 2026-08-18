package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.IncidentsSummary;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
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
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import org.mockito.Mockito;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testng.Assert;
import org.testng.annotations.Test;

public class IncidentServiceTest {

  private static final Urn TEST_INCIDENT_URN = UrnUtils.getUrn("urn:li:incident:test");
  private static final Urn TEST_NON_EXISTENT_INCIDENT_URN =
      UrnUtils.getUrn("urn:li:incident:test-non-existant");
  private static final Urn TEST_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,name,PROD)");
  private static final Urn TEST_NON_EXISTENT_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,non-existant,PROD)");
  private static final Urn TEST_USER_URN = UrnUtils.getUrn(SYSTEM_ACTOR);

  @Test
  private void testGetIncidentInfo() throws Exception {
    final SystemEntityClient mockClient = createMockEntityClient();
    final IncidentService service = new IncidentService(mockClient);

    // Case 1: Info exists
    IncidentInfo info = service.getIncidentInfo(mockOperationContext(), TEST_INCIDENT_URN);
    Assert.assertEquals(info, mockIncidentInfo());
    Mockito.verify(mockClient, Mockito.times(1))
        .getV2(
            any(OperationContext.class),
            Mockito.eq(Constants.INCIDENT_ENTITY_NAME),
            Mockito.eq(TEST_INCIDENT_URN),
            Mockito.eq(ImmutableSet.of(Constants.INCIDENT_INFO_ASPECT_NAME)));

    // Case 2: Info does not exist
    info = service.getIncidentInfo(mockOperationContext(), TEST_NON_EXISTENT_INCIDENT_URN);
    Assert.assertNull(info);
    Mockito.verify(mockClient, Mockito.times(1))
        .getV2(
            any(OperationContext.class),
            Mockito.eq(Constants.INCIDENT_ENTITY_NAME),
            Mockito.eq(TEST_NON_EXISTENT_INCIDENT_URN),
            Mockito.eq(ImmutableSet.of(Constants.INCIDENT_INFO_ASPECT_NAME)));
  }

  @Test
  private void testGetIncidentsSummary() throws Exception {
    final SystemEntityClient mockClient = createMockEntityClient();
    final IncidentService service = new IncidentService(mockClient);

    // Case 1: Summary exists
    IncidentsSummary summary =
        service.getIncidentsSummary(mockOperationContext(), TEST_DATASET_URN);
    Assert.assertEquals(summary, mockIncidentSummary());
    Mockito.verify(mockClient, Mockito.times(1))
        .getV2(
            any(OperationContext.class),
            Mockito.eq(DATASET_ENTITY_NAME),
            Mockito.eq(TEST_DATASET_URN),
            Mockito.eq(ImmutableSet.of(INCIDENTS_SUMMARY_ASPECT_NAME)));

    // Case 2: Summary does not exist
    summary = service.getIncidentsSummary(mockOperationContext(), TEST_NON_EXISTENT_DATASET_URN);
    Assert.assertNull(summary);
    Mockito.verify(mockClient, Mockito.times(1))
        .getV2(
            any(OperationContext.class),
            Mockito.eq(Constants.DATASET_ENTITY_NAME),
            Mockito.eq(TEST_DATASET_URN),
            Mockito.eq(ImmutableSet.of(Constants.INCIDENTS_SUMMARY_ASPECT_NAME)));
  }

  @Test
  private void testUpdateIncidentsSummary() throws Exception {
    final SystemEntityClient mockClient = createMockEntityClient();
    final IncidentService service = new IncidentService(mockClient);
    service.updateIncidentsSummary(mockOperationContext(), TEST_DATASET_URN, mockIncidentSummary());
    Mockito.verify(mockClient, Mockito.times(1))
        .ingestProposal(
            any(OperationContext.class), Mockito.eq(mockIncidentSummaryMcp()), Mockito.eq(false));
  }

  @Test
  private void testRaiseIncidentRequiredFields() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);
    final IncidentService service = new IncidentService(mockClient);
    service.raiseIncident(
        mockOperationContext(),
        IncidentType.OPERATIONAL,
        null,
        null,
        null,
        null,
        ImmutableList.of(TEST_DATASET_URN),
        null,
        UrnUtils.getUrn(SYSTEM_ACTOR),
        null);

    final IncidentInfo expectedInfo =
        new IncidentInfo()
            .setType(IncidentType.OPERATIONAL)
            .setEntities(new UrnArray(ImmutableList.of(TEST_DATASET_URN)))
            .setStatus(
                new IncidentStatus()
                    .setState(IncidentState.ACTIVE)
                    .setLastUpdated(new AuditStamp().setTime(0L).setActor(TEST_USER_URN)))
            .setCreated(new AuditStamp().setTime(0L).setActor(TEST_USER_URN));

    Mockito.verify(mockClient, Mockito.times(1))
        .ingestProposal(
            any(OperationContext.class),
            Mockito.argThat(
                new IncidentInfoArgumentMatcher(
                    AspectUtils.buildMetadataChangeProposal(
                        TEST_INCIDENT_URN, INCIDENT_INFO_ASPECT_NAME, expectedInfo))),
            Mockito.eq(false));
  }

  @Test
  private void testRaiseIncidentAllFields() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);
    final IncidentService service = new IncidentService(mockClient);
    service.raiseIncident(
        mockOperationContext(),
        IncidentType.OPERATIONAL,
        "custom type",
        2,
        "title",
        "description",
        ImmutableList.of(TEST_DATASET_URN),
        new IncidentSource().setType(IncidentSourceType.MANUAL),
        UrnUtils.getUrn(SYSTEM_ACTOR),
        "message");

    final IncidentInfo expectedInfo =
        new IncidentInfo()
            .setType(IncidentType.OPERATIONAL)
            .setCustomType("custom type")
            .setPriority(2)
            .setTitle("title")
            .setDescription("description")
            .setEntities(new UrnArray(ImmutableList.of(TEST_DATASET_URN)))
            .setStatus(
                new IncidentStatus()
                    .setState(IncidentState.ACTIVE)
                    .setLastUpdated(new AuditStamp().setTime(0L).setActor(TEST_USER_URN))
                    .setMessage("message"))
            .setSource(new IncidentSource().setType(IncidentSourceType.MANUAL))
            .setCreated(new AuditStamp().setTime(0L).setActor(TEST_USER_URN));

    Mockito.verify(mockClient, Mockito.times(1))
        .ingestProposal(
            any(OperationContext.class),
            Mockito.argThat(
                new IncidentInfoArgumentMatcher(
                    AspectUtils.buildMetadataChangeProposal(
                        TEST_INCIDENT_URN, INCIDENT_INFO_ASPECT_NAME, expectedInfo))),
            Mockito.eq(false));
  }

  @Test
  private void testUpdateIncidentStatus() throws Exception {
    final SystemEntityClient mockClient = createMockEntityClient();
    final IncidentService service = new IncidentService(mockClient);
    service.updateIncidentStatus(
        mockOperationContext(),
        TEST_INCIDENT_URN,
        IncidentState.RESOLVED,
        TEST_USER_URN,
        "message");

    IncidentInfo expectedInfo = new IncidentInfo(mockIncidentInfo().data());
    expectedInfo.setStatus(
        new IncidentStatus()
            .setState(IncidentState.RESOLVED)
            .setMessage("message")
            .setLastUpdated(new AuditStamp().setActor(TEST_USER_URN).setTime(0L)));

    Mockito.verify(mockClient, Mockito.times(1))
        .ingestProposal(
            any(OperationContext.class),
            Mockito.argThat(
                new IncidentInfoArgumentMatcher(
                    AspectUtils.buildMetadataChangeProposal(
                        TEST_INCIDENT_URN, INCIDENT_INFO_ASPECT_NAME, expectedInfo))),
            Mockito.eq(false));
  }

  @Test
  private void testDeleteIncident() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);
    final IncidentService service = new IncidentService(mockClient);
    service.deleteIncident(mockOperationContext(), TEST_INCIDENT_URN);
    Mockito.verify(mockClient, Mockito.times(1))
        .deleteEntity(any(OperationContext.class), Mockito.eq(TEST_INCIDENT_URN));
    Mockito.verify(mockClient, Mockito.times(1))
        .deleteEntityReferences(any(OperationContext.class), Mockito.eq(TEST_INCIDENT_URN));
  }

  private static SystemEntityClient createMockEntityClient() throws Exception {
    SystemEntityClient mockClient = mock(SystemEntityClient.class);

    // Init for incident info
    when(mockClient.getV2(
            any(OperationContext.class),
            Mockito.eq(Constants.INCIDENT_ENTITY_NAME),
            Mockito.eq(TEST_INCIDENT_URN),
            Mockito.eq(ImmutableSet.of(Constants.INCIDENT_INFO_ASPECT_NAME))))
        .thenReturn(
            new EntityResponse()
                .setUrn(TEST_INCIDENT_URN)
                .setEntityName(INCIDENT_ENTITY_NAME)
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            INCIDENT_INFO_ASPECT_NAME,
                            new EnvelopedAspect()
                                .setValue(new Aspect(mockIncidentInfo().data()))))));
    when(mockClient.getV2(
            any(OperationContext.class),
            Mockito.eq(Constants.INCIDENT_ENTITY_NAME),
            Mockito.eq(TEST_NON_EXISTENT_INCIDENT_URN),
            Mockito.eq(ImmutableSet.of(Constants.INCIDENT_INFO_ASPECT_NAME))))
        .thenReturn(
            new EntityResponse()
                .setUrn(TEST_NON_EXISTENT_INCIDENT_URN)
                .setEntityName(INCIDENT_ENTITY_NAME)
                .setAspects(new EnvelopedAspectMap(Collections.emptyMap())));

    // Init for incidents summary
    when(mockClient.getV2(
            any(OperationContext.class),
            Mockito.eq(DATASET_ENTITY_NAME),
            Mockito.eq(TEST_DATASET_URN),
            Mockito.eq(ImmutableSet.of(INCIDENTS_SUMMARY_ASPECT_NAME))))
        .thenReturn(
            new EntityResponse()
                .setUrn(TEST_DATASET_URN)
                .setEntityName(DATASET_ENTITY_NAME)
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            INCIDENTS_SUMMARY_ASPECT_NAME,
                            new EnvelopedAspect()
                                .setValue(new Aspect(mockIncidentSummary().data()))))));
    when(mockClient.getV2(
            any(OperationContext.class),
            Mockito.eq(DATASET_ENTITY_NAME),
            Mockito.eq(TEST_NON_EXISTENT_DATASET_URN),
            Mockito.eq(ImmutableSet.of(INCIDENTS_SUMMARY_ASPECT_NAME))))
        .thenReturn(
            new EntityResponse()
                .setUrn(TEST_NON_EXISTENT_DATASET_URN)
                .setEntityName(DATASET_ENTITY_NAME)
                .setAspects(new EnvelopedAspectMap(Collections.emptyMap())));

    // Init for update summary
    when(mockClient.ingestProposal(
            any(OperationContext.class), Mockito.eq(mockIncidentSummaryMcp()), Mockito.eq(false)))
        .thenReturn(TEST_DATASET_URN.toString());

    return mockClient;
  }

  private static IncidentInfo mockIncidentInfo() throws Exception {
    return new IncidentInfo()
        .setType(IncidentType.OPERATIONAL)
        .setEntities(new UrnArray(ImmutableList.of(TEST_DATASET_URN)))
        .setStatus(
            new IncidentStatus()
                .setState(IncidentState.ACTIVE)
                .setLastUpdated(new AuditStamp().setTime(0L).setActor(TEST_USER_URN)))
        .setCreated(new AuditStamp().setTime(0L).setActor(TEST_USER_URN));
  }

  private static IncidentsSummary mockIncidentSummary() throws Exception {
    final IncidentsSummary summary = new IncidentsSummary();
    summary.setResolvedIncidents(new UrnArray(ImmutableList.of(TEST_INCIDENT_URN)));
    return summary;
  }

  private static MetadataChangeProposal mockIncidentSummaryMcp() throws Exception {

    final MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(TEST_DATASET_URN);
    mcp.setEntityType(DATASET_ENTITY_NAME);
    mcp.setAspectName(INCIDENTS_SUMMARY_ASPECT_NAME);
    mcp.setChangeType(ChangeType.UPSERT);
    mcp.setAspect(GenericRecordUtils.serializeAspect(mockIncidentSummary()));

    return mcp;
  }

  private static OperationContext mockOperationContext() {
    OperationContext operationContext = mock(OperationContext.class);
    when(operationContext.getSessionAuthentication()).thenReturn(mock(Authentication.class));
    return operationContext;
  }
}
