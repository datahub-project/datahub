package com.linkedin.metadata.kafka.hook.incident;

import static com.linkedin.metadata.Constants.INCIDENT_ACTIVITY_EVENT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.INCIDENT_ENTITY_NAME;
import static com.linkedin.metadata.Constants.INCIDENT_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.INCIDENT_KEY_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.incident.IncidentActivityChange;
import com.linkedin.incident.IncidentActivityChangeArray;
import com.linkedin.incident.IncidentActivityChangeType;
import com.linkedin.incident.IncidentActivityEvent;
import com.linkedin.incident.IncidentAssignee;
import com.linkedin.incident.IncidentAssigneeArray;
import com.linkedin.incident.IncidentInfo;
import com.linkedin.incident.IncidentSource;
import com.linkedin.incident.IncidentSourceType;
import com.linkedin.incident.IncidentStage;
import com.linkedin.incident.IncidentState;
import com.linkedin.incident.IncidentStatus;
import com.linkedin.incident.IncidentType;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import org.mockito.Mockito;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class IncidentActivityEventHookTest {
  private static final Urn TEST_INCIDENT_URN = UrnUtils.getUrn("urn:li:incident:test");
  private static final Urn TEST_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,name,PROD)");
  private static final Urn TEST_DATASET_2_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,name2,PROD)");
  private OperationContext opContext;

  @BeforeTest
  public void setup() {
    opContext = TestOperationContexts.systemContextNoSearchAuthorization();
  }

  @Test
  public void testInvokeNotEnabled() throws Exception {
    final IncidentInfo incidentInfo =
        mockIncidentInfo(ImmutableList.of(TEST_DATASET_URN), IncidentState.ACTIVE);

    final SystemEntityClient systemEntityClient = mock(SystemEntityClient.class);

    final IncidentActivityEventHook hook =
        new IncidentActivityEventHook(systemEntityClient, false).init(opContext);

    final MetadataChangeLog event =
        buildMetadataChangeLog(
            TEST_INCIDENT_URN,
            INCIDENT_INFO_ASPECT_NAME,
            ChangeType.UPSERT,
            incidentInfo,
            incidentInfo);

    hook.invoke(event);
    Mockito.verify(systemEntityClient, Mockito.times(0))
        .ingestProposal(any(OperationContext.class), any());
  }

  @Test
  public void testInvokeNotEligibleChange() throws Exception {
    final IncidentInfo info =
        mockIncidentInfo(ImmutableList.of(TEST_DATASET_URN), IncidentState.ACTIVE);

    final SystemEntityClient systemEntityClient = mock(SystemEntityClient.class);

    final IncidentActivityEventHook hook =
        new IncidentActivityEventHook(systemEntityClient, false).init(opContext);

    // Case 1: Incorrect aspect
    MetadataChangeLog event =
        buildMetadataChangeLog(
            TEST_INCIDENT_URN,
            INCIDENT_KEY_ASPECT_NAME,
            ChangeType.UPSERT,
            new IncidentInfo(),
            null);
    hook.invoke(event);

    Mockito.verify(systemEntityClient, Mockito.times(0))
        .ingestProposal(any(OperationContext.class), any());

    // Case 2: Info Event But Delete
    event =
        buildMetadataChangeLog(
            TEST_INCIDENT_URN, INCIDENT_INFO_ASPECT_NAME, ChangeType.DELETE, info, null);
    hook.invoke(event);

    Mockito.verify(systemEntityClient, Mockito.times(0))
        .ingestProposal(any(OperationContext.class), any());

    // Case 3: Info Event but no previous
    event =
        buildMetadataChangeLog(
            TEST_INCIDENT_URN, INCIDENT_INFO_ASPECT_NAME, ChangeType.UPSERT, info, null);
    hook.invoke(event);

    Mockito.verify(systemEntityClient, Mockito.times(0))
        .ingestProposal(any(OperationContext.class), any());

    // Case 4: Info Event but no new
    event =
        buildMetadataChangeLog(
            TEST_INCIDENT_URN, INCIDENT_INFO_ASPECT_NAME, ChangeType.UPSERT, null, info);
    hook.invoke(event);

    Mockito.verify(systemEntityClient, Mockito.times(0))
        .ingestProposal(any(OperationContext.class), any());
  }

  @Test
  public void testInvokeAllChanges() throws Exception {
    final IncidentInfo prevInfo = new IncidentInfo();
    prevInfo.setType(IncidentType.CUSTOM); // Not supported yet
    prevInfo.setCustomType("Custom Type"); // Not supported yet
    prevInfo.setDescription("New description"); // Not supported yet
    prevInfo.setEntities(new UrnArray(ImmutableList.of(TEST_DATASET_2_URN))); // Not supported yet
    prevInfo.setPriority(2);
    prevInfo.setStatus(
        new IncidentStatus()
            .setState(IncidentState.RESOLVED)
            .setMessage("New message") // Not supported yet
            .setStage(IncidentStage.INVESTIGATION)
            .setLastUpdated(
                new AuditStamp()
                    .setActor(UrnUtils.getUrn("urn:li:corpuser:test-2"))
                    .setTime(0L))); // Not supported yet
    prevInfo.setSource(
        new IncidentSource().setType(IncidentSourceType.ASSERTION_FAILURE)); // Not supported yet
    prevInfo.setAssignees(
        new IncidentAssigneeArray(
            ImmutableList.of(
                new IncidentAssignee()
                    .setActor(UrnUtils.getUrn("urn:li:corpuser:test-2"))
                    .setAssignedAt(new AuditStamp().setTime(1L)),
                new IncidentAssignee()
                    .setActor(UrnUtils.getUrn("urn:li:corpuser:test-3"))
                    .setAssignedAt(new AuditStamp().setTime(2L)))));

    final IncidentInfo newInfo = new IncidentInfo();
    newInfo.setType(IncidentType.OPERATIONAL);
    newInfo.setDescription("description");
    newInfo.setEntities(new UrnArray(ImmutableList.of(TEST_DATASET_URN)));
    newInfo.setPriority(1);
    newInfo.setStatus(
        new IncidentStatus()
            .setState(IncidentState.ACTIVE)
            .setMessage("Test")
            .setStage(IncidentStage.FIXED)
            .setLastUpdated(
                new AuditStamp().setActor(UrnUtils.getUrn("urn:li:corpuser:test")).setTime(1L)));
    newInfo.setSource(new IncidentSource().setType(IncidentSourceType.MANUAL));
    newInfo.setAssignees(
        new IncidentAssigneeArray(
            ImmutableList.of(
                new IncidentAssignee()
                    .setActor(UrnUtils.getUrn("urn:li:corpuser:test"))
                    .setAssignedAt(new AuditStamp().setTime(0L)))));

    final SystemEntityClient systemEntityClient = mock(SystemEntityClient.class);

    final IncidentActivityEventHook hook =
        new IncidentActivityEventHook(systemEntityClient, true).init(opContext);

    hook.invoke(
        buildMetadataChangeLog(
            TEST_INCIDENT_URN, INCIDENT_INFO_ASPECT_NAME, ChangeType.UPSERT, newInfo, prevInfo));

    final IncidentActivityEvent expectedEvent = new IncidentActivityEvent();
    expectedEvent.setActorUrn(UrnUtils.getUrn("urn:li:corpuser:test"));
    expectedEvent.setNewInfo(newInfo);
    expectedEvent.setPreviousInfo(prevInfo);
    expectedEvent.setTimestampMillis(1L);
    expectedEvent.setChanges(
        new IncidentActivityChangeArray(
            ImmutableList.of(
                new IncidentActivityChange().setType(IncidentActivityChangeType.STATE),
                new IncidentActivityChange().setType(IncidentActivityChangeType.STAGE),
                new IncidentActivityChange().setType(IncidentActivityChangeType.ASSIGNEES),
                new IncidentActivityChange().setType(IncidentActivityChangeType.PRIORITY))));

    Mockito.verify(systemEntityClient, Mockito.times(1))
        .ingestProposal(
            eq(opContext),
            Mockito.argThat(
                proposal ->
                    proposal.getEntityUrn().equals(TEST_INCIDENT_URN)
                        && proposal.getAspectName().equals(INCIDENT_ACTIVITY_EVENT_ASPECT_NAME)
                        && GenericRecordUtils.deserializeAspect(
                                proposal.getAspect().getValue(),
                                "application/json",
                                IncidentActivityEvent.class)
                            .getChanges()
                            .equals(expectedEvent.getChanges())));
  }

  @Test
  public void testInvokeNoChanges() throws Exception {
    final IncidentInfo newInfo = new IncidentInfo();
    newInfo.setType(IncidentType.OPERATIONAL);
    newInfo.setDescription("description");
    newInfo.setEntities(new UrnArray(ImmutableList.of(TEST_DATASET_URN)));
    newInfo.setPriority(1);
    newInfo.setStatus(
        new IncidentStatus()
            .setState(IncidentState.ACTIVE)
            .setMessage("Test")
            .setStage(IncidentStage.FIXED)
            .setLastUpdated(
                new AuditStamp().setActor(UrnUtils.getUrn("urn:li:corpuser:test")).setTime(1L)));
    newInfo.setSource(new IncidentSource().setType(IncidentSourceType.MANUAL));
    newInfo.setAssignees(
        new IncidentAssigneeArray(
            ImmutableList.of(
                new IncidentAssignee()
                    .setActor(UrnUtils.getUrn("urn:li:corpuser:test"))
                    .setAssignedAt(new AuditStamp().setTime(0L)))));

    final SystemEntityClient systemEntityClient = mock(SystemEntityClient.class);

    final IncidentActivityEventHook hook =
        new IncidentActivityEventHook(systemEntityClient, true).init(opContext);

    hook.invoke(
        buildMetadataChangeLog(
            TEST_INCIDENT_URN, INCIDENT_INFO_ASPECT_NAME, ChangeType.UPSERT, newInfo, newInfo));

    // No changes, no event.
    Mockito.verify(systemEntityClient, Mockito.times(0))
        .ingestProposal(eq(opContext), Mockito.any(MetadataChangeProposal.class));
  }

  private IncidentInfo mockIncidentInfo(final List<Urn> entityUrns, final IncidentState state) {
    IncidentInfo event = new IncidentInfo();
    event.setEntities(new UrnArray(entityUrns));
    event.setType(IncidentType.OPERATIONAL);
    event.setSource(new IncidentSource().setType(IncidentSourceType.MANUAL));
    event.setStatus(
        new IncidentStatus()
            .setState(state)
            .setLastUpdated(
                new AuditStamp().setTime(1L).setActor(UrnUtils.getUrn("urn:li:corpuser:test"))));
    event.setCreated(
        new AuditStamp().setTime(0L).setActor(UrnUtils.getUrn("urn:li:corpuser:test")));
    return event;
  }

  private MetadataChangeLog buildMetadataChangeLog(
      Urn urn,
      String aspectName,
      ChangeType changeType,
      RecordTemplate aspect,
      RecordTemplate prevAspect)
      throws Exception {
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityUrn(urn);
    event.setEntityType(INCIDENT_ENTITY_NAME);
    event.setAspectName(aspectName);
    event.setChangeType(changeType);
    if (aspect != null) {
      event.setAspect(GenericRecordUtils.serializeAspect(aspect));
    }
    if (prevAspect != null) {
      event.setPreviousAspectValue(GenericRecordUtils.serializeAspect(prevAspect));
    }
    return event;
  }
}
