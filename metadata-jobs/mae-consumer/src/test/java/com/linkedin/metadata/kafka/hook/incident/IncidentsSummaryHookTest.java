package com.linkedin.metadata.kafka.hook.incident;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.kafka.hook.EntityRegistryTestUtil.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.IncidentSummaryDetails;
import com.linkedin.common.IncidentSummaryDetailsArray;
import com.linkedin.common.IncidentsSummary;
import com.linkedin.common.Status;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.incident.IncidentInfo;
import com.linkedin.incident.IncidentSource;
import com.linkedin.incident.IncidentSourceType;
import com.linkedin.incident.IncidentState;
import com.linkedin.incident.IncidentStatus;
import com.linkedin.incident.IncidentType;
import com.linkedin.metadata.service.IncidentService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.stream.Collectors;
import org.mockito.Mockito;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class IncidentsSummaryHookTest {
  private static final Urn TEST_EXISTING_INCIDENT_URN = UrnUtils.getUrn("urn:li:incident:existing");
  private static final Urn TEST_INCIDENT_URN = UrnUtils.getUrn("urn:li:incident:test");
  private static final Urn TEST_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,name,PROD)");
  private static final Urn TEST_DATASET_2_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,name2,PROD)");
  private static final String TEST_INCIDENT_TYPE = "TestType";
  private OperationContext opContext;

  @BeforeTest
  public void setup() {
    opContext = TestOperationContexts.systemContextNoSearchAuthorization();
  }

  @Test
  public void testInvokeNotEnabled() throws Exception {
    IncidentInfo incidentInfo =
        mockIncidentInfo(
            ImmutableList.of(TEST_DATASET_URN, TEST_DATASET_2_URN), IncidentState.ACTIVE);
    IncidentService service = mockIncidentService(new IncidentsSummary(), incidentInfo);
    IncidentsSummaryHook hook = new IncidentsSummaryHook(service, false, 100).init(opContext);
    final MetadataChangeLog event =
        buildMetadataChangeLog(
            TEST_INCIDENT_URN, INCIDENT_INFO_ASPECT_NAME, ChangeType.UPSERT, incidentInfo);
    hook.invoke(event);
    Mockito.verify(service, Mockito.times(0)).getIncidentInfo(any(OperationContext.class), any());
  }

  @Test
  public void testInvokeNotEligibleChange() throws Exception {
    IncidentInfo info =
        mockIncidentInfo(
            ImmutableList.of(TEST_DATASET_URN, TEST_DATASET_2_URN), IncidentState.ACTIVE);
    IncidentService service = mockIncidentService(new IncidentsSummary(), info);
    IncidentsSummaryHook hook = new IncidentsSummaryHook(service, true, 100).init(opContext);

    // Case 1: Incorrect aspect
    MetadataChangeLog event =
        buildMetadataChangeLog(
            TEST_INCIDENT_URN, INCIDENT_KEY_ASPECT_NAME, ChangeType.UPSERT, new IncidentInfo());
    hook.invoke(event);
    Mockito.verify(service, Mockito.times(0)).getIncidentInfo(any(OperationContext.class), any());

    // Case 2: Run Event But Delete
    event =
        buildMetadataChangeLog(
            TEST_INCIDENT_URN, INCIDENT_INFO_ASPECT_NAME, ChangeType.DELETE, info);
    hook.invoke(event);
    Mockito.verify(service, Mockito.times(0)).getIncidentInfo(any(OperationContext.class), any());
  }

  @DataProvider(name = "incidentsSummaryBaseProvider")
  static Object[][] incidentsSummaryBaseProvider() {
    return new Object[][] {
      new Object[] {null},
      new Object[] {
        new IncidentsSummary()
            .setActiveIncidentDetails(new IncidentSummaryDetailsArray())
            .setResolvedIncidentDetails(new IncidentSummaryDetailsArray())
      },
      new Object[] {
        new IncidentsSummary()
            .setActiveIncidentDetails(
                new IncidentSummaryDetailsArray(
                    ImmutableList.of(
                        new IncidentSummaryDetails()
                            .setUrn(TEST_EXISTING_INCIDENT_URN)
                            .setType(TEST_INCIDENT_TYPE)
                            .setCreatedAt(0L))))
            .setResolvedIncidentDetails(new IncidentSummaryDetailsArray())
      },
      new Object[] {
        new IncidentsSummary()
            .setActiveIncidentDetails(new IncidentSummaryDetailsArray())
            .setResolvedIncidentDetails(
                new IncidentSummaryDetailsArray(
                    ImmutableList.of(
                        new IncidentSummaryDetails()
                            .setUrn(TEST_EXISTING_INCIDENT_URN)
                            .setType(TEST_INCIDENT_TYPE)
                            .setCreatedAt(0L))))
      },
      new Object[] {
        new IncidentsSummary()
            .setActiveIncidentDetails(
                new IncidentSummaryDetailsArray(
                    ImmutableList.of(
                        new IncidentSummaryDetails()
                            .setUrn(TEST_INCIDENT_URN)
                            .setType(TEST_INCIDENT_TYPE)
                            .setCreatedAt(0L))))
            .setResolvedIncidentDetails(
                new IncidentSummaryDetailsArray(
                    ImmutableList.of(
                        new IncidentSummaryDetails()
                            .setUrn(TEST_INCIDENT_URN)
                            .setType(TEST_INCIDENT_TYPE)
                            .setCreatedAt(0L))))
      }
    };
  }

  @Test(dataProvider = "incidentsSummaryBaseProvider")
  public void testInvokeIncidentRunEventActive(IncidentsSummary summary) throws Exception {
    IncidentInfo info =
        mockIncidentInfo(
            ImmutableList.of(TEST_DATASET_URN, TEST_DATASET_2_URN), IncidentState.ACTIVE);
    IncidentService service = mockIncidentService(summary, info);
    IncidentsSummaryHook hook = new IncidentsSummaryHook(service, true, 100).init(opContext);
    final MetadataChangeLog event =
        buildMetadataChangeLog(
            TEST_INCIDENT_URN, INCIDENT_INFO_ASPECT_NAME, ChangeType.UPSERT, info);
    hook.invoke(event);
    Mockito.verify(service, Mockito.times(1))
        .getIncidentInfo(any(OperationContext.class), eq(TEST_INCIDENT_URN));
    Mockito.verify(service, Mockito.times(1))
        .getIncidentsSummary(any(OperationContext.class), eq(TEST_DATASET_URN));
    Mockito.verify(service, Mockito.times(1))
        .getIncidentsSummary(any(OperationContext.class), eq(TEST_DATASET_2_URN));

    if (summary == null) {
      summary = new IncidentsSummary();
    }
    IncidentsSummary expectedSummary = new IncidentsSummary(summary.data());
    expectedSummary.setActiveIncidentDetails(
        new IncidentSummaryDetailsArray(
            expectedSummary.getActiveIncidentDetails().stream()
                .filter(details -> !details.getUrn().equals(TEST_INCIDENT_URN))
                .collect(Collectors.toList())));
    expectedSummary.setResolvedIncidentDetails(
        new IncidentSummaryDetailsArray(
            expectedSummary.getResolvedIncidentDetails().stream()
                .filter(details -> !details.getUrn().equals(TEST_INCIDENT_URN))
                .collect(Collectors.toList())));
    expectedSummary
        .getActiveIncidentDetails()
        .add(buildIncidentSummaryDetails(TEST_INCIDENT_URN, info));

    // Ensure we ingested a new aspect.
    Mockito.verify(service, Mockito.times(1))
        .updateIncidentsSummary(
            any(OperationContext.class), eq(TEST_DATASET_URN), eq(expectedSummary));
    Mockito.verify(service, Mockito.times(1))
        .updateIncidentsSummary(
            any(OperationContext.class), eq(TEST_DATASET_2_URN), eq(expectedSummary));
  }

  @Test(dataProvider = "incidentsSummaryBaseProvider")
  public void testInvokeIncidentRunEventResolved(IncidentsSummary summary) throws Exception {
    IncidentInfo info =
        mockIncidentInfo(
            ImmutableList.of(TEST_DATASET_URN, TEST_DATASET_2_URN), IncidentState.RESOLVED);
    IncidentService service = mockIncidentService(summary, info);
    IncidentsSummaryHook hook = new IncidentsSummaryHook(service, true, 100).init(opContext);
    final MetadataChangeLog event =
        buildMetadataChangeLog(
            TEST_INCIDENT_URN, INCIDENT_INFO_ASPECT_NAME, ChangeType.UPSERT, info);
    hook.invoke(event);
    Mockito.verify(service, Mockito.times(1))
        .getIncidentInfo(any(OperationContext.class), eq(TEST_INCIDENT_URN));
    Mockito.verify(service, Mockito.times(1))
        .getIncidentsSummary(any(OperationContext.class), eq(TEST_DATASET_URN));
    Mockito.verify(service, Mockito.times(1))
        .getIncidentsSummary(any(OperationContext.class), eq(TEST_DATASET_2_URN));

    if (summary == null) {
      summary = new IncidentsSummary();
    }
    IncidentsSummary expectedSummary = new IncidentsSummary(summary.data());
    expectedSummary.setActiveIncidentDetails(
        new IncidentSummaryDetailsArray(
            expectedSummary.getActiveIncidentDetails().stream()
                .filter(details -> !details.getUrn().equals(TEST_INCIDENT_URN))
                .collect(Collectors.toList())));
    expectedSummary.setResolvedIncidentDetails(
        new IncidentSummaryDetailsArray(
            expectedSummary.getResolvedIncidentDetails().stream()
                .filter(details -> !details.getUrn().equals(TEST_INCIDENT_URN))
                .collect(Collectors.toList())));
    expectedSummary
        .getResolvedIncidentDetails()
        .add(buildIncidentSummaryDetails(TEST_INCIDENT_URN, info));

    // Ensure we ingested a new aspect.
    Mockito.verify(service, Mockito.times(1))
        .updateIncidentsSummary(
            any(OperationContext.class), eq(TEST_DATASET_URN), eq(expectedSummary));
    Mockito.verify(service, Mockito.times(1))
        .updateIncidentsSummary(
            any(OperationContext.class), eq(TEST_DATASET_2_URN), eq(expectedSummary));
  }

  @Test(dataProvider = "incidentsSummaryBaseProvider")
  public void testInvokeIncidentSoftDeleted(IncidentsSummary summary) throws Exception {
    IncidentInfo info =
        mockIncidentInfo(
            ImmutableList.of(TEST_DATASET_URN, TEST_DATASET_2_URN), IncidentState.RESOLVED);
    IncidentService service = mockIncidentService(summary, info);
    IncidentsSummaryHook hook = new IncidentsSummaryHook(service, true, 100).init(opContext);
    final MetadataChangeLog event =
        buildMetadataChangeLog(
            TEST_INCIDENT_URN, STATUS_ASPECT_NAME, ChangeType.UPSERT, mockIncidentSoftDeleted());
    hook.invoke(event);

    Mockito.verify(service, Mockito.times(1))
        .getIncidentInfo(any(OperationContext.class), eq(TEST_INCIDENT_URN));
    Mockito.verify(service, Mockito.times(1))
        .getIncidentsSummary(any(OperationContext.class), eq(TEST_DATASET_URN));
    Mockito.verify(service, Mockito.times(1))
        .getIncidentsSummary(any(OperationContext.class), eq(TEST_DATASET_2_URN));

    if (summary == null) {
      summary = new IncidentsSummary();
    }
    IncidentsSummary expectedSummary = new IncidentsSummary(summary.data());
    expectedSummary.setResolvedIncidentDetails(
        new IncidentSummaryDetailsArray(
            expectedSummary.getResolvedIncidentDetails().stream()
                .filter(details -> !details.getUrn().equals(TEST_INCIDENT_URN))
                .collect(Collectors.toList())));
    expectedSummary.setActiveIncidentDetails(
        new IncidentSummaryDetailsArray(
            expectedSummary.getActiveIncidentDetails().stream()
                .filter(details -> !details.getUrn().equals(TEST_INCIDENT_URN))
                .collect(Collectors.toList())));

    // Ensure we ingested a new aspect.
    Mockito.verify(service, Mockito.times(1))
        .updateIncidentsSummary(
            any(OperationContext.class), eq(TEST_DATASET_URN), eq(expectedSummary));
    Mockito.verify(service, Mockito.times(1))
        .updateIncidentsSummary(
            any(OperationContext.class), eq(TEST_DATASET_2_URN), eq(expectedSummary));
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

  private Status mockIncidentSoftDeleted() {
    Status status = new Status();
    status.setRemoved(true);
    return status;
  }

  private IncidentService mockIncidentService(IncidentsSummary summary, IncidentInfo info) {
    IncidentService mockService = mock(IncidentService.class);

    Mockito.when(mockService.getIncidentInfo(any(OperationContext.class), eq(TEST_INCIDENT_URN)))
        .thenReturn(info);

    Mockito.when(mockService.getIncidentsSummary(any(OperationContext.class), eq(TEST_DATASET_URN)))
        .thenReturn(summary);
    Mockito.when(
            mockService.getIncidentsSummary(any(OperationContext.class), eq(TEST_DATASET_2_URN)))
        .thenReturn(summary);

    return mockService;
  }

  private IncidentSummaryDetails buildIncidentSummaryDetails(
      final Urn incidentUrn, final IncidentInfo info) {
    IncidentSummaryDetails incidentSummaryDetails = new IncidentSummaryDetails();
    incidentSummaryDetails.setUrn(incidentUrn);
    incidentSummaryDetails.setCreatedAt(info.getCreated().getTime());
    if (IncidentType.CUSTOM.equals(info.getType())) {
      incidentSummaryDetails.setType(info.getCustomType());
    } else {
      incidentSummaryDetails.setType(info.getType().toString());
    }
    if (info.hasPriority()) {
      incidentSummaryDetails.setPriority(info.getPriority());
    }
    if (IncidentState.RESOLVED.equals(info.getStatus().getState())) {
      incidentSummaryDetails.setResolvedAt(info.getStatus().getLastUpdated().getTime());
    }
    return incidentSummaryDetails;
  }

  private MetadataChangeLog buildMetadataChangeLog(
      Urn urn, String aspectName, ChangeType changeType, RecordTemplate aspect) throws Exception {
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityUrn(urn);
    event.setEntityType(INCIDENT_ENTITY_NAME);
    event.setAspectName(aspectName);
    event.setChangeType(changeType);
    event.setAspect(GenericRecordUtils.serializeAspect(aspect));
    return event;
  }
}
