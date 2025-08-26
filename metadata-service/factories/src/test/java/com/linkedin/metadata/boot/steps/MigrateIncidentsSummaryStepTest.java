package com.linkedin.metadata.boot.steps;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.IncidentSummaryDetails;
import com.linkedin.common.IncidentSummaryDetailsArray;
import com.linkedin.common.IncidentsSummary;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.incident.IncidentInfo;
import com.linkedin.incident.IncidentSource;
import com.linkedin.incident.IncidentSourceType;
import com.linkedin.incident.IncidentState;
import com.linkedin.incident.IncidentStatus;
import com.linkedin.incident.IncidentType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.service.IncidentService;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class MigrateIncidentsSummaryStepTest {

  private static final Urn INCIDENT_URN =
      UrnUtils.getUrn("urn:li:incident:126d8dc8939e0cf9bf0fd03264ad1a06");
  private static final String DATASET_URN =
      "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)";
  private static final String SCROLL_ID = "test123";

  @Test
  public void testExecuteIncidentsSummaryStepWithActiveIncident() throws Exception {
    final EntityService entityService = mock(EntityService.class);
    final EntitySearchService entitySearchService = mock(EntitySearchService.class);
    final IncidentService incidentService = mock(IncidentService.class);
    configureEntitySearchServiceMock(entitySearchService);
    configureIncidentServiceMock(incidentService, IncidentState.ACTIVE);

    final MigrateIncidentsSummaryStep step =
        new MigrateIncidentsSummaryStep(entityService, entitySearchService, incidentService);

    step.upgrade(mock(OperationContext.class));

    IncidentsSummary expectedSummary = new IncidentsSummary();
    IncidentSummaryDetailsArray activeIncidentDetails = new IncidentSummaryDetailsArray();
    activeIncidentDetails.add(buildIncidentSummaryDetails(INCIDENT_URN, IncidentState.ACTIVE));
    expectedSummary.setActiveIncidentDetails(activeIncidentDetails);
    expectedSummary.setResolvedIncidents(new UrnArray());
    expectedSummary.setActiveIncidents(new UrnArray());

    Mockito.verify(incidentService, times(1))
        .updateIncidentsSummary(
            any(OperationContext.class),
            Mockito.eq(UrnUtils.getUrn(DATASET_URN)),
            Mockito.eq(expectedSummary));
  }

  @Test
  public void testExecuteIncidentsSummaryStepWithResolvedIncident() throws Exception {
    final EntityService entityService = mock(EntityService.class);
    final EntitySearchService entitySearchService = mock(EntitySearchService.class);
    final IncidentService incidentService = mock(IncidentService.class);
    configureEntitySearchServiceMock(entitySearchService);
    configureIncidentServiceMock(incidentService, IncidentState.RESOLVED);

    final MigrateIncidentsSummaryStep step =
        new MigrateIncidentsSummaryStep(entityService, entitySearchService, incidentService);

    step.upgrade(mock(OperationContext.class));

    IncidentsSummary expectedSummary = new IncidentsSummary();
    IncidentSummaryDetailsArray resolvedIncidentDetails = new IncidentSummaryDetailsArray();
    resolvedIncidentDetails.add(buildIncidentSummaryDetails(INCIDENT_URN, IncidentState.RESOLVED));
    expectedSummary.setResolvedIncidentDetails(resolvedIncidentDetails);
    expectedSummary.setResolvedIncidents(new UrnArray());
    expectedSummary.setActiveIncidents(new UrnArray());

    Mockito.verify(incidentService, times(1))
        .updateIncidentsSummary(
            any(OperationContext.class),
            Mockito.eq(UrnUtils.getUrn(DATASET_URN)),
            Mockito.eq(expectedSummary));
  }

  private static void configureEntitySearchServiceMock(
      final EntitySearchService mockSearchService) {
    SearchEntity searchEntity = new SearchEntity();
    searchEntity.setEntity(INCIDENT_URN);
    SearchEntityArray searchEntityArray = new SearchEntityArray();
    searchEntityArray.add(searchEntity);
    ScrollResult scrollResult = new ScrollResult();
    scrollResult.setEntities(searchEntityArray);
    scrollResult.setScrollId(SCROLL_ID);

    Mockito.when(
            mockSearchService.scroll(
                any(),
                Mockito.eq(Collections.singletonList(Constants.INCIDENT_ENTITY_NAME)),
                Mockito.eq(null),
                Mockito.eq(null),
                Mockito.eq(1000),
                Mockito.eq(null),
                Mockito.eq("5m"),
                Mockito.eq(null)))
        .thenReturn(scrollResult);

    ScrollResult newScrollResult = new ScrollResult();
    newScrollResult.setEntities(new SearchEntityArray());

    Mockito.when(
            mockSearchService.scroll(
                any(),
                Mockito.eq(Collections.singletonList(Constants.INCIDENT_ENTITY_NAME)),
                Mockito.eq(null),
                Mockito.eq(null),
                Mockito.eq(1000),
                Mockito.eq(SCROLL_ID),
                Mockito.eq("5m"),
                Mockito.eq(null)))
        .thenReturn(newScrollResult);
  }

  private static IncidentSummaryDetails buildIncidentSummaryDetails(
      final Urn incidentUrn, final IncidentState state) {
    IncidentSummaryDetails summaryDetails = new IncidentSummaryDetails();
    summaryDetails.setUrn(incidentUrn);
    summaryDetails.setCreatedAt(0L);
    summaryDetails.setPriority(1);
    summaryDetails.setType(IncidentType.DATASET_COLUMN.toString());
    if (IncidentState.RESOLVED.equals(state)) {
      summaryDetails.setResolvedAt(1L);
    }
    return summaryDetails;
  }

  private static void configureIncidentServiceMock(
      final IncidentService mockIncidentService, final IncidentState state) {
    IncidentInfo incidentInfo = new IncidentInfo();
    incidentInfo.setEntities(new UrnArray(ImmutableList.of(UrnUtils.getUrn(DATASET_URN))));
    incidentInfo.setSource(new IncidentSource().setType(IncidentSourceType.MANUAL));
    incidentInfo.setStatus(
        new IncidentStatus().setState(state).setLastUpdated(new AuditStamp().setTime(1L)));
    incidentInfo.setType(IncidentType.DATASET_COLUMN);
    incidentInfo.setCreated(new AuditStamp().setTime(0L));
    incidentInfo.setPriority(1);

    Mockito.when(
            mockIncidentService.getIncidentInfo(
                any(OperationContext.class), Mockito.eq(INCIDENT_URN)))
        .thenReturn(incidentInfo);

    Mockito.when(
            mockIncidentService.getIncidentsSummary(
                any(OperationContext.class), Mockito.eq(UrnUtils.getUrn(DATASET_URN))))
        .thenReturn(
            new IncidentsSummary()
                .setActiveIncidents(new UrnArray(ImmutableList.of(INCIDENT_URN)))
                .setResolvedIncidents(new UrnArray(ImmutableList.of(INCIDENT_URN))));
  }
}
