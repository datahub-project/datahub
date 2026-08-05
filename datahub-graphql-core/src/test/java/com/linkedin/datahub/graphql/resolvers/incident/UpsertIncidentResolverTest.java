package com.linkedin.datahub.graphql.resolvers.incident;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.metadata.Constants.INCIDENT_INFO_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.generated.IncidentState;
import com.linkedin.datahub.graphql.generated.IncidentStatusInput;
import com.linkedin.datahub.graphql.generated.UpsertIncidentInput;
import com.linkedin.incident.IncidentInfo;
import com.linkedin.incident.IncidentStatus;
import com.linkedin.incident.IncidentType;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.service.IncidentInfoUpsert;
import com.linkedin.metadata.service.IncidentService;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class UpsertIncidentResolverTest {

  private static final Urn TEST_INCIDENT_URN = UrnUtils.getUrn("urn:li:incident:TEST");

  @Test
  public void testUpsertPassesExplicitClearsToService() throws Exception {
    IncidentService mockIncidentService = Mockito.mock(IncidentService.class);
    EntityService mockEntityService = Mockito.mock(EntityService.class);
    IncidentInfo existingInfo = existingInfo();
    Mockito.when(
            mockEntityService.getAspect(
                any(OperationContext.class),
                Mockito.eq(TEST_INCIDENT_URN),
                Mockito.eq(INCIDENT_INFO_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(existingInfo);

    UpsertIncidentInput input = new UpsertIncidentInput();
    input.setTitle(null);
    input.setDescription(null);
    input.setStatus(
        new IncidentStatusInput(
            IncidentState.RESOLVED,
            com.linkedin.datahub.graphql.generated.IncidentStage.FIXED,
            null));
    input.setPriority(null);
    input.setResourceUrns(List.of("urn:li:dataset:(test,test,test2)"));
    input.setAssigneeUrns(List.of());

    DataFetchingEnvironment environment = Mockito.mock(DataFetchingEnvironment.class);
    var context = getMockAllowContext();
    Mockito.when(environment.getContext()).thenReturn(context);
    Mockito.when(environment.getArgument(Mockito.eq("urn")))
        .thenReturn(TEST_INCIDENT_URN.toString());
    Mockito.when(environment.getArgument(Mockito.eq("input"))).thenReturn(input);

    Boolean result =
        new UpsertIncidentResolver(mockIncidentService, mockEntityService).get(environment).get();

    Assert.assertTrue(result);
    ArgumentCaptor<IncidentInfoUpsert> upsertCaptor =
        ArgumentCaptor.forClass(IncidentInfoUpsert.class);
    Mockito.verify(mockIncidentService)
        .upsertIncident(
            any(OperationContext.class), Mockito.eq(TEST_INCIDENT_URN), upsertCaptor.capture());
    IncidentInfoUpsert upsert = upsertCaptor.getValue();
    Assert.assertNull(upsert.getTitle());
    Assert.assertNull(upsert.getDescription());
    Assert.assertNull(upsert.getPriority());
    Assert.assertEquals(upsert.getEntities(), IncidentUtils.stringsToUrns(input.getResourceUrns()));
    Assert.assertNotNull(upsert.getAssignees());
    Assert.assertTrue(upsert.getAssignees().isEmpty());
    Assert.assertEquals(
        upsert.getStatus().getState(), com.linkedin.incident.IncidentState.RESOLVED);
  }

  private static IncidentInfo existingInfo() {
    return new IncidentInfo()
        .setType(IncidentType.SQL)
        .setEntities(
            new UrnArray(ImmutableList.of(UrnUtils.getUrn("urn:li:dataset:(test,test,test)"))))
        .setStatus(new IncidentStatus().setState(com.linkedin.incident.IncidentState.ACTIVE));
  }
}
