package com.linkedin.datahub.graphql.resolvers.incident;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.metadata.Constants.INCIDENT_INFO_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.IncidentPriority;
import com.linkedin.datahub.graphql.generated.UpdateIncidentInput;
import com.linkedin.incident.IncidentInfo;
import com.linkedin.incident.IncidentState;
import com.linkedin.incident.IncidentStatus;
import com.linkedin.incident.IncidentType;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.service.IncidentInfoPatch;
import com.linkedin.metadata.service.IncidentService;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class UpdateIncidentResolverTest {

  private static final Urn TEST_INCIDENT_URN = UrnUtils.getUrn("urn:li:incident:TEST");

  @Test
  public void testAllSuppliedFieldsArePassedToService() throws Exception {
    IncidentService mockIncidentService = Mockito.mock(IncidentService.class);
    EntityService mockEntityService = Mockito.mock(EntityService.class);
    Mockito.when(
            mockEntityService.getAspect(
                any(OperationContext.class),
                Mockito.eq(TEST_INCIDENT_URN),
                Mockito.eq(INCIDENT_INFO_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(existingInfo());

    UpdateIncidentInput input = new UpdateIncidentInput();
    input.setTitle("New Title");
    input.setDescription("New Description");
    input.setStartedAt(10L);
    input.setStatus(
        new com.linkedin.datahub.graphql.generated.IncidentStatusInput(
            com.linkedin.datahub.graphql.generated.IncidentState.RESOLVED,
            com.linkedin.datahub.graphql.generated.IncidentStage.FIXED,
            "Message 2"));
    input.setAssigneeUrns(ImmutableList.of("urn:li:corpuser:test", "urn:li:corpuser:test2"));
    input.setPriority(IncidentPriority.LOW);
    input.setResourceUrns(List.of("urn:li:dataset:(test,test,test2)"));

    DataFetchingEnvironment environment = newEnvironment(input);
    Boolean result = newResolver(mockIncidentService, mockEntityService).get(environment).get();

    Assert.assertTrue(result);
    ArgumentCaptor<IncidentInfoPatch> updateCaptor =
        ArgumentCaptor.forClass(IncidentInfoPatch.class);
    Mockito.verify(mockIncidentService)
        .updateIncident(
            any(OperationContext.class), Mockito.eq(TEST_INCIDENT_URN), updateCaptor.capture());
    IncidentInfoPatch update = updateCaptor.getValue();
    Assert.assertEquals(update.getTitle(), "New Title");
    Assert.assertEquals(update.getDescription(), "New Description");
    Assert.assertEquals(update.getStartedAt(), Long.valueOf(10L));
    Assert.assertEquals(update.getPriority(), Integer.valueOf(3));
    Assert.assertEquals(update.getEntities(), IncidentUtils.stringsToUrns(input.getResourceUrns()));
    Assert.assertEquals(update.getAssignees().size(), 2);
    Assert.assertEquals(update.getStatus().getState(), IncidentState.RESOLVED);
  }

  @Test
  public void testOmittedFieldsRemainUnchanged() throws Exception {
    IncidentService mockIncidentService = Mockito.mock(IncidentService.class);
    EntityService mockEntityService = Mockito.mock(EntityService.class);
    Mockito.when(
            mockEntityService.getAspect(
                any(OperationContext.class),
                Mockito.eq(TEST_INCIDENT_URN),
                Mockito.eq(INCIDENT_INFO_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(existingInfo());

    UpdateIncidentInput input = new UpdateIncidentInput();
    input.setTitle("Only title changes");

    DataFetchingEnvironment environment = newEnvironment(input);
    Boolean result = newResolver(mockIncidentService, mockEntityService).get(environment).get();

    Assert.assertTrue(result);
    ArgumentCaptor<IncidentInfoPatch> updateCaptor =
        ArgumentCaptor.forClass(IncidentInfoPatch.class);
    Mockito.verify(mockIncidentService)
        .updateIncident(
            any(OperationContext.class), Mockito.eq(TEST_INCIDENT_URN), updateCaptor.capture());
    IncidentInfoPatch update = updateCaptor.getValue();
    Assert.assertEquals(update.getTitle(), "Only title changes");
    Assert.assertNull(update.getDescription());
    Assert.assertNull(update.getStartedAt());
    Assert.assertNull(update.getStatus());
    Assert.assertNull(update.getPriority());
    Assert.assertNull(update.getEntities());
    Assert.assertNull(update.getAssignees());
  }

  @Test
  public void testGetFailureIncidentDoesNotExist() throws Exception {
    IncidentService mockIncidentService = Mockito.mock(IncidentService.class);
    EntityService mockEntityService = Mockito.mock(EntityService.class);
    Mockito.when(
            mockEntityService.getAspect(
                any(OperationContext.class),
                Mockito.eq(TEST_INCIDENT_URN),
                Mockito.eq(INCIDENT_INFO_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(null);

    Assert.assertThrows(
        () ->
            newResolver(mockIncidentService, mockEntityService)
                .get(newEnvironment(new UpdateIncidentInput()))
                .get());
  }

  private static UpdateIncidentResolver newResolver(
      IncidentService incidentService, EntityService entityService) {
    return new UpdateIncidentResolver(incidentService, entityService);
  }

  private static DataFetchingEnvironment newEnvironment(UpdateIncidentInput input) {
    DataFetchingEnvironment environment = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext context = getMockAllowContext();
    Mockito.when(environment.getContext()).thenReturn(context);
    Mockito.when(environment.getArgument(Mockito.eq("urn")))
        .thenReturn(TEST_INCIDENT_URN.toString());
    Mockito.when(environment.getArgument(Mockito.eq("input"))).thenReturn(input);
    return environment;
  }

  private static IncidentInfo existingInfo() {
    return new IncidentInfo()
        .setType(IncidentType.SQL)
        .setEntities(
            new UrnArray(ImmutableList.of(UrnUtils.getUrn("urn:li:dataset:(test,test,test)"))))
        .setStatus(new IncidentStatus().setState(IncidentState.ACTIVE));
  }
}
