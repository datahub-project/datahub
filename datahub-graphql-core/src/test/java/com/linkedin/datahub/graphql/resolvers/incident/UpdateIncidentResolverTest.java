package com.linkedin.datahub.graphql.resolvers.incident;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.metadata.Constants.INCIDENT_INFO_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.IncidentPriority;
import com.linkedin.datahub.graphql.generated.UpdateIncidentInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.incident.IncidentAssignee;
import com.linkedin.incident.IncidentAssigneeArray;
import com.linkedin.incident.IncidentInfo;
import com.linkedin.incident.IncidentSource;
import com.linkedin.incident.IncidentSourceType;
import com.linkedin.incident.IncidentStage;
import com.linkedin.incident.IncidentState;
import com.linkedin.incident.IncidentStatus;
import com.linkedin.incident.IncidentType;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class UpdateIncidentResolverTest {

  private static final Urn TEST_INCIDENT_URN = UrnUtils.getUrn("urn:li:incident:TEST");

  @Test
  public void testGetSuccessAllFields() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.when(
            mockClient.ingestProposal(
                any(OperationContext.class),
                Mockito.any(MetadataChangeProposal.class),
                Mockito.anyBoolean()))
        .thenReturn(TEST_INCIDENT_URN.toString());

    IncidentInfo existingInfo = new IncidentInfo();
    existingInfo.setTitle("Title");
    existingInfo.setDescription("Description");
    existingInfo.setType(IncidentType.SQL);
    existingInfo.setEntities(
        new UrnArray(ImmutableList.of(UrnUtils.getUrn("urn:li:dataset:(test,test,test)"))));
    existingInfo.setStatus(
        new IncidentStatus().setState(IncidentState.ACTIVE).setMessage("Message"));
    existingInfo.setSource(new IncidentSource().setType(IncidentSourceType.MANUAL));

    EntityService mockEntityService = Mockito.mock(EntityService.class);
    Mockito.when(
            mockEntityService.getAspect(
                any(OperationContext.class),
                Mockito.eq(TEST_INCIDENT_URN),
                Mockito.eq(INCIDENT_INFO_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(existingInfo);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    UpdateIncidentInput testInput = new UpdateIncidentInput();
    testInput.setTitle("New Title");
    testInput.setDescription("New Description");
    final Long incidentStartedAtNew = 10L;
    testInput.setStartedAt(incidentStartedAtNew);
    testInput.setStatus(
        new com.linkedin.datahub.graphql.generated.IncidentStatusInput(
            com.linkedin.datahub.graphql.generated.IncidentState.RESOLVED,
            com.linkedin.datahub.graphql.generated.IncidentStage.FIXED,
            "Message 2"));
    testInput.setAssigneeUrns(ImmutableList.of("urn:li:corpuser:test", "urn:li:corpuser:test2"));
    testInput.setPriority(IncidentPriority.LOW);
    testInput.setResourceUrns(List.of("urn:li:dataset:(test,test,test2)"));

    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_INCIDENT_URN.toString());
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(testInput);

    UpdateIncidentResolver resolver = new UpdateIncidentResolver(mockClient, mockEntityService);
    Boolean result = resolver.get(mockEnv).get();

    Assert.assertTrue(result);

    IncidentInfo expectedInfo = new IncidentInfo();
    expectedInfo.setTitle("New Title");
    expectedInfo.setDescription("New Description");
    expectedInfo.setType(IncidentType.SQL);
    expectedInfo.setEntities(
        new UrnArray(ImmutableList.of(UrnUtils.getUrn("urn:li:dataset:(test,test,test2)"))));
    expectedInfo.setStartedAt(incidentStartedAtNew);
    expectedInfo.setStatus(
        new IncidentStatus()
            .setState(IncidentState.RESOLVED)
            .setStage(IncidentStage.FIXED)
            .setMessage("Message 2"));
    expectedInfo.setAssignees(
        new IncidentAssigneeArray(
            ImmutableList.of(
                new IncidentAssignee()
                    .setActor(UrnUtils.getUrn("urn:li:corpuser:test"))
                    .setAssignedAt(new AuditStamp()),
                new IncidentAssignee()
                    .setActor(UrnUtils.getUrn("urn:li:corpuser:test2"))
                    .setAssignedAt(new AuditStamp()))));
    expectedInfo.setPriority(3);
    expectedInfo.setSource(new IncidentSource().setType(IncidentSourceType.MANUAL));

    // Verify entity client
    Mockito.verify(mockClient, Mockito.times(1))
        .ingestProposal(
            any(OperationContext.class),
            Mockito.argThat(
                new IncidentInfoMatcher(
                    AspectUtils.buildMetadataChangeProposal(
                        TEST_INCIDENT_URN, INCIDENT_INFO_ASPECT_NAME, expectedInfo))),
            Mockito.anyBoolean());
  }

  @Test
  public void testGetSuccessRequiredFields() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.when(
            mockClient.ingestProposal(
                any(OperationContext.class),
                Mockito.any(MetadataChangeProposal.class),
                Mockito.anyBoolean()))
        .thenReturn(TEST_INCIDENT_URN.toString());

    IncidentInfo existingInfo = new IncidentInfo();
    existingInfo.setTitle("Title");
    existingInfo.setDescription("Description");
    existingInfo.setType(IncidentType.SQL);
    existingInfo.setEntities(
        new UrnArray(ImmutableList.of(UrnUtils.getUrn("urn:li:dataset:(test,test,test)"))));
    existingInfo.setStatus(
        new IncidentStatus()
            .setState(IncidentState.ACTIVE)
            .setStage(IncidentStage.INVESTIGATION)
            .setMessage("Message"));
    existingInfo.setAssignees(
        new IncidentAssigneeArray(
            ImmutableList.of(
                new IncidentAssignee()
                    .setActor(UrnUtils.getUrn("urn:li:corpuser:test"))
                    .setAssignedAt(new AuditStamp()))));
    existingInfo.setPriority(0);
    existingInfo.setSource(new IncidentSource().setType(IncidentSourceType.MANUAL));

    EntityService mockEntityService = Mockito.mock(EntityService.class);
    Mockito.when(
            mockEntityService.getAspect(
                any(OperationContext.class),
                Mockito.eq(TEST_INCIDENT_URN),
                Mockito.eq(INCIDENT_INFO_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(existingInfo);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    UpdateIncidentInput testInput = new UpdateIncidentInput();

    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_INCIDENT_URN.toString());
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(testInput);

    UpdateIncidentResolver resolver = new UpdateIncidentResolver(mockClient, mockEntityService);
    Boolean result = resolver.get(mockEnv).get();

    Assert.assertTrue(result);

    IncidentInfo expectedInfo = new IncidentInfo();
    expectedInfo.setTitle("Title");
    expectedInfo.setDescription("Description");
    expectedInfo.setType(IncidentType.SQL);
    expectedInfo.setEntities(
        new UrnArray(ImmutableList.of(UrnUtils.getUrn("urn:li:dataset:(test,test,test)"))));
    expectedInfo.setStatus(
        new IncidentStatus().setState(IncidentState.ACTIVE).setMessage("Message"));
    expectedInfo.setSource(new IncidentSource().setType(IncidentSourceType.MANUAL));

    // Verify entity client
    Mockito.verify(mockClient, Mockito.times(1))
        .ingestProposal(
            any(OperationContext.class),
            Mockito.argThat(
                new IncidentInfoMatcher(
                    AspectUtils.buildMetadataChangeProposal(
                        TEST_INCIDENT_URN, INCIDENT_INFO_ASPECT_NAME, expectedInfo))),
            Mockito.anyBoolean());
  }

  @Test
  public void testGetFailureIncidentDoesNotExist() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.when(
            mockClient.ingestProposal(
                any(OperationContext.class),
                Mockito.any(MetadataChangeProposal.class),
                Mockito.anyBoolean()))
        .thenReturn(TEST_INCIDENT_URN.toString());

    IncidentInfo existingInfo = new IncidentInfo();
    existingInfo.setTitle("Title");
    existingInfo.setDescription("Description");
    existingInfo.setType(IncidentType.SQL);
    existingInfo.setEntities(
        new UrnArray(ImmutableList.of(UrnUtils.getUrn("urn:li:dataset:(test,test,test)"))));
    existingInfo.setStatus(
        new IncidentStatus()
            .setState(IncidentState.ACTIVE)
            .setStage(IncidentStage.INVESTIGATION)
            .setMessage("Message"));
    existingInfo.setAssignees(
        new IncidentAssigneeArray(
            ImmutableList.of(
                new IncidentAssignee()
                    .setActor(UrnUtils.getUrn("urn:li:corpuser:test"))
                    .setAssignedAt(new AuditStamp()))));
    existingInfo.setPriority(0);
    existingInfo.setSource(new IncidentSource().setType(IncidentSourceType.MANUAL));

    EntityService mockEntityService = Mockito.mock(EntityService.class);
    Mockito.when(
            mockEntityService.getAspect(
                any(OperationContext.class),
                Mockito.eq(TEST_INCIDENT_URN),
                Mockito.eq(INCIDENT_INFO_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(null);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    UpdateIncidentInput testInput = new UpdateIncidentInput();

    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_INCIDENT_URN.toString());
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(testInput);

    UpdateIncidentResolver resolver = new UpdateIncidentResolver(mockClient, mockEntityService);

    Assert.assertThrows(() -> resolver.get(mockEnv).get());
  }
}
