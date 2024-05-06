package com.linkedin.datahub.graphql.resolvers.incident;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.metadata.Constants.INCIDENT_INFO_ASPECT_NAME;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.IncidentPriority;
import com.linkedin.datahub.graphql.generated.RaiseIncidentInput;
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
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetchingEnvironment;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class RaiseIncidentResolverTest {

  private static final Urn TEST_INCIDENT_URN = UrnUtils.getUrn("urn:li:incident:TEST");

  @Test
  public void testGetSuccessAllFields() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.when(
            mockClient.ingestProposal(
                Mockito.any(MetadataChangeProposal.class),
                Mockito.any(Authentication.class),
                Mockito.anyBoolean()))
        .thenReturn(TEST_INCIDENT_URN.toString());

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    RaiseIncidentInput testInput = new RaiseIncidentInput();
    testInput.setTitle("Title");
    testInput.setDescription("Description");
    testInput.setType(com.linkedin.datahub.graphql.generated.IncidentType.SQL);
    testInput.setResourceUrn("urn:li:dataset:(test,test,test)");
    Long incidentStartedAtMillis = System.currentTimeMillis();
    testInput.setStartedAt(incidentStartedAtMillis);
    testInput.setStatus(
        new com.linkedin.datahub.graphql.generated.IncidentStatusInput(
            com.linkedin.datahub.graphql.generated.IncidentState.ACTIVE,
            com.linkedin.datahub.graphql.generated.IncidentStage.INVESTIGATION,
            "Message"));
    testInput.setAssigneeUrns(ImmutableList.of("urn:li:corpuser:test"));
    testInput.setSource(
        new com.linkedin.datahub.graphql.generated.IncidentSourceInput(
            com.linkedin.datahub.graphql.generated.IncidentSourceType.MANUAL));
    testInput.setPriority(IncidentPriority.CRITICAL);

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(testInput);

    RaiseIncidentResolver resolver = new RaiseIncidentResolver(mockClient);
    String result = resolver.get(mockEnv).get();

    Assert.assertEquals(result, TEST_INCIDENT_URN.toString());

    IncidentInfo expectedInfo = new IncidentInfo();
    expectedInfo.setTitle("Title");
    expectedInfo.setDescription("Description");
    expectedInfo.setType(IncidentType.SQL);
    expectedInfo.setEntities(
        new UrnArray(ImmutableList.of(UrnUtils.getUrn("urn:li:dataset:(test,test,test)"))));
    expectedInfo.setStartedAt(incidentStartedAtMillis);
    expectedInfo.setStatus(
        new IncidentStatus()
            .setState(IncidentState.ACTIVE)
            .setStage(IncidentStage.INVESTIGATION)
            .setMessage("Message"));
    expectedInfo.setAssignees(
        new IncidentAssigneeArray(
            ImmutableList.of(
                new IncidentAssignee()
                    .setActor(UrnUtils.getUrn("urn:li:corpuser:test"))
                    .setAssignedAt(new AuditStamp()))));
    expectedInfo.setPriority(0);
    expectedInfo.setSource(new IncidentSource().setType(IncidentSourceType.MANUAL));

    // Verify entity client
    Mockito.verify(mockClient, Mockito.times(1))
        .ingestProposal(
            Mockito.argThat(
                new IncidentInfoMatcher(
                    AspectUtils.buildMetadataChangeProposal(
                        TEST_INCIDENT_URN, INCIDENT_INFO_ASPECT_NAME, expectedInfo))),
            Mockito.any(Authentication.class),
            Mockito.anyBoolean());
  }

  @Test
  public void testGetSuccessRequiredFields() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.when(
            mockClient.ingestProposal(
                Mockito.any(MetadataChangeProposal.class),
                Mockito.any(Authentication.class),
                Mockito.anyBoolean()))
        .thenReturn(TEST_INCIDENT_URN.toString());

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    RaiseIncidentInput testInput = new RaiseIncidentInput();
    testInput.setType(com.linkedin.datahub.graphql.generated.IncidentType.SQL);
    testInput.setResourceUrn("urn:li:dataset:(test,test,test)");

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(testInput);

    RaiseIncidentResolver resolver = new RaiseIncidentResolver(mockClient);
    String result = resolver.get(mockEnv).get();

    Assert.assertEquals(result, TEST_INCIDENT_URN.toString());

    IncidentInfo expectedInfo = new IncidentInfo();
    expectedInfo.setType(IncidentType.SQL);
    expectedInfo.setEntities(
        new UrnArray(ImmutableList.of(UrnUtils.getUrn("urn:li:dataset:(test,test,test)"))));
    expectedInfo.setStatus(new IncidentStatus().setState(IncidentState.ACTIVE));
    expectedInfo.setSource(new IncidentSource().setType(IncidentSourceType.MANUAL));

    // Verify entity client
    Mockito.verify(mockClient, Mockito.times(1))
        .ingestProposal(
            Mockito.argThat(
                new IncidentInfoMatcher(
                    AspectUtils.buildMetadataChangeProposal(
                        TEST_INCIDENT_URN, INCIDENT_INFO_ASPECT_NAME, expectedInfo))),
            Mockito.any(Authentication.class),
            Mockito.anyBoolean());
  }
}
