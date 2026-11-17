package com.linkedin.datahub.graphql.resolvers.incident;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContextForResource;
import static com.linkedin.metadata.Constants.INCIDENT_INFO_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.IncidentPriority;
import com.linkedin.datahub.graphql.generated.RaiseIncidentInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.incident.IncidentAssignee;
import com.linkedin.incident.IncidentAssigneeArray;
import com.linkedin.incident.IncidentInfo;
import com.linkedin.incident.IncidentSource;
import com.linkedin.incident.IncidentSourceType;
import com.linkedin.incident.IncidentStage;
import com.linkedin.incident.IncidentState;
import com.linkedin.incident.IncidentStatus;
import com.linkedin.incident.IncidentType;
import com.linkedin.metadata.aspect.validation.CreateIfNotExistsValidator;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.key.IncidentKey;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metadata.utils.SchemaFieldUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class RaiseIncidentResolverTest {

  private static final Urn TEST_INCIDENT_URN = UrnUtils.getUrn("urn:li:incident:TEST");
  private static final Urn TEST_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,SampleTable,PROD)");
  private static final Urn TEST_SCHEMA_FIELD_URN =
      SchemaFieldUtils.generateSchemaFieldUrn(TEST_DATASET_URN, "user_id");

  @Test
  public void testGetSuccessAllFields() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.when(
            mockClient.ingestProposal(
                any(OperationContext.class),
                Mockito.any(MetadataChangeProposal.class),
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
            any(OperationContext.class),
            Mockito.argThat(
                new IncidentInfoMatcher(
                    AspectUtils.buildMetadataChangeProposal(
                        TEST_INCIDENT_URN, INCIDENT_INFO_ASPECT_NAME, expectedInfo))),
            Mockito.anyBoolean());
  }

  @Test
  public void testCustomTypeRequired() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    RaiseIncidentInput testInput = new RaiseIncidentInput();
    testInput.setType(com.linkedin.datahub.graphql.generated.IncidentType.CUSTOM);
    testInput.setResourceUrn("urn:li:dataset:(test,test,test)");
    testInput.setTitle("Title");
    testInput.setDescription("Description");

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(testInput);

    RaiseIncidentResolver resolver = new RaiseIncidentResolver(mockClient);

    try {
      resolver.get(mockEnv).get();
      Assert.fail("Expected exception was not thrown");
    } catch (ExecutionException e) {
      Assert.assertEquals(
          "Failed to raise incident: customType is required when type is CUSTOM",
          e.getCause().getMessage());
    }
  }

  @Test
  public void testCustomTypeBlankRejected() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    RaiseIncidentInput testInput = new RaiseIncidentInput();
    testInput.setType(com.linkedin.datahub.graphql.generated.IncidentType.CUSTOM);
    testInput.setCustomType("   ");
    testInput.setResourceUrn("urn:li:dataset:(test,test,test)");
    testInput.setTitle("Title");
    testInput.setDescription("Description");

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(testInput);

    RaiseIncidentResolver resolver = new RaiseIncidentResolver(mockClient);

    try {
      resolver.get(mockEnv).get();
      Assert.fail("Expected exception was not thrown");
    } catch (ExecutionException e) {
      Assert.assertEquals(
          "Failed to raise incident: customType is required when type is CUSTOM",
          e.getCause().getMessage());
    }
  }

  @Test
  public void testGetFailRequiredFields() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.when(
            mockClient.ingestProposal(
                any(OperationContext.class),
                Mockito.any(MetadataChangeProposal.class),
                Mockito.anyBoolean()))
        .thenReturn(TEST_INCIDENT_URN.toString());

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    RaiseIncidentInput testInput = new RaiseIncidentInput();
    testInput.setType(com.linkedin.datahub.graphql.generated.IncidentType.SQL);
    testInput.setResourceUrns(Collections.emptyList());

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(testInput);

    RaiseIncidentResolver resolver = new RaiseIncidentResolver(mockClient);
    Exception exception =
        Assert.expectThrows(
            RuntimeException.class,
            () -> {
              resolver.get(mockEnv).get();
            });

    Assert.assertEquals(
        exception.getMessage(), "At least 1 resource urn must be defined to raise an incident.");
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

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    RaiseIncidentInput testInput = new RaiseIncidentInput();
    testInput.setType(com.linkedin.datahub.graphql.generated.IncidentType.SQL);
    testInput.setResourceUrn("urn:li:dataset:(test,test,test)");
    testInput.setResourceUrns(
        List.of(
            "urn:li:dataset:(test,test,test)",
            "urn:li:dataset:(test,test,test2)",
            "urn:li:dataset:(test,test,test3)"));

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(testInput);

    RaiseIncidentResolver resolver = new RaiseIncidentResolver(mockClient);
    String result = resolver.get(mockEnv).get();

    Assert.assertEquals(result, TEST_INCIDENT_URN.toString());

    IncidentInfo expectedInfo = new IncidentInfo();
    expectedInfo.setType(IncidentType.SQL);
    expectedInfo.setEntities(
        new UrnArray(
            IncidentUtils.stringsToUrns(
                ImmutableList.of(
                    "urn:li:dataset:(test,test,test)",
                    "urn:li:dataset:(test,test,test2)",
                    "urn:li:dataset:(test,test,test3)"))));
    expectedInfo.setStatus(new IncidentStatus().setState(IncidentState.ACTIVE));
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
  public void testGetWithBlankIdThrowsBadRequest() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    RaiseIncidentInput testInput = new RaiseIncidentInput();
    testInput.setType(com.linkedin.datahub.graphql.generated.IncidentType.SQL);
    testInput.setResourceUrn("urn:li:dataset:(test,test,test)");

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(testInput);

    RaiseIncidentResolver resolver = new RaiseIncidentResolver(mockClient);

    for (String invalidId : List.of("", "   ")) {
      testInput.setId(invalidId);
      try {
        resolver.get(mockEnv).get();
        Assert.fail("Expected exception was not thrown for invalid id: " + invalidId);
      } catch (ExecutionException e) {
        Assert.assertTrue(e.getCause() instanceof DataHubGraphQLException);
        Assert.assertEquals(
            ((DataHubGraphQLException) e.getCause()).errorCode(),
            DataHubGraphQLErrorCode.BAD_REQUEST);
      }
    }
    Mockito.verifyNoInteractions(mockClient);
  }

  @Test
  public void testGetWithIdCreatesAtGivenUrn() throws Exception {
    final String callerId = "checkout-drift-2026-08-01-poll-status";
    final Urn expectedUrn = UrnUtils.getUrn("urn:li:incident:" + callerId);

    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.when(
            mockClient.ingestProposal(
                any(OperationContext.class),
                Mockito.any(MetadataChangeProposal.class),
                Mockito.anyBoolean()))
        .thenReturn(expectedUrn.toString());

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    RaiseIncidentInput testInput = new RaiseIncidentInput();
    testInput.setId(callerId);
    testInput.setType(com.linkedin.datahub.graphql.generated.IncidentType.SQL);
    testInput.setResourceUrn("urn:li:dataset:(test,test,test)");

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(testInput);

    RaiseIncidentResolver resolver = new RaiseIncidentResolver(mockClient);
    String result = resolver.get(mockEnv).get();

    Assert.assertEquals(result, expectedUrn.toString());

    ArgumentCaptor<MetadataChangeProposal> proposalCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    Mockito.verify(mockClient, Mockito.times(1))
        .ingestProposal(
            any(OperationContext.class), proposalCaptor.capture(), Mockito.anyBoolean());

    MetadataChangeProposal proposal = proposalCaptor.getValue();
    Assert.assertEquals(proposal.getChangeType(), ChangeType.CREATE_ENTITY);
    Assert.assertEquals(
        proposal.getHeaders().get(CreateIfNotExistsValidator.FILTER_EXCEPTION_HEADER),
        CreateIfNotExistsValidator.FILTER_EXCEPTION_VALUE);
    Assert.assertEquals(
        proposal.getEntityKeyAspect(),
        GenericRecordUtils.serializeAspect(new IncidentKey().setId(callerId)));
  }

  @Test
  public void testGetWithExistingIdThrowsConflict() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    // A null urn back from ingestProposal is what CreateIfNotExistsValidator produces when the
    // CREATE_ENTITY write was filtered because the key already exists.
    Mockito.when(
            mockClient.ingestProposal(
                any(OperationContext.class),
                Mockito.any(MetadataChangeProposal.class),
                Mockito.anyBoolean()))
        .thenReturn(null);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    RaiseIncidentInput testInput = new RaiseIncidentInput();
    testInput.setId("already-exists");
    testInput.setType(com.linkedin.datahub.graphql.generated.IncidentType.SQL);
    testInput.setResourceUrn("urn:li:dataset:(test,test,test)");

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(testInput);

    RaiseIncidentResolver resolver = new RaiseIncidentResolver(mockClient);

    try {
      resolver.get(mockEnv).get();
      Assert.fail("Expected exception was not thrown");
    } catch (ExecutionException e) {
      Assert.assertTrue(e.getCause() instanceof DataHubGraphQLException);
      Assert.assertEquals(
          ((DataHubGraphQLException) e.getCause()).errorCode(), DataHubGraphQLErrorCode.CONFLICT);
    }
  }

  @Test
  public void testGetWithoutIdUsesUpsertChangeType() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.when(
            mockClient.ingestProposal(
                any(OperationContext.class),
                Mockito.any(MetadataChangeProposal.class),
                Mockito.anyBoolean()))
        .thenReturn(TEST_INCIDENT_URN.toString());

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    RaiseIncidentInput testInput = new RaiseIncidentInput();
    testInput.setType(com.linkedin.datahub.graphql.generated.IncidentType.SQL);
    testInput.setResourceUrn("urn:li:dataset:(test,test,test)");

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(testInput);

    RaiseIncidentResolver resolver = new RaiseIncidentResolver(mockClient);
    resolver.get(mockEnv).get();

    ArgumentCaptor<MetadataChangeProposal> proposalCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    Mockito.verify(mockClient, Mockito.times(1))
        .ingestProposal(
            any(OperationContext.class), proposalCaptor.capture(), Mockito.anyBoolean());

    Assert.assertEquals(proposalCaptor.getValue().getChangeType(), ChangeType.UPSERT);
    Assert.assertFalse(proposalCaptor.getValue().hasHeaders());
  }

  @Test
  public void testRaiseOnSchemaFieldAllowedByParentDatasetPolicy() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.when(
            mockClient.ingestProposal(
                any(OperationContext.class),
                Mockito.any(MetadataChangeProposal.class),
                Mockito.anyBoolean()))
        .thenReturn(TEST_INCIDENT_URN.toString());

    // The actor holds EDIT_ENTITY_INCIDENTS on the parent dataset only. No policy can name the
    // field itself, so this is the realistic grant for someone raising a column incident.
    QueryContext mockContext =
        getMockAllowContextForResource(
            "urn:li:corpuser:test",
            PoliciesConfig.EDIT_ENTITY_INCIDENTS_PRIVILEGE.getType(),
            TEST_DATASET_URN);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    RaiseIncidentInput testInput = new RaiseIncidentInput();
    testInput.setType(com.linkedin.datahub.graphql.generated.IncidentType.OPERATIONAL);
    testInput.setResourceUrn(TEST_SCHEMA_FIELD_URN.toString());
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(testInput);

    RaiseIncidentResolver resolver = new RaiseIncidentResolver(mockClient);
    Assert.assertEquals(resolver.get(mockEnv).get(), TEST_INCIDENT_URN.toString());
  }

  @Test
  public void testRaiseOnSchemaFieldDeniedWhenParentPolicyDoesNotMatch() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    QueryContext mockContext =
        getMockAllowContextForResource(
            "urn:li:corpuser:test",
            PoliciesConfig.EDIT_ENTITY_INCIDENTS_PRIVILEGE.getType(),
            UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,OtherTable,PROD)"));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    RaiseIncidentInput testInput = new RaiseIncidentInput();
    testInput.setType(com.linkedin.datahub.graphql.generated.IncidentType.OPERATIONAL);
    testInput.setResourceUrn(TEST_SCHEMA_FIELD_URN.toString());
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(testInput);

    RaiseIncidentResolver resolver = new RaiseIncidentResolver(mockClient);
    try {
      resolver.get(mockEnv).get();
      Assert.fail("Expected exception was not thrown");
    } catch (ExecutionException e) {
      Assert.assertTrue(e.getCause() instanceof AuthorizationException);
    }
    Mockito.verifyNoInteractions(mockClient);
  }
}
