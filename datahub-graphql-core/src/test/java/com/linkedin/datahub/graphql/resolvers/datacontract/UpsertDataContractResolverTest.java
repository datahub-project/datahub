package com.linkedin.datahub.graphql.resolvers.datacontract;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.datahub.graphql.resolvers.datacontract.EntityDataContractResolver.*;
import static com.linkedin.metadata.utils.SystemMetadataUtils.createDefaultSystemMetadata;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.EntityRelationship;
import com.linkedin.common.EntityRelationshipArray;
import com.linkedin.common.EntityRelationships;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringMap;
import com.linkedin.datacontract.DataContractProperties;
import com.linkedin.datacontract.DataContractStatus;
import com.linkedin.datacontract.DataQualityContract;
import com.linkedin.datacontract.DataQualityContractArray;
import com.linkedin.datacontract.FreshnessContract;
import com.linkedin.datacontract.FreshnessContractArray;
import com.linkedin.datacontract.SchemaContract;
import com.linkedin.datacontract.SchemaContractArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataContract;
import com.linkedin.datahub.graphql.generated.DataContractState;
import com.linkedin.datahub.graphql.generated.DataQualityContractInput;
import com.linkedin.datahub.graphql.generated.FreshnessContractInput;
import com.linkedin.datahub.graphql.generated.SchemaContractInput;
import com.linkedin.datahub.graphql.generated.UpsertDataContractInput;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.AspectType;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.key.DataContractKey;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class UpsertDataContractResolverTest {

  private static final Urn TEST_CONTRACT_URN = UrnUtils.getUrn("urn:li:dataContract:test-id");
  private static final Urn TEST_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,test,PROD)");
  private static final Urn TEST_FRESHNESS_ASSERTION_URN =
      UrnUtils.getUrn("urn:li:assertion:freshness");
  private static final Urn TEST_SCHEMA_ASSERTION_URN = UrnUtils.getUrn("urn:li:assertion:schema");
  private static final Urn TEST_QUALITY_ASSERTION_URN = UrnUtils.getUrn("urn:li:assertion:quality");

  private static final UpsertDataContractInput TEST_CREATE_INPUT =
      new UpsertDataContractInput(
          TEST_DATASET_URN.toString(),
          ImmutableList.of(new FreshnessContractInput(TEST_FRESHNESS_ASSERTION_URN.toString())),
          ImmutableList.of(new SchemaContractInput(TEST_SCHEMA_ASSERTION_URN.toString())),
          ImmutableList.of(new DataQualityContractInput(TEST_QUALITY_ASSERTION_URN.toString())),
          DataContractState.PENDING,
          "test-id");

  private static final UpsertDataContractInput TEST_VALID_UPDATE_INPUT =
      new UpsertDataContractInput(
          TEST_DATASET_URN.toString(),
          ImmutableList.of(new FreshnessContractInput(TEST_FRESHNESS_ASSERTION_URN.toString())),
          ImmutableList.of(new SchemaContractInput(TEST_SCHEMA_ASSERTION_URN.toString())),
          ImmutableList.of(new DataQualityContractInput(TEST_QUALITY_ASSERTION_URN.toString())),
          DataContractState.ACTIVE,
          null);

  private static final Urn TEST_ACTOR_URN = UrnUtils.getUrn("urn:li:corpuser:test");

  @Captor private ArgumentCaptor<List<MetadataChangeProposal>> proposalCaptor;

  @BeforeTest
  public void init() {
    MockitoAnnotations.openMocks(this);
  }

  @Test
  public void testGetSuccessCreate() throws Exception {
    // Expected results
    final DataContractKey key = new DataContractKey();
    key.setId("test-id");
    final Urn dataContractUrn =
        EntityKeyUtils.convertEntityKeyToUrn(key, Constants.DATA_CONTRACT_ENTITY_NAME);

    final DataContractStatus status = new DataContractStatus();
    status.setState(com.linkedin.datacontract.DataContractState.PENDING);

    final DataContractProperties props = new DataContractProperties();
    props.setEntity(TEST_DATASET_URN);
    props.setDataQuality(
        new DataQualityContractArray(
            ImmutableList.of(new DataQualityContract().setAssertion(TEST_QUALITY_ASSERTION_URN))));
    props.setFreshness(
        new FreshnessContractArray(
            ImmutableList.of(new FreshnessContract().setAssertion(TEST_FRESHNESS_ASSERTION_URN))));
    props.setSchema(
        new SchemaContractArray(
            ImmutableList.of(new SchemaContract().setAssertion(TEST_SCHEMA_ASSERTION_URN))));

    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    GraphClient mockGraphClient = Mockito.mock(GraphClient.class);
    initMockGraphClient(mockGraphClient, null);
    initMockEntityClient(mockClient, null, props); // No existing contract
    UpsertDataContractResolver resolver =
        new UpsertDataContractResolver(mockClient, mockGraphClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_CREATE_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    DataContract result = resolver.get(mockEnv).get();

    final MetadataChangeProposal propertiesProposal = new MetadataChangeProposal();
    propertiesProposal.setEntityUrn(dataContractUrn);
    propertiesProposal.setEntityType(Constants.DATA_CONTRACT_ENTITY_NAME);
    propertiesProposal.setSystemMetadata(
        createDefaultSystemMetadata()
            .setProperties(new StringMap(ImmutableMap.of("appSource", "ui"))));
    propertiesProposal.setAspectName(Constants.DATA_CONTRACT_PROPERTIES_ASPECT_NAME);
    propertiesProposal.setAspect(GenericRecordUtils.serializeAspect(props));
    propertiesProposal.setChangeType(ChangeType.UPSERT);

    final MetadataChangeProposal statusProposal = new MetadataChangeProposal();
    statusProposal.setEntityUrn(dataContractUrn);
    statusProposal.setEntityType(Constants.DATA_CONTRACT_ENTITY_NAME);
    statusProposal.setSystemMetadata(
        createDefaultSystemMetadata()
            .setProperties(new StringMap(ImmutableMap.of("appSource", "ui"))));
    statusProposal.setAspectName(Constants.DATA_CONTRACT_STATUS_ASPECT_NAME);
    statusProposal.setAspect(GenericRecordUtils.serializeAspect(status));
    statusProposal.setChangeType(ChangeType.UPSERT);

    Mockito.verify(mockClient, Mockito.times(1))
        .batchIngestProposals(
            any(OperationContext.class), proposalCaptor.capture(), Mockito.eq(false));

    // check has time
    Assert.assertTrue(
        proposalCaptor.getValue().stream()
            .allMatch(prop -> prop.getSystemMetadata().getLastObserved() > 0L));

    // check without time
    Assert.assertEquals(
        proposalCaptor.getValue().stream()
            .map(m -> m.getSystemMetadata().setLastObserved(0))
            .collect(Collectors.toList()),
        List.of(propertiesProposal, statusProposal).stream()
            .map(m -> m.getSystemMetadata().setLastObserved(0))
            .collect(Collectors.toList()));

    Assert.assertEquals(result.getUrn(), TEST_CONTRACT_URN.toString());
  }

  @Test
  public void testGetSuccessUpdate() throws Exception {

    DataContractProperties props = new DataContractProperties();
    props.setEntity(TEST_DATASET_URN);
    props.setDataQuality(
        new DataQualityContractArray(
            ImmutableList.of(new DataQualityContract().setAssertion(TEST_QUALITY_ASSERTION_URN))));
    props.setFreshness(
        new FreshnessContractArray(
            ImmutableList.of(new FreshnessContract().setAssertion(TEST_FRESHNESS_ASSERTION_URN))));
    props.setSchema(
        new SchemaContractArray(
            ImmutableList.of(new SchemaContract().setAssertion(TEST_SCHEMA_ASSERTION_URN))));

    DataContractStatus status = new DataContractStatus();
    status.setState(com.linkedin.datacontract.DataContractState.ACTIVE);

    // Update resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    GraphClient mockGraphClient = Mockito.mock(GraphClient.class);
    initMockGraphClient(mockGraphClient, TEST_CONTRACT_URN);
    initMockEntityClient(mockClient, TEST_CONTRACT_URN, props); // Contract Exists
    UpsertDataContractResolver resolver =
        new UpsertDataContractResolver(mockClient, mockGraphClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_VALID_UPDATE_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    DataContract result = resolver.get(mockEnv).get();

    final MetadataChangeProposal propertiesProposal = new MetadataChangeProposal();
    propertiesProposal.setEntityUrn(TEST_CONTRACT_URN);
    propertiesProposal.setEntityType(Constants.DATA_CONTRACT_ENTITY_NAME);
    propertiesProposal.setSystemMetadata(
        createDefaultSystemMetadata()
            .setProperties(new StringMap(ImmutableMap.of("appSource", "ui"))));
    propertiesProposal.setAspectName(Constants.DATA_CONTRACT_PROPERTIES_ASPECT_NAME);
    propertiesProposal.setAspect(GenericRecordUtils.serializeAspect(props));
    propertiesProposal.setChangeType(ChangeType.UPSERT);

    final MetadataChangeProposal statusProposal = new MetadataChangeProposal();
    statusProposal.setEntityUrn(TEST_CONTRACT_URN);
    statusProposal.setEntityType(Constants.DATA_CONTRACT_ENTITY_NAME);
    statusProposal.setSystemMetadata(
        createDefaultSystemMetadata()
            .setProperties(new StringMap(ImmutableMap.of("appSource", "ui"))));
    statusProposal.setAspectName(Constants.DATA_CONTRACT_STATUS_ASPECT_NAME);
    statusProposal.setAspect(GenericRecordUtils.serializeAspect(status));
    statusProposal.setChangeType(ChangeType.UPSERT);

    Mockito.verify(mockClient, Mockito.times(1))
        .batchIngestProposals(
            any(OperationContext.class), proposalCaptor.capture(), Mockito.eq(false));

    // check has time
    Assert.assertTrue(
        proposalCaptor.getValue().stream()
            .allMatch(prop -> prop.getSystemMetadata().getLastObserved() > 0L));

    // check without time
    Assert.assertEquals(
        proposalCaptor.getValue().stream()
            .map(m -> m.getSystemMetadata().setLastObserved(0))
            .collect(Collectors.toList()),
        List.of(propertiesProposal, statusProposal).stream()
            .map(m -> m.getSystemMetadata().setLastObserved(0))
            .collect(Collectors.toList()));

    Assert.assertEquals(result.getUrn(), TEST_CONTRACT_URN.toString());
  }

  @Test
  public void testGetFailureEntityDoesNotExist() throws Exception {
    // Update resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    GraphClient mockGraphClient = Mockito.mock(GraphClient.class);
    initMockGraphClient(mockGraphClient, TEST_CONTRACT_URN);
    Mockito.when(mockClient.exists(any(OperationContext.class), Mockito.eq(TEST_DATASET_URN)))
        .thenReturn(false);
    UpsertDataContractResolver resolver =
        new UpsertDataContractResolver(mockClient, mockGraphClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_CREATE_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Assert.assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  @Test
  public void testGetFailureAssertionDoesNotExist() throws Exception {
    // Update resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    GraphClient mockGraphClient = Mockito.mock(GraphClient.class);
    initMockGraphClient(mockGraphClient, TEST_CONTRACT_URN);
    Mockito.when(mockClient.exists(any(OperationContext.class), Mockito.eq(TEST_DATASET_URN)))
        .thenReturn(true);
    Mockito.when(
            mockClient.exists(
                any(OperationContext.class), Mockito.eq(TEST_FRESHNESS_ASSERTION_URN)))
        .thenReturn(false);
    Mockito.when(
            mockClient.exists(any(OperationContext.class), Mockito.eq(TEST_QUALITY_ASSERTION_URN)))
        .thenReturn(false);
    Mockito.when(
            mockClient.exists(any(OperationContext.class), Mockito.eq(TEST_SCHEMA_ASSERTION_URN)))
        .thenReturn(false);
    UpsertDataContractResolver resolver =
        new UpsertDataContractResolver(mockClient, mockGraphClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_CREATE_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Assert.assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    GraphClient mockGraphClient = Mockito.mock(GraphClient.class);
    UpsertDataContractResolver resolver =
        new UpsertDataContractResolver(mockClient, mockGraphClient);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_CREATE_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockClient, Mockito.times(0))
        .ingestProposal(any(OperationContext.class), Mockito.any());
  }

  @Test
  public void testGetEntityClientException() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    GraphClient mockGraphClient = Mockito.mock(GraphClient.class);
    Mockito.doThrow(RemoteInvocationException.class)
        .when(mockClient)
        .ingestProposal(any(OperationContext.class), Mockito.any(), Mockito.eq(false));
    UpsertDataContractResolver resolver =
        new UpsertDataContractResolver(mockClient, mockGraphClient);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_CREATE_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  private void initMockGraphClient(GraphClient client, Urn existingContractUrn) {
    if (existingContractUrn != null) {
      Mockito.when(
              client.getRelatedEntities(
                  Mockito.eq(TEST_DATASET_URN.toString()),
                  Mockito.eq(ImmutableList.of(CONTRACT_FOR_RELATIONSHIP)),
                  Mockito.eq(RelationshipDirection.INCOMING),
                  Mockito.eq(0),
                  Mockito.eq(1),
                  Mockito.anyString()))
          .thenReturn(
              new EntityRelationships()
                  .setTotal(1)
                  .setCount(1)
                  .setStart(0)
                  .setRelationships(
                      new EntityRelationshipArray(
                          ImmutableList.of(
                              new EntityRelationship()
                                  .setEntity(existingContractUrn)
                                  .setType(CONTRACT_FOR_RELATIONSHIP)
                                  .setCreated(
                                      new AuditStamp().setActor(TEST_ACTOR_URN).setTime(0L))))));
    } else {
      Mockito.when(
              client.getRelatedEntities(
                  Mockito.eq(TEST_DATASET_URN.toString()),
                  Mockito.eq(ImmutableList.of(CONTRACT_FOR_RELATIONSHIP)),
                  Mockito.eq(RelationshipDirection.INCOMING),
                  Mockito.eq(0),
                  Mockito.eq(1),
                  Mockito.anyString()))
          .thenReturn(
              new EntityRelationships()
                  .setTotal(0)
                  .setCount(0)
                  .setStart(0)
                  .setRelationships(new EntityRelationshipArray(Collections.emptyList())));
    }
  }

  private void initMockEntityClient(
      EntityClient client, Urn existingContractUrn, DataContractProperties newContractProperties)
      throws Exception {
    if (existingContractUrn != null) {
      Mockito.when(client.exists(any(OperationContext.class), Mockito.eq(existingContractUrn)))
          .thenReturn(true);
    }
    Mockito.when(client.exists(any(OperationContext.class), Mockito.eq(TEST_DATASET_URN)))
        .thenReturn(true);
    Mockito.when(client.exists(any(OperationContext.class), Mockito.eq(TEST_QUALITY_ASSERTION_URN)))
        .thenReturn(true);
    Mockito.when(
            client.exists(any(OperationContext.class), Mockito.eq(TEST_FRESHNESS_ASSERTION_URN)))
        .thenReturn(true);
    Mockito.when(client.exists(any(OperationContext.class), Mockito.eq(TEST_SCHEMA_ASSERTION_URN)))
        .thenReturn(true);

    Mockito.when(
            client.getV2(
                any(OperationContext.class),
                Mockito.eq(Constants.DATA_CONTRACT_ENTITY_NAME),
                Mockito.eq(TEST_CONTRACT_URN),
                Mockito.eq(null)))
        .thenReturn(
            new EntityResponse()
                .setUrn(TEST_CONTRACT_URN)
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            Constants.DATA_CONTRACT_PROPERTIES_ASPECT_NAME,
                            new EnvelopedAspect()
                                .setType(AspectType.VERSIONED)
                                .setName(Constants.DATA_CONTRACT_ENTITY_NAME)
                                .setValue(new Aspect(newContractProperties.data()))))));
  }
}
