package com.linkedin.datahub.graphql.resolvers.datacontract;

import static com.linkedin.datahub.graphql.resolvers.datacontract.EntityDataContractResolver.*;
import static org.mockito.ArgumentMatchers.nullable;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.EntityRelationship;
import com.linkedin.common.EntityRelationshipArray;
import com.linkedin.common.EntityRelationships;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datacontract.DataContractProperties;
import com.linkedin.datacontract.DataContractState;
import com.linkedin.datacontract.DataContractStatus;
import com.linkedin.datacontract.DataQualityContract;
import com.linkedin.datacontract.DataQualityContractArray;
import com.linkedin.datacontract.FreshnessContract;
import com.linkedin.datacontract.FreshnessContractArray;
import com.linkedin.datacontract.SchemaContract;
import com.linkedin.datacontract.SchemaContractArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataContract;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.key.DataContractKey;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class EntityDataContractResolverTest {

  private static final Urn TEST_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,test,PROD)");
  private static final Urn TEST_DATA_CONTRACT_URN = UrnUtils.getUrn("urn:li:dataContract:test");
  private static final Urn TEST_QUALITY_ASSERTION_URN = UrnUtils.getUrn("urn:li:assertion:quality");
  private static final Urn TEST_FRESHNESS_ASSERTION_URN =
      UrnUtils.getUrn("urn:li:assertion:freshness");
  private static final Urn TEST_SCHEMA_ASSERTION_URN = UrnUtils.getUrn("urn:li:assertion:schema");

  @Test
  public void testGetSuccessOneContract() throws Exception {
    GraphClient mockGraphClient = Mockito.mock(GraphClient.class);
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    Mockito.when(
            mockGraphClient.getRelatedEntities(
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
                                .setType(CONTRACT_FOR_RELATIONSHIP)
                                .setEntity(TEST_DATA_CONTRACT_URN)
                                .setCreated(
                                    new AuditStamp()
                                        .setActor(UrnUtils.getUrn("urn:li:corpuser:test"))
                                        .setTime(0L))))));

    Map<String, EnvelopedAspect> dataContractAspects = new HashMap<>();

    // 1. Key Aspect
    dataContractAspects.put(
        Constants.DATA_CONTRACT_KEY_ASPECT_NAME,
        new com.linkedin.entity.EnvelopedAspect()
            .setValue(new Aspect(new DataContractKey().setId("test").data())));

    // 2. Properties Aspect.
    DataContractProperties expectedProperties =
        new DataContractProperties()
            .setEntity(TEST_DATASET_URN)
            .setDataQuality(
                new DataQualityContractArray(
                    ImmutableList.of(
                        new DataQualityContract().setAssertion(TEST_QUALITY_ASSERTION_URN))))
            .setFreshness(
                new FreshnessContractArray(
                    ImmutableList.of(
                        new FreshnessContract().setAssertion(TEST_FRESHNESS_ASSERTION_URN))))
            .setSchema(
                new SchemaContractArray(
                    ImmutableList.of(
                        new SchemaContract().setAssertion(TEST_SCHEMA_ASSERTION_URN))));

    dataContractAspects.put(
        Constants.DATA_CONTRACT_PROPERTIES_ASPECT_NAME,
        new com.linkedin.entity.EnvelopedAspect().setValue(new Aspect(expectedProperties.data())));

    // 3. Status Aspect
    DataContractStatus expectedStatus = new DataContractStatus().setState(DataContractState.ACTIVE);

    dataContractAspects.put(
        Constants.DATA_CONTRACT_STATUS_ASPECT_NAME,
        new com.linkedin.entity.EnvelopedAspect().setValue(new Aspect(expectedStatus.data())));

    Mockito.when(
            mockClient.getV2(
                nullable(OperationContext.class),
                Mockito.eq(Constants.DATA_CONTRACT_ENTITY_NAME),
                Mockito.eq(TEST_DATA_CONTRACT_URN),
                Mockito.eq(null)))
        .thenReturn(
            new EntityResponse()
                .setEntityName(Constants.DATA_CONTRACT_ENTITY_NAME)
                .setUrn(TEST_DATA_CONTRACT_URN)
                .setAspects(new EnvelopedAspectMap(dataContractAspects)));

    // Execute resolver
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getActorUrn()).thenReturn("urn:li:corpuser:test");
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Dataset parentDataset = new Dataset();
    parentDataset.setUrn(TEST_DATASET_URN.toString());
    Mockito.when(mockEnv.getSource()).thenReturn(parentDataset);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    EntityDataContractResolver resolver =
        new EntityDataContractResolver(mockClient, mockGraphClient);
    DataContract result = resolver.get(mockEnv).get();

    // Assert that the result we get matches the expectations.
    assertEquals(result.getUrn(), TEST_DATA_CONTRACT_URN.toString());
    assertEquals(result.getType(), EntityType.DATA_CONTRACT);

    // Verify Properties
    assertEquals(result.getProperties().getDataQuality().size(), 1);
    assertEquals(result.getProperties().getFreshness().size(), 1);
    assertEquals(result.getProperties().getSchema().size(), 1);
    assertEquals(
        result.getProperties().getDataQuality().get(0).getAssertion().getUrn(),
        TEST_QUALITY_ASSERTION_URN.toString());
    assertEquals(
        result.getProperties().getFreshness().get(0).getAssertion().getUrn(),
        TEST_FRESHNESS_ASSERTION_URN.toString());
    assertEquals(
        result.getProperties().getSchema().get(0).getAssertion().getUrn(),
        TEST_SCHEMA_ASSERTION_URN.toString());

    // Verify Status
    assertEquals(result.getStatus().getState().toString(), expectedStatus.getState().toString());
  }

  @Test
  public void testGetSuccessNoContracts() throws Exception {
    GraphClient mockGraphClient = Mockito.mock(GraphClient.class);
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    Mockito.when(
            mockGraphClient.getRelatedEntities(
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

    EntityDataContractResolver resolver =
        new EntityDataContractResolver(mockClient, mockGraphClient);

    // Execute resolver
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getActorUrn()).thenReturn("urn:li:corpuser:test");
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Dataset parentDataset = new Dataset();
    parentDataset.setUrn(TEST_DATASET_URN.toString());
    Mockito.when(mockEnv.getSource()).thenReturn(parentDataset);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    DataContract result = resolver.get(mockEnv).get();

    assertNull(result);
    Mockito.verifyNoMoreInteractions(mockClient);
  }
}
