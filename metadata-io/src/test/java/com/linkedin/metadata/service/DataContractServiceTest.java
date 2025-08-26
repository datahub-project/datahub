package com.linkedin.metadata.service;

import static org.mockito.Mockito.mock;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.EntityRelationship;
import com.linkedin.common.EntityRelationshipArray;
import com.linkedin.common.EntityRelationships;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datacontract.DataContractProperties;
import com.linkedin.datacontract.DataQualityContract;
import com.linkedin.datacontract.DataQualityContractArray;
import com.linkedin.datacontract.FreshnessContract;
import com.linkedin.datacontract.FreshnessContractArray;
import com.linkedin.datacontract.SchemaContract;
import com.linkedin.datacontract.SchemaContractArray;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.openapi.client.OpenApiClient;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collections;
import java.util.List;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class DataContractServiceTest {

  private static final Urn TEST_CONTRACT_URN = UrnUtils.getUrn("urn:li:dataContract:test-contract");

  private static final Urn TEST_ACTOR_URN = UrnUtils.getUrn("urn:li:corpuser:test");
  private static final Urn TEST_FRESHNESS_ASSERTION_URN =
      UrnUtils.getUrn("urn:li:assertion:test-dataset-freshness");

  private static final Urn TEST_VOLUME_ASSERTION_URN =
      UrnUtils.getUrn("urn:li:assertion:test-dataset-volume");

  private static final Urn TEST_SCHEMA_ASSERTION_URN =
      UrnUtils.getUrn("urn:li:assertion:test-schema");

  private static final Urn TEST_FIELD_ASSERTION_URN =
      UrnUtils.getUrn("urn:li:assertion:test-dataset-field");
  private static final Urn TEST_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,name,PROD)");

  private static final ObjectMapper objectMapper = new ObjectMapper();

  private OperationContext opContext;

  @BeforeTest
  public void setup() {
    opContext = TestOperationContexts.userContextNoSearchAuthorization(TEST_ACTOR_URN);
  }

  @Test
  private void testGetEntityContractUrn() throws Exception {
    SystemEntityClient mockClient = mock(SystemEntityClient.class);
    GraphClient mockGraphClient = mock(GraphClient.class);

    Mockito.when(
            mockGraphClient.getRelatedEntities(
                Mockito.eq(TEST_DATASET_URN.toString()),
                Mockito.eq(ImmutableSet.of("ContractFor")),
                Mockito.eq(RelationshipDirection.INCOMING),
                Mockito.eq(0),
                Mockito.eq(1),
                Mockito.anyString()))
        .thenReturn(
            new EntityRelationships()
                .setTotal(1)
                .setRelationships(
                    new EntityRelationshipArray(
                        ImmutableList.of(new EntityRelationship().setEntity(TEST_CONTRACT_URN)))));

    // Exists
    Mockito.when(
            mockClient.exists(
                Mockito.any(OperationContext.class),
                Mockito.eq(TEST_CONTRACT_URN),
                Mockito.eq(false)))
        .thenReturn(true);

    DataContractService dataContractService =
        new DataContractService(
            mockClient, mockGraphClient, mock(OpenApiClient.class), objectMapper);

    Assert.assertEquals(
        dataContractService.getEntityContractUrn(opContext, TEST_DATASET_URN), TEST_CONTRACT_URN);
  }

  @Test
  private void testGetEntityContractUrnNoContracts() throws Exception {
    SystemEntityClient mockClient = mock(SystemEntityClient.class);
    GraphClient mockGraphClient = mock(GraphClient.class);

    Mockito.when(
            mockGraphClient.getRelatedEntities(
                Mockito.eq(TEST_DATASET_URN.toString()),
                Mockito.eq(ImmutableSet.of("ContractFor")),
                Mockito.eq(RelationshipDirection.INCOMING),
                Mockito.eq(0),
                Mockito.eq(1),
                Mockito.anyString()))
        .thenReturn(
            new EntityRelationships()
                .setTotal(1)
                .setRelationships(new EntityRelationshipArray(Collections.emptyList())));

    DataContractService dataContractService =
        new DataContractService(
            mockClient, mockGraphClient, mock(OpenApiClient.class), objectMapper);

    Assert.assertNull(dataContractService.getEntityContractUrn(opContext, TEST_DATASET_URN));
  }

  @Test
  private void testGetEntityContractUrnContractDoesNotExist() throws Exception {
    SystemEntityClient mockClient = mock(SystemEntityClient.class);
    GraphClient mockGraphClient = mock(GraphClient.class);

    Mockito.when(
            mockGraphClient.getRelatedEntities(
                Mockito.eq(TEST_DATASET_URN.toString()),
                Mockito.eq(ImmutableSet.of("ContractFor")),
                Mockito.eq(RelationshipDirection.INCOMING),
                Mockito.eq(0),
                Mockito.eq(1),
                Mockito.anyString()))
        .thenReturn(
            new EntityRelationships()
                .setTotal(1)
                .setRelationships(
                    new EntityRelationshipArray(
                        ImmutableList.of(new EntityRelationship().setEntity(TEST_CONTRACT_URN)))));

    // Does not exist
    Mockito.when(
            mockClient.exists(
                Mockito.any(OperationContext.class),
                Mockito.eq(TEST_CONTRACT_URN),
                Mockito.eq(false)))
        .thenReturn(false);

    DataContractService dataContractService =
        new DataContractService(
            mockClient, mockGraphClient, mock(OpenApiClient.class), objectMapper);

    Assert.assertNull(dataContractService.getEntityContractUrn(opContext, TEST_DATASET_URN));
  }

  @Test
  private void testGetDataContractProperties() throws Exception {
    SystemEntityClient mockClient = mock(SystemEntityClient.class);
    GraphClient mockGraphClient = mock(GraphClient.class);

    DataContractProperties properties = new DataContractProperties();
    properties.setEntity(TEST_DATASET_URN);
    properties.setFreshness(
        new FreshnessContractArray(
            ImmutableList.of(new FreshnessContract().setAssertion(TEST_FRESHNESS_ASSERTION_URN))));
    properties.setDataQuality(
        new DataQualityContractArray(
            ImmutableList.of(new DataQualityContract().setAssertion(TEST_VOLUME_ASSERTION_URN))));
    properties.setSchema(
        new SchemaContractArray(
            ImmutableList.of(new SchemaContract().setAssertion(TEST_SCHEMA_ASSERTION_URN))));

    Mockito.when(
            mockClient.getV2(
                Mockito.any(OperationContext.class),
                Mockito.eq(Constants.DATA_CONTRACT_ENTITY_NAME),
                Mockito.eq(TEST_CONTRACT_URN),
                Mockito.eq(null)))
        .thenReturn(
            new EntityResponse()
                .setEntityName(Constants.DATA_CONTRACT_ENTITY_NAME)
                .setUrn(TEST_CONTRACT_URN)
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            Constants.DATA_CONTRACT_PROPERTIES_ASPECT_NAME,
                            new EnvelopedAspect()
                                .setName(Constants.DATA_CONTRACT_PROPERTIES_ASPECT_NAME)
                                .setValue(new Aspect(properties.data()))))));

    DataContractService dataContractService =
        new DataContractService(
            mockClient, mockGraphClient, mock(OpenApiClient.class), objectMapper);

    Assert.assertEquals(
        dataContractService.getDataContractProperties(opContext, TEST_CONTRACT_URN), properties);
  }

  @Test
  private void testGetDataContractPropertiesNull() throws Exception {
    SystemEntityClient mockClient = mock(SystemEntityClient.class);
    GraphClient mockGraphClient = mock(GraphClient.class);

    Mockito.when(
            mockClient.getV2(
                Mockito.any(OperationContext.class),
                Mockito.eq(Constants.DATA_CONTRACT_ENTITY_NAME),
                Mockito.eq(TEST_CONTRACT_URN),
                Mockito.eq(null)))
        .thenReturn(
            new EntityResponse()
                .setEntityName(Constants.DATA_CONTRACT_ENTITY_NAME)
                .setUrn(TEST_CONTRACT_URN)
                .setAspects(new EnvelopedAspectMap(Collections.emptyMap())));

    DataContractService dataContractService =
        new DataContractService(
            mockClient, mockGraphClient, mock(OpenApiClient.class), objectMapper);

    Assert.assertNull(dataContractService.getDataContractProperties(opContext, TEST_CONTRACT_URN));
  }

  @Test
  private void testGetDataContractAssertionUrns() throws Exception {
    SystemEntityClient mockClient = mock(SystemEntityClient.class);
    GraphClient mockGraphClient = mock(GraphClient.class);

    DataContractProperties properties = new DataContractProperties();
    properties.setEntity(TEST_DATASET_URN);
    properties.setFreshness(
        new FreshnessContractArray(
            ImmutableList.of(new FreshnessContract().setAssertion(TEST_FRESHNESS_ASSERTION_URN))));
    properties.setDataQuality(
        new DataQualityContractArray(
            ImmutableList.of(
                new DataQualityContract().setAssertion(TEST_VOLUME_ASSERTION_URN),
                new DataQualityContract().setAssertion(TEST_FIELD_ASSERTION_URN))));
    properties.setSchema(
        new SchemaContractArray(
            ImmutableList.of(new SchemaContract().setAssertion(TEST_SCHEMA_ASSERTION_URN))));

    Mockito.when(
            mockClient.getV2(
                Mockito.any(OperationContext.class),
                Mockito.eq(Constants.DATA_CONTRACT_ENTITY_NAME),
                Mockito.eq(TEST_CONTRACT_URN),
                Mockito.eq(null)))
        .thenReturn(
            new EntityResponse()
                .setEntityName(Constants.DATA_CONTRACT_ENTITY_NAME)
                .setUrn(TEST_CONTRACT_URN)
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            Constants.DATA_CONTRACT_PROPERTIES_ASPECT_NAME,
                            new EnvelopedAspect()
                                .setName(Constants.DATA_CONTRACT_PROPERTIES_ASPECT_NAME)
                                .setValue(new Aspect(properties.data()))))));

    DataContractService dataContractService =
        new DataContractService(
            mockClient, mockGraphClient, mock(OpenApiClient.class), objectMapper);

    List<Urn> assertionUrns =
        dataContractService.getDataContractAssertionUrns(opContext, TEST_CONTRACT_URN);
    Assert.assertEquals(assertionUrns.size(), 4);
    Assert.assertTrue(assertionUrns.contains(TEST_FRESHNESS_ASSERTION_URN));
    Assert.assertTrue(assertionUrns.contains(TEST_VOLUME_ASSERTION_URN));
    Assert.assertTrue(assertionUrns.contains(TEST_FIELD_ASSERTION_URN));
    Assert.assertTrue(assertionUrns.contains(TEST_SCHEMA_ASSERTION_URN));
  }

  @Test
  private void testGetDataContractAssertionsNull() throws Exception {
    SystemEntityClient mockClient = mock(SystemEntityClient.class);
    GraphClient mockGraphClient = mock(GraphClient.class);

    Mockito.when(
            mockClient.getV2(
                Mockito.any(OperationContext.class),
                Mockito.eq(Constants.DATA_CONTRACT_ENTITY_NAME),
                Mockito.eq(TEST_CONTRACT_URN),
                Mockito.eq(null)))
        .thenReturn(
            new EntityResponse()
                .setEntityName(Constants.DATA_CONTRACT_ENTITY_NAME)
                .setUrn(TEST_CONTRACT_URN)
                .setAspects(new EnvelopedAspectMap(Collections.emptyMap())));

    DataContractService dataContractService =
        new DataContractService(
            mockClient, mockGraphClient, mock(OpenApiClient.class), objectMapper);

    Assert.assertNull(
        dataContractService.getDataContractAssertionUrns(opContext, TEST_CONTRACT_URN));
  }
}
