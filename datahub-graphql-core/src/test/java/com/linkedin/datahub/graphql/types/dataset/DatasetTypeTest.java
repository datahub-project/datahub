package com.linkedin.datahub.graphql.types.dataset;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DatasetUpdateInput;
import com.linkedin.domain.Domains;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class DatasetTypeTest {

  private static final String TEST_DATASET_URN =
      "urn:li:dataset:(urn:li:dataPlatform:kafka,test,PROD)";
  private static final String TEST_DOMAIN_URN = "urn:li:domain:test-domain";
  private static final String TEST_ACTOR_URN = "urn:li:corpuser:test";

  @Test
  public void testUpdateDatasetWithoutDomainOldAuthorizationPattern() throws Exception {
    // This test demonstrates the OLD authorization pattern (entity-level only)
    // Dataset has no domain, so only entity-level authorization is checked

    EntityClient mockClient = Mockito.mock(EntityClient.class);

    // Mock dataset exists and has no domain
    Mockito.when(
            mockClient.batchGetV2(
                any(),
                eq(Constants.DATASET_ENTITY_NAME),
                eq(new HashSet<>(ImmutableSet.of(Urn.createFromString(TEST_DATASET_URN)))),
                any()))
        .thenReturn(
            ImmutableMap.of(
                Urn.createFromString(TEST_DATASET_URN),
                new EntityResponse()
                    .setEntityName(Constants.DATASET_ENTITY_NAME)
                    .setUrn(Urn.createFromString(TEST_DATASET_URN))
                    .setAspects(new EnvelopedAspectMap(Collections.emptyMap()))));

    DatasetType datasetType = new DatasetType(mockClient);

    // Create update input (empty update to demonstrate authorization)
    DatasetUpdateInput input = new DatasetUpdateInput();

    // Setup context with ALLOW authorization (entity-level only)
    QueryContext mockContext = getMockAllowContext(TEST_ACTOR_URN);

    // Execute update - should succeed with entity-level authorization only
    datasetType.update(TEST_DATASET_URN, input, mockContext);

    // Verify proposals were ingested
    Mockito.verify(mockClient, Mockito.times(1)).batchIngestProposals(any(), any(), anyBoolean());
  }

  @Test
  public void testUpdateDatasetWithDomainNewAuthorizationPattern() throws Exception {
    // This test demonstrates the NEW authorization pattern (entity-level + domain-level)
    // Dataset belongs to a domain, so BOTH entity and domain authorization should be checked
    // NOTE: Current implementation does NOT do this - this test shows what SHOULD happen

    EntityClient mockClient = Mockito.mock(EntityClient.class);

    // Mock dataset exists WITH domain
    Domains existingDomains =
        new Domains()
            .setDomains(
                new com.linkedin.common.UrnArray(
                    ImmutableList.of(Urn.createFromString(TEST_DOMAIN_URN))));

    Mockito.when(
            mockClient.batchGetV2(
                any(),
                eq(Constants.DATASET_ENTITY_NAME),
                eq(new HashSet<>(ImmutableSet.of(Urn.createFromString(TEST_DATASET_URN)))),
                any()))
        .thenReturn(
            ImmutableMap.of(
                Urn.createFromString(TEST_DATASET_URN),
                new EntityResponse()
                    .setEntityName(Constants.DATASET_ENTITY_NAME)
                    .setUrn(Urn.createFromString(TEST_DATASET_URN))
                    .setAspects(
                        new EnvelopedAspectMap(
                            ImmutableMap.of(
                                Constants.DOMAINS_ASPECT_NAME,
                                new EnvelopedAspect()
                                    .setValue(new Aspect(existingDomains.data())))))));

    DatasetType datasetType = new DatasetType(mockClient);

    // Create update input (empty update to demonstrate authorization)
    DatasetUpdateInput input = new DatasetUpdateInput();

    // Setup context with ALLOW authorization (for BOTH entity and domain)
    QueryContext mockContext = getMockAllowContext(TEST_ACTOR_URN);

    // Execute update - currently succeeds with just entity-level auth
    // SHOULD check domain-based authorization too
    datasetType.update(TEST_DATASET_URN, input, mockContext);

    // Verify proposals were ingested
    Mockito.verify(mockClient, Mockito.times(1)).batchIngestProposals(any(), any(), anyBoolean());
  }

  @Test(expectedExceptions = Exception.class)
  public void testUpdateDatasetWithDomainAuthorizationDenied() throws Exception {
    // This test demonstrates authorization denial for dataset with domain
    // When user lacks domain permissions, update should fail

    EntityClient mockClient = Mockito.mock(EntityClient.class);

    // Mock dataset exists WITH domain
    Domains existingDomains =
        new Domains()
            .setDomains(
                new com.linkedin.common.UrnArray(
                    ImmutableList.of(Urn.createFromString(TEST_DOMAIN_URN))));

    Mockito.when(
            mockClient.batchGetV2(
                any(),
                eq(Constants.DATASET_ENTITY_NAME),
                eq(new HashSet<>(ImmutableSet.of(Urn.createFromString(TEST_DATASET_URN)))),
                any()))
        .thenReturn(
            ImmutableMap.of(
                Urn.createFromString(TEST_DATASET_URN),
                new EntityResponse()
                    .setEntityName(Constants.DATASET_ENTITY_NAME)
                    .setUrn(Urn.createFromString(TEST_DATASET_URN))
                    .setAspects(
                        new EnvelopedAspectMap(
                            ImmutableMap.of(
                                Constants.DOMAINS_ASPECT_NAME,
                                new EnvelopedAspect()
                                    .setValue(new Aspect(existingDomains.data())))))));

    DatasetType datasetType = new DatasetType(mockClient);

    // Create update input (empty update to demonstrate authorization)
    DatasetUpdateInput input = new DatasetUpdateInput();

    // Setup context with DENY authorization
    QueryContext mockContext = getMockDenyContext(TEST_ACTOR_URN);

    // Execute update - should fail due to authorization denial
    datasetType.update(TEST_DATASET_URN, input, mockContext);

    // Should not reach here - exception expected above
    fail("Expected authorization exception");
  }

  @Test
  public void testBatchUpdateDatasetOldAuthorizationPattern() throws Exception {
    // This test demonstrates the OLD authorization pattern for batch updates
    // Multiple datasets without domains, entity-level authorization only

    EntityClient mockClient = Mockito.mock(EntityClient.class);

    String dataset1Urn = "urn:li:dataset:(urn:li:dataPlatform:kafka,test1,PROD)";
    String dataset2Urn = "urn:li:dataset:(urn:li:dataPlatform:kafka,test2,PROD)";

    // Mock datasets exist without domains
    Mockito.when(mockClient.batchGetV2(any(), eq(Constants.DATASET_ENTITY_NAME), any(), any()))
        .thenReturn(
            ImmutableMap.of(
                Urn.createFromString(dataset1Urn),
                new EntityResponse()
                    .setEntityName(Constants.DATASET_ENTITY_NAME)
                    .setUrn(Urn.createFromString(dataset1Urn))
                    .setAspects(new EnvelopedAspectMap(Collections.emptyMap())),
                Urn.createFromString(dataset2Urn),
                new EntityResponse()
                    .setEntityName(Constants.DATASET_ENTITY_NAME)
                    .setUrn(Urn.createFromString(dataset2Urn))
                    .setAspects(new EnvelopedAspectMap(Collections.emptyMap()))));

    DatasetType datasetType = new DatasetType(mockClient);

    // Create batch update input
    com.linkedin.datahub.graphql.generated.BatchDatasetUpdateInput[] input =
        new com.linkedin.datahub.graphql.generated.BatchDatasetUpdateInput[2];

    com.linkedin.datahub.graphql.generated.BatchDatasetUpdateInput update1 =
        new com.linkedin.datahub.graphql.generated.BatchDatasetUpdateInput();
    update1.setUrn(dataset1Urn);
    DatasetUpdateInput updateInput1 = new DatasetUpdateInput();
    update1.setUpdate(updateInput1);

    com.linkedin.datahub.graphql.generated.BatchDatasetUpdateInput update2 =
        new com.linkedin.datahub.graphql.generated.BatchDatasetUpdateInput();
    update2.setUrn(dataset2Urn);
    DatasetUpdateInput updateInput2 = new DatasetUpdateInput();
    update2.setUpdate(updateInput2);

    input[0] = update1;
    input[1] = update2;

    // Setup context with ALLOW authorization
    QueryContext mockContext = getMockAllowContext(TEST_ACTOR_URN);

    // Execute batch update - should succeed with entity-level authorization only
    List<com.linkedin.datahub.graphql.generated.Dataset> results =
        datasetType.batchUpdate(input, mockContext);

    // Verify proposals were ingested
    Mockito.verify(mockClient, Mockito.times(1)).batchIngestProposals(any(), any(), anyBoolean());
  }

  @Test
  public void testBatchUpdateDatasetWithDomainsNewAuthorizationPattern() throws Exception {
    // This test demonstrates the NEW authorization pattern for batch updates with domains
    // Multiple datasets WITH domains, should check both entity and domain authorization
    // NOTE: Current implementation does NOT do this - this test shows what SHOULD happen

    EntityClient mockClient = Mockito.mock(EntityClient.class);

    String dataset1Urn = "urn:li:dataset:(urn:li:dataPlatform:kafka,test1,PROD)";
    String dataset2Urn = "urn:li:dataset:(urn:li:dataPlatform:kafka,test2,PROD)";
    String domain1Urn = "urn:li:domain:domain1";
    String domain2Urn = "urn:li:domain:domain2";

    // Mock datasets exist WITH different domains
    Domains domain1 =
        new Domains()
            .setDomains(
                new com.linkedin.common.UrnArray(
                    ImmutableList.of(Urn.createFromString(domain1Urn))));
    Domains domain2 =
        new Domains()
            .setDomains(
                new com.linkedin.common.UrnArray(
                    ImmutableList.of(Urn.createFromString(domain2Urn))));

    Mockito.when(mockClient.batchGetV2(any(), eq(Constants.DATASET_ENTITY_NAME), any(), any()))
        .thenReturn(
            ImmutableMap.of(
                Urn.createFromString(dataset1Urn),
                new EntityResponse()
                    .setEntityName(Constants.DATASET_ENTITY_NAME)
                    .setUrn(Urn.createFromString(dataset1Urn))
                    .setAspects(
                        new EnvelopedAspectMap(
                            ImmutableMap.of(
                                Constants.DOMAINS_ASPECT_NAME,
                                new EnvelopedAspect().setValue(new Aspect(domain1.data()))))),
                Urn.createFromString(dataset2Urn),
                new EntityResponse()
                    .setEntityName(Constants.DATASET_ENTITY_NAME)
                    .setUrn(Urn.createFromString(dataset2Urn))
                    .setAspects(
                        new EnvelopedAspectMap(
                            ImmutableMap.of(
                                Constants.DOMAINS_ASPECT_NAME,
                                new EnvelopedAspect().setValue(new Aspect(domain2.data())))))));

    DatasetType datasetType = new DatasetType(mockClient);

    // Create batch update input
    com.linkedin.datahub.graphql.generated.BatchDatasetUpdateInput[] input =
        new com.linkedin.datahub.graphql.generated.BatchDatasetUpdateInput[2];

    com.linkedin.datahub.graphql.generated.BatchDatasetUpdateInput update1 =
        new com.linkedin.datahub.graphql.generated.BatchDatasetUpdateInput();
    update1.setUrn(dataset1Urn);
    DatasetUpdateInput updateInput1 = new DatasetUpdateInput();
    update1.setUpdate(updateInput1);

    com.linkedin.datahub.graphql.generated.BatchDatasetUpdateInput update2 =
        new com.linkedin.datahub.graphql.generated.BatchDatasetUpdateInput();
    update2.setUrn(dataset2Urn);
    DatasetUpdateInput updateInput2 = new DatasetUpdateInput();
    update2.setUpdate(updateInput2);

    input[0] = update1;
    input[1] = update2;

    // Setup context with ALLOW authorization (for BOTH entities and their domains)
    QueryContext mockContext = getMockAllowContext(TEST_ACTOR_URN);

    // Execute batch update - currently succeeds with just entity-level auth
    // SHOULD check domain-based authorization for each dataset's domain
    List<com.linkedin.datahub.graphql.generated.Dataset> results =
        datasetType.batchUpdate(input, mockContext);

    // Verify proposals were ingested
    Mockito.verify(mockClient, Mockito.times(1)).batchIngestProposals(any(), any(), anyBoolean());
  }

  @Test(expectedExceptions = Exception.class)
  public void testBatchUpdateDatasetWithDomainsAuthorizationDenied() throws Exception {
    // This test demonstrates authorization denial for batch update with domains
    // When user lacks domain permissions for any dataset, entire batch should fail

    EntityClient mockClient = Mockito.mock(EntityClient.class);

    String dataset1Urn = "urn:li:dataset:(urn:li:dataPlatform:kafka,test1,PROD)";
    String dataset2Urn = "urn:li:dataset:(urn:li:dataPlatform:kafka,test2,PROD)";

    // Mock datasets exist WITH domains
    Domains domain1 =
        new Domains()
            .setDomains(
                new com.linkedin.common.UrnArray(
                    ImmutableList.of(Urn.createFromString(TEST_DOMAIN_URN))));

    Mockito.when(mockClient.batchGetV2(any(), eq(Constants.DATASET_ENTITY_NAME), any(), any()))
        .thenReturn(
            ImmutableMap.of(
                Urn.createFromString(dataset1Urn),
                new EntityResponse()
                    .setEntityName(Constants.DATASET_ENTITY_NAME)
                    .setUrn(Urn.createFromString(dataset1Urn))
                    .setAspects(
                        new EnvelopedAspectMap(
                            ImmutableMap.of(
                                Constants.DOMAINS_ASPECT_NAME,
                                new EnvelopedAspect().setValue(new Aspect(domain1.data()))))),
                Urn.createFromString(dataset2Urn),
                new EntityResponse()
                    .setEntityName(Constants.DATASET_ENTITY_NAME)
                    .setUrn(Urn.createFromString(dataset2Urn))
                    .setAspects(new EnvelopedAspectMap(Collections.emptyMap()))));

    DatasetType datasetType = new DatasetType(mockClient);

    // Create batch update input
    com.linkedin.datahub.graphql.generated.BatchDatasetUpdateInput[] input =
        new com.linkedin.datahub.graphql.generated.BatchDatasetUpdateInput[2];

    com.linkedin.datahub.graphql.generated.BatchDatasetUpdateInput update1 =
        new com.linkedin.datahub.graphql.generated.BatchDatasetUpdateInput();
    update1.setUrn(dataset1Urn);
    DatasetUpdateInput updateInput1 = new DatasetUpdateInput();
    update1.setUpdate(updateInput1);

    com.linkedin.datahub.graphql.generated.BatchDatasetUpdateInput update2 =
        new com.linkedin.datahub.graphql.generated.BatchDatasetUpdateInput();
    update2.setUrn(dataset2Urn);
    DatasetUpdateInput updateInput2 = new DatasetUpdateInput();
    update2.setUpdate(updateInput2);

    input[0] = update1;
    input[1] = update2;

    // Setup context with DENY authorization
    QueryContext mockContext = getMockDenyContext(TEST_ACTOR_URN);

    // Execute batch update - should fail due to authorization denial
    datasetType.batchUpdate(input, mockContext);

    // Should not reach here - exception expected above
    fail("Expected authorization exception");
  }
}
