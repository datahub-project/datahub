package com.linkedin.datahub.graphql.types.dataset;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.*;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.AspectLoadContext;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.DatasetKey;
import com.linkedin.r2.RemoteInvocationException;
import graphql.execution.DataFetcherResult;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class DatasetTypeTest {

  private static final String TEST_DATASET_URN =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,test.db,PROD)";
  private static final DatasetKey TEST_DATASET_KEY =
      new DatasetKey()
          .setPlatform(Urn.createFromTuple("dataPlatform", "mysql"))
          .setName("test.db")
          .setOrigin(com.linkedin.common.FabricType.PROD);
  private static final DatasetProperties TEST_DATASET_PROPERTIES =
      new DatasetProperties().setDescription("test description").setName("Test Dataset");

  @Test
  public void testBatchLoadWithOptimizedAspects() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    QueryContext mockContext = getMockAllowContext();

    Urn datasetUrn = Urn.createFromString(TEST_DATASET_URN);
    Set<String> optimizedAspects = ImmutableSet.of("datasetKey", "datasetProperties");

    Mockito.when(mockContext.getAspectLoadContext("Dataset"))
        .thenReturn(AspectLoadContext.of(optimizedAspects));

    Mockito.when(
            mockClient.batchGetV2(
                any(),
                Mockito.eq(Constants.DATASET_ENTITY_NAME),
                Mockito.eq(new HashSet<>(ImmutableSet.of(datasetUrn))),
                Mockito.eq(ImmutableSet.of("datasetKey", "datasetProperties"))))
        .thenReturn(
            ImmutableMap.of(
                datasetUrn,
                new EntityResponse()
                    .setEntityName(Constants.DATASET_ENTITY_NAME)
                    .setUrn(datasetUrn)
                    .setAspects(
                        new EnvelopedAspectMap(
                            ImmutableMap.of(
                                Constants.DATASET_KEY_ASPECT_NAME,
                                new EnvelopedAspect().setValue(new Aspect(TEST_DATASET_KEY.data())),
                                Constants.DATASET_PROPERTIES_ASPECT_NAME,
                                new EnvelopedAspect()
                                    .setValue(new Aspect(TEST_DATASET_PROPERTIES.data())))))));

    DatasetType type = new DatasetType(mockClient);
    List<DataFetcherResult<Dataset>> result =
        type.batchLoad(ImmutableList.of(TEST_DATASET_URN), mockContext);

    ArgumentCaptor<Set<String>> aspectsCaptor = ArgumentCaptor.forClass(Set.class);
    Mockito.verify(mockClient, Mockito.times(1))
        .batchGetV2(
            any(),
            Mockito.eq(Constants.DATASET_ENTITY_NAME),
            Mockito.eq(ImmutableSet.of(datasetUrn)),
            aspectsCaptor.capture());

    Set<String> capturedAspects = aspectsCaptor.getValue();
    assertEquals(capturedAspects.size(), 2);
    assertTrue(capturedAspects.contains("datasetKey"));
    assertTrue(capturedAspects.contains("datasetProperties"));

    assertEquals(result.size(), 1);
    Dataset dataset = result.get(0).getData();
    assertEquals(dataset.getUrn(), TEST_DATASET_URN);
    assertEquals(dataset.getType(), EntityType.DATASET);
  }

  @Test
  public void testBatchLoadFallsBackToAllAspects() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    QueryContext mockContext = getMockAllowContext();

    Urn datasetUrn = Urn.createFromString(TEST_DATASET_URN);

    Mockito.when(mockContext.getAspectLoadContext("Dataset"))
        .thenReturn(AspectLoadContext.fetchAll());

    Mockito.when(
            mockClient.batchGetV2(
                any(),
                Mockito.eq(Constants.DATASET_ENTITY_NAME),
                Mockito.eq(new HashSet<>(ImmutableSet.of(datasetUrn))),
                Mockito.eq(DatasetType.ASPECTS_TO_RESOLVE)))
        .thenReturn(
            ImmutableMap.of(
                datasetUrn,
                new EntityResponse()
                    .setEntityName(Constants.DATASET_ENTITY_NAME)
                    .setUrn(datasetUrn)
                    .setAspects(
                        new EnvelopedAspectMap(
                            ImmutableMap.of(
                                Constants.DATASET_KEY_ASPECT_NAME,
                                new EnvelopedAspect().setValue(new Aspect(TEST_DATASET_KEY.data())),
                                Constants.DATASET_PROPERTIES_ASPECT_NAME,
                                new EnvelopedAspect()
                                    .setValue(new Aspect(TEST_DATASET_PROPERTIES.data())))))));

    DatasetType type = new DatasetType(mockClient);
    List<DataFetcherResult<Dataset>> result =
        type.batchLoad(ImmutableList.of(TEST_DATASET_URN), mockContext);

    ArgumentCaptor<Set<String>> aspectsCaptor = ArgumentCaptor.forClass(Set.class);
    Mockito.verify(mockClient, Mockito.times(1))
        .batchGetV2(
            any(),
            Mockito.eq(Constants.DATASET_ENTITY_NAME),
            Mockito.eq(ImmutableSet.of(datasetUrn)),
            aspectsCaptor.capture());

    Set<String> capturedAspects = aspectsCaptor.getValue();
    assertEquals(capturedAspects, DatasetType.ASPECTS_TO_RESOLVE);

    assertEquals(result.size(), 1);
    Dataset dataset = result.get(0).getData();
    assertEquals(dataset.getUrn(), TEST_DATASET_URN);
    assertEquals(dataset.getType(), EntityType.DATASET);
  }

  @Test
  public void testBatchLoadFallsBackWhenContextIncomplete() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    QueryContext mockContext = getMockAllowContext();

    Urn datasetUrn = Urn.createFromString(TEST_DATASET_URN);

    Mockito.when(mockContext.getAspectLoadContext("Dataset")).thenReturn(null);

    Mockito.when(
            mockClient.batchGetV2(
                any(),
                Mockito.eq(Constants.DATASET_ENTITY_NAME),
                Mockito.eq(new HashSet<>(ImmutableSet.of(datasetUrn))),
                Mockito.eq(DatasetType.ASPECTS_TO_RESOLVE)))
        .thenReturn(
            ImmutableMap.of(
                datasetUrn,
                new EntityResponse()
                    .setEntityName(Constants.DATASET_ENTITY_NAME)
                    .setUrn(datasetUrn)
                    .setAspects(
                        new EnvelopedAspectMap(
                            ImmutableMap.of(
                                Constants.DATASET_KEY_ASPECT_NAME,
                                new EnvelopedAspect().setValue(new Aspect(TEST_DATASET_KEY.data())),
                                Constants.DATASET_PROPERTIES_ASPECT_NAME,
                                new EnvelopedAspect()
                                    .setValue(new Aspect(TEST_DATASET_PROPERTIES.data())))))));

    DatasetType type = new DatasetType(mockClient);
    List<DataFetcherResult<Dataset>> result =
        type.batchLoad(ImmutableList.of(TEST_DATASET_URN), mockContext);

    Mockito.verify(mockClient, Mockito.times(1))
        .batchGetV2(
            any(),
            Mockito.eq(Constants.DATASET_ENTITY_NAME),
            Mockito.eq(ImmutableSet.of(datasetUrn)),
            Mockito.eq(DatasetType.ASPECTS_TO_RESOLVE));

    assertEquals(result.size(), 1);
    Dataset dataset = result.get(0).getData();
    assertEquals(dataset.getUrn(), TEST_DATASET_URN);
    assertEquals(dataset.getType(), EntityType.DATASET);
  }

  @Test
  public void testBatchLoadClientException() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.doThrow(RemoteInvocationException.class)
        .when(mockClient)
        .batchGetV2(any(), Mockito.anyString(), Mockito.anySet(), Mockito.anySet());

    DatasetType type = new DatasetType(mockClient);
    QueryContext context = getMockAllowContext();

    assertThrows(
        RuntimeException.class, () -> type.batchLoad(ImmutableList.of(TEST_DATASET_URN), context));
  }

  /**
   * batchUpdate calls batchLoad directly, bypassing the DataLoader resolvers that contribute a
   * selection. If the request already accumulated a narrow Dataset aspect union, the mutation
   * response must not under-hydrate: batchUpdate widens to fetch-all first (like update() does via
   * LoadableType.load), so batchGetV2 receives the full default aspect set.
   */
  @Test
  public void testBatchUpdateWidensNarrowAspectContext() throws Exception {
    QueryContext mockContext = getMockAllowContext();

    // Simulate a narrow selection accumulated earlier in the request, honoring merges so
    // ensureFetchAllForDirectLoad can widen it.
    final AspectLoadContext[] accumulated = {
      AspectLoadContext.of(Set.of(Constants.OWNERSHIP_ASPECT_NAME))
    };
    Mockito.doAnswer(
            invocation -> {
              accumulated[0] = accumulated[0].union(invocation.getArgument(1));
              return null;
            })
        .when(mockContext)
        .mergeAspectLoadContext(Mockito.eq("Dataset"), any());
    Mockito.when(mockContext.getAspectLoadContext("Dataset"))
        .thenAnswer(invocation -> accumulated[0]);

    Urn datasetUrn = Urn.createFromString(TEST_DATASET_URN);
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.when(mockClient.batchGetV2(any(), Mockito.anyString(), Mockito.anySet(), any()))
        .thenReturn(
            ImmutableMap.of(
                datasetUrn,
                new EntityResponse()
                    .setEntityName(Constants.DATASET_ENTITY_NAME)
                    .setUrn(datasetUrn)
                    .setAspects(
                        new EnvelopedAspectMap(
                            ImmutableMap.of(
                                Constants.DATASET_KEY_ASPECT_NAME,
                                new EnvelopedAspect()
                                    .setValue(new Aspect(TEST_DATASET_KEY.data())))))));

    DatasetType type = new DatasetType(mockClient);
    com.linkedin.datahub.graphql.generated.BatchDatasetUpdateInput input =
        new com.linkedin.datahub.graphql.generated.BatchDatasetUpdateInput();
    input.setUrn(TEST_DATASET_URN);
    input.setUpdate(new com.linkedin.datahub.graphql.generated.DatasetUpdateInput());

    type.batchUpdate(
        new com.linkedin.datahub.graphql.generated.BatchDatasetUpdateInput[] {input}, mockContext);

    ArgumentCaptor<Set<String>> aspectsCaptor = ArgumentCaptor.forClass(Set.class);
    Mockito.verify(mockClient)
        .batchGetV2(any(), Mockito.anyString(), Mockito.anySet(), aspectsCaptor.capture());
    Set<String> fetched = aspectsCaptor.getValue();

    assertTrue(
        fetched.contains(Constants.UPSTREAM_LINEAGE_ASPECT_NAME),
        "batchUpdate must widen past the narrow {ownership} union; got: " + fetched);
    assertEquals(fetched, DatasetType.ASPECTS_TO_RESOLVE);
  }
}
