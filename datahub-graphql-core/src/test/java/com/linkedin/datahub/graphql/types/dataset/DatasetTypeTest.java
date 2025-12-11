package com.linkedin.datahub.graphql.types.dataset;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.*;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.AspectMappingRegistry;
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
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.DataFetchingFieldSelectionSet;
import graphql.schema.SelectedField;
import java.util.Collections;
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
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    DataFetchingFieldSelectionSet mockSelectionSet =
        Mockito.mock(DataFetchingFieldSelectionSet.class);
    AspectMappingRegistry mockRegistry = Mockito.mock(AspectMappingRegistry.class);

    Urn datasetUrn = Urn.createFromString(TEST_DATASET_URN);
    List<SelectedField> fields = Collections.emptyList();
    Set<String> optimizedAspects = ImmutableSet.of("datasetKey", "datasetProperties");

    Mockito.when(mockContext.getDataFetchingEnvironment()).thenReturn(mockEnv);
    Mockito.when(mockContext.getAspectMappingRegistry()).thenReturn(mockRegistry);
    Mockito.when(mockEnv.getSelectionSet()).thenReturn(mockSelectionSet);
    Mockito.when(mockSelectionSet.getFields()).thenReturn(fields);
    Mockito.when(mockRegistry.getRequiredAspects("Dataset", fields)).thenReturn(optimizedAspects);

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
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    DataFetchingFieldSelectionSet mockSelectionSet =
        Mockito.mock(DataFetchingFieldSelectionSet.class);
    AspectMappingRegistry mockRegistry = Mockito.mock(AspectMappingRegistry.class);

    Urn datasetUrn = Urn.createFromString(TEST_DATASET_URN);
    List<SelectedField> fields = Collections.emptyList();

    Mockito.when(mockContext.getDataFetchingEnvironment()).thenReturn(mockEnv);
    Mockito.when(mockContext.getAspectMappingRegistry()).thenReturn(mockRegistry);
    Mockito.when(mockEnv.getSelectionSet()).thenReturn(mockSelectionSet);
    Mockito.when(mockSelectionSet.getFields()).thenReturn(fields);
    Mockito.when(mockRegistry.getRequiredAspects("Dataset", fields)).thenReturn(null);

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

    Mockito.when(mockContext.getDataFetchingEnvironment()).thenReturn(null);
    Mockito.when(mockContext.getAspectMappingRegistry()).thenReturn(null);

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
}
