package com.linkedin.datahub.graphql.resolvers.load;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;

import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.SemanticModel;
import com.linkedin.datahub.graphql.generated.SemanticModelInfo;
import com.linkedin.datahub.graphql.generated.SemanticModelProperties;
import com.linkedin.datahub.graphql.types.dataset.DatasetType;
import com.linkedin.datahub.graphql.types.semanticmodel.SemanticModelType;
import com.linkedin.entity.client.EntityClient;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.dataloader.DataLoader;
import org.dataloader.DataLoaderRegistry;
import org.testng.annotations.Test;

/**
 * Mirrors the key-provider lambdas wired in {@code
 * GmsGraphQLEngine#configureSemanticModelResolvers} for {@code SemanticModelInfo.datasets} and
 * {@code SemanticModelProperties.semanticModel}. Ensures stub entities produced by mappers are
 * hydrated via DataLoader rather than returned as urn+type only.
 */
public class SemanticModelTypeWiringResolverTest {

  private static final String DATASET_URN =
      "urn:li:dataset:(urn:li:dataPlatform:dbt,analytics.orders_model.orders_ds,PROD)";
  private static final String SEMANTIC_MODEL_URN =
      "urn:li:semanticModel:(urn:li:dataPlatform:dbt,analytics.orders_model,my_model)";

  @Test
  public void testSemanticModelInfoDatasetsBatchResolverHydratesDatasets() throws Exception {
    Dataset stub = new Dataset();
    stub.setUrn(DATASET_URN);
    stub.setType(EntityType.DATASET);

    SemanticModelInfo info = new SemanticModelInfo();
    info.setDatasets(Collections.singletonList(stub));

    Dataset hydrated = new Dataset();
    hydrated.setUrn(DATASET_URN);
    hydrated.setType(EntityType.DATASET);
    hydrated.setName("analytics.orders_model.orders_ds");
    SemanticModelProperties props = new SemanticModelProperties();
    props.setAlias("orders_ds");
    hydrated.setSemanticModelProperties(props);

    DatasetType datasetType = new DatasetType(mock(EntityClient.class));
    DataFetchingEnvironment env =
        environmentWithBatchLoader(datasetType.name(), info, Collections.singletonList(hydrated));

    LoadableTypeBatchResolver<Dataset, String> resolver =
        new LoadableTypeBatchResolver<>(
            datasetType,
            e ->
                ((SemanticModelInfo) e.getSource())
                    .getDatasets().stream().map(Dataset::getUrn).collect(Collectors.toList()));

    List<Dataset> result = resolver.get(env).get();
    assertEquals(result.size(), 1);
    assertEquals(result.get(0).getName(), "analytics.orders_model.orders_ds");
    assertNotNull(result.get(0).getSemanticModelProperties());
    assertEquals(result.get(0).getSemanticModelProperties().getAlias(), "orders_ds");
  }

  @Test
  public void testSemanticModelPropertiesSemanticModelResolverHydratesModel() throws Exception {
    SemanticModel stub = new SemanticModel();
    stub.setUrn(SEMANTIC_MODEL_URN);
    stub.setType(EntityType.SEMANTIC_MODEL);

    SemanticModelProperties props = new SemanticModelProperties();
    props.setAlias("orders_ds");
    props.setSemanticModel(stub);

    SemanticModel hydrated = new SemanticModel();
    hydrated.setUrn(SEMANTIC_MODEL_URN);
    hydrated.setType(EntityType.SEMANTIC_MODEL);
    hydrated.setPath("analytics.orders_model");
    hydrated.setId("my_model");

    SemanticModelType semanticModelType = new SemanticModelType(mock(EntityClient.class));
    DataFetchingEnvironment env = environmentWithLoader(semanticModelType.name(), props, hydrated);

    LoadableTypeResolver<SemanticModel, String> resolver =
        new LoadableTypeResolver<>(
            semanticModelType,
            e -> {
              final SemanticModelProperties smp = e.getSource();
              return smp.getSemanticModel() != null ? smp.getSemanticModel().getUrn() : null;
            });

    SemanticModel result = resolver.get(env).get();
    assertSame(result, hydrated);
    assertEquals(result.getPath(), "analytics.orders_model");
    assertEquals(result.getId(), "my_model");
  }

  @Test
  public void testSemanticModelPropertiesSemanticModelResolverNullWhenAbsent() throws Exception {
    SemanticModelProperties props = new SemanticModelProperties();
    props.setAlias("orders_ds");

    SemanticModelType semanticModelType = new SemanticModelType(mock(EntityClient.class));
    DataFetchingEnvironment env = environmentWithLoader(semanticModelType.name(), props, null);

    LoadableTypeResolver<SemanticModel, String> resolver =
        new LoadableTypeResolver<>(
            semanticModelType,
            e -> {
              final SemanticModelProperties smp = e.getSource();
              return smp.getSemanticModel() != null ? smp.getSemanticModel().getUrn() : null;
            });

    // Key provider returns null → resolver short-circuits without invoking the DataLoader.
    assertNull(resolver.get(env));
  }

  @SuppressWarnings("unchecked")
  private static DataFetchingEnvironment environmentWithBatchLoader(
      final String loaderName, final Object source, final List<?> loaderResult) {
    final DataLoader<String, Object> loader = mock(DataLoader.class);
    when(loader.loadMany(anyList()))
        .thenReturn(CompletableFuture.completedFuture((List<Object>) loaderResult));

    final DataLoaderRegistry registry = mock(DataLoaderRegistry.class);
    when(registry.getDataLoader(loaderName)).thenReturn((DataLoader) loader);

    final DataFetchingEnvironment env = mock(DataFetchingEnvironment.class);
    when(env.getSource()).thenReturn(source);
    when(env.getDataLoaderRegistry()).thenReturn(registry);
    return env;
  }

  @SuppressWarnings("unchecked")
  private static DataFetchingEnvironment environmentWithLoader(
      final String loaderName, final Object source, final Object loaderResult) {
    final DataLoader<String, Object> loader = mock(DataLoader.class);
    when(loader.load(anyString())).thenReturn(CompletableFuture.completedFuture(loaderResult));

    final DataLoaderRegistry registry = mock(DataLoaderRegistry.class);
    when(registry.getDataLoader(loaderName)).thenReturn((DataLoader) loader);

    final DataFetchingEnvironment env = mock(DataFetchingEnvironment.class);
    when(env.getSource()).thenReturn(source);
    when(env.getDataLoaderRegistry()).thenReturn(registry);
    return env;
  }
}
