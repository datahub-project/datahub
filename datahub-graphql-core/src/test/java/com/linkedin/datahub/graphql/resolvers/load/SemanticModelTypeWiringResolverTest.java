package com.linkedin.datahub.graphql.resolvers.load;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.SemanticModel;
import com.linkedin.datahub.graphql.generated.SemanticModelProperties;
import com.linkedin.datahub.graphql.types.semanticmodel.SemanticModelType;
import com.linkedin.entity.client.EntityClient;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import org.dataloader.DataLoader;
import org.dataloader.DataLoaderRegistry;
import org.testng.annotations.Test;

/**
 * Mirrors the key-provider lambda wired in {@code GmsGraphQLEngine#configureSemanticModelResolvers}
 * for {@code SemanticModelProperties.semanticModel}. Ensures the stub URN produced by {@code
 * SemanticModelPropertiesMapper} is hydrated via DataLoader.
 */
public class SemanticModelTypeWiringResolverTest {

  private static final String SEMANTIC_MODEL_URN =
      "urn:li:semanticModel:(urn:li:dataPlatform:dbt,analytics.orders_model,my_model)";

  @Test
  public void testSemanticModelPropertiesResolverHydratesSemanticModel() throws Exception {
    SemanticModel stub = new SemanticModel();
    stub.setUrn(SEMANTIC_MODEL_URN);
    stub.setType(EntityType.SEMANTIC_MODEL);

    SemanticModelProperties props = new SemanticModelProperties();
    props.setAlias("orders_ds");
    props.setSemanticModel(stub);

    SemanticModel hydrated = new SemanticModel();
    hydrated.setUrn(SEMANTIC_MODEL_URN);
    hydrated.setType(EntityType.SEMANTIC_MODEL);
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
    assertNotNull(result);
    assertEquals(result.getId(), "my_model");
    assertEquals(result.getUrn(), SEMANTIC_MODEL_URN);
  }

  @SuppressWarnings("unchecked")
  private static DataFetchingEnvironment environmentWithLoader(
      final String loaderName, final Object source, final Object loaderResult) {
    final DataLoader<String, Object> loader = mock(DataLoader.class);
    when(loader.load(any())).thenReturn(CompletableFuture.completedFuture(loaderResult));

    final DataLoaderRegistry registry = mock(DataLoaderRegistry.class);
    when(registry.getDataLoader(loaderName)).thenReturn((DataLoader) loader);

    final DataFetchingEnvironment env = mock(DataFetchingEnvironment.class);
    when(env.getSource()).thenReturn(source);
    when(env.getDataLoaderRegistry()).thenReturn(registry);
    return env;
  }
}
