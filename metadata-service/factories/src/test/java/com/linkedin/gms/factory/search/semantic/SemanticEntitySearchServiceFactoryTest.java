package com.linkedin.gms.factory.search.semantic;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.search.SearchClusterRegistry;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.EmbeddingProviderConfiguration;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.ModelEmbeddingConfig;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder;
import com.linkedin.metadata.search.embedding.EmbeddingProvider;
import com.linkedin.metadata.search.semantic.SemanticEntitySearchService;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.mockito.MockedConstruction;
import org.testng.annotations.Test;

public class SemanticEntitySearchServiceFactoryTest {

  // The key the factory derives when no embedding provider is configured.
  private static final String DEFAULT_MODEL_KEY = "text_embedding_3_large";

  @Test
  public void passesActiveModelDimensionToService() throws Exception {
    assertEquals(dimensionPassedToService(semanticSearch(true, DEFAULT_MODEL_KEY, 1024)), 1024);

    // A configured provider's model id selects its own models entry.
    EmbeddingProviderConfiguration local = new EmbeddingProviderConfiguration();
    local.setType("local");
    local.getLocal().setModel("nomic-embed-text");
    SemanticSearchConfiguration localSearch = semanticSearch(true, "nomic_embed_text", 768);
    localSearch.setEmbeddingProvider(local);
    assertEquals(dimensionPassedToService(localSearch), 768);
  }

  @Test
  public void disablesDimensionCheckWhenNoDimensionIsConfigured() throws Exception {
    assertEquals(dimensionPassedToService(null), 0);
    assertEquals(dimensionPassedToService(semanticSearch(false, DEFAULT_MODEL_KEY, 1024)), 0);
    assertEquals(dimensionPassedToService(semanticSearch(true, "other_model", 1024)), 0);
    assertEquals(dimensionPassedToService(semanticSearch(true, DEFAULT_MODEL_KEY, -1)), 0);
  }

  private static SemanticSearchConfiguration semanticSearch(
      boolean enabled, String modelKey, int vectorDimension) {
    ModelEmbeddingConfig model = new ModelEmbeddingConfig();
    model.setVectorDimension(vectorDimension);
    SemanticSearchConfiguration semanticSearch = new SemanticSearchConfiguration();
    semanticSearch.setEnabled(enabled);
    semanticSearch.setModels(Map.of(modelKey, model));
    return semanticSearch;
  }

  /** Builds the bean and returns the expected vector dimension it hands the service. */
  private static int dimensionPassedToService(SemanticSearchConfiguration semanticSearch)
      throws Exception {
    EntityIndexConfiguration entityIndex = new EntityIndexConfiguration();
    entityIndex.setSemanticSearch(semanticSearch);
    ElasticSearchConfiguration elasticSearch = new ElasticSearchConfiguration();
    elasticSearch.setEntityIndex(entityIndex);
    ConfigurationProvider configurationProvider = mock(ConfigurationProvider.class);
    when(configurationProvider.getElasticSearch()).thenReturn(elasticSearch);

    SemanticEntitySearchServiceFactory factory = new SemanticEntitySearchServiceFactory();
    inject(factory, "configurationProvider", configurationProvider);
    inject(factory, "searchClusterRegistry", mock(SearchClusterRegistry.class));
    inject(factory, "embeddingProvider", mock(EmbeddingProvider.class));

    AtomicReference<List<?>> constructorArgs = new AtomicReference<>();
    try (MockedConstruction<SemanticEntitySearchService> ignored =
        mockConstruction(
            SemanticEntitySearchService.class,
            (service, context) -> constructorArgs.set(context.arguments()))) {
      factory.getInstance(mock(MappingsBuilder.class));
    }
    // (searchClient, embeddingProvider, mappingsBuilder, modelEmbeddingKey,
    // expectedVectorDimension, entityIndexConfiguration)
    return (Integer) constructorArgs.get().get(4);
  }

  private static void inject(Object target, String fieldName, Object value) throws Exception {
    Field field = SemanticEntitySearchServiceFactory.class.getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(target, value);
  }
}
