package io.datahubproject.test.search.config;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datahub.test.Snapshot;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.config.search.CustomConfiguration;
import com.linkedin.metadata.config.search.ExactMatchConfiguration;
import com.linkedin.metadata.config.search.PartialConfiguration;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.config.search.WordGramConfiguration;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistryException;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.net.URISyntaxException;
import java.util.Map;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

/** This is common configuration for search regardless of which test container implementation. */
@TestConfiguration
public class SearchCommonTestConfiguration {
  @Bean
  public SearchConfiguration searchConfiguration() {
    SearchConfiguration searchConfiguration = new SearchConfiguration();
    searchConfiguration.setMaxTermBucketSize(20);

    ExactMatchConfiguration exactMatchConfiguration = new ExactMatchConfiguration();
    exactMatchConfiguration.setExclusive(false);
    exactMatchConfiguration.setExactFactor(10.0f);
    exactMatchConfiguration.setWithPrefix(true);
    exactMatchConfiguration.setPrefixFactor(6.0f);
    exactMatchConfiguration.setCaseSensitivityFactor(0.7f);
    exactMatchConfiguration.setEnableStructured(true);

    WordGramConfiguration wordGramConfiguration = new WordGramConfiguration();
    wordGramConfiguration.setTwoGramFactor(1.2f);
    wordGramConfiguration.setThreeGramFactor(1.5f);
    wordGramConfiguration.setFourGramFactor(1.8f);

    PartialConfiguration partialConfiguration = new PartialConfiguration();
    partialConfiguration.setFactor(0.4f);
    partialConfiguration.setUrnFactor(0.5f);

    searchConfiguration.setExactMatch(exactMatchConfiguration);
    searchConfiguration.setWordGram(wordGramConfiguration);
    searchConfiguration.setPartial(partialConfiguration);
    return searchConfiguration;
  }

  @Bean
  public CustomSearchConfiguration customSearchConfiguration() throws Exception {
    CustomConfiguration customConfiguration = new CustomConfiguration();
    customConfiguration.setEnabled(true);
    customConfiguration.setFile("search_config_builder_test.yml");
    return customConfiguration.resolve(new YAMLMapper());
  }

  @Bean(name = "entityRegistry")
  public EntityRegistry entityRegistry() throws EntityRegistryException {
    return new ConfigEntityRegistry(
        SearchCommonTestConfiguration.class
            .getClassLoader()
            .getResourceAsStream("entity-registry.yml"));
  }

  @Bean(name = "aspectRetriever")
  protected AspectRetriever aspectRetriever(final EntityRegistry entityRegistry)
      throws RemoteInvocationException, URISyntaxException {
    AspectRetriever aspectRetriever = mock(AspectRetriever.class);
    when(aspectRetriever.getEntityRegistry()).thenReturn(entityRegistry);
    when(aspectRetriever.getLatestAspectObjects(any(), any())).thenReturn(Map.of());
    return aspectRetriever;
  }

  @Bean(name = "snapshotRegistryAspectRetriever")
  protected AspectRetriever snapshotRegistryAspectRetriever()
      throws RemoteInvocationException, URISyntaxException {
    AspectRetriever aspectRetriever = mock(AspectRetriever.class);
    when(aspectRetriever.getEntityRegistry())
        .thenReturn(new SnapshotEntityRegistry(new Snapshot()));
    when(aspectRetriever.getLatestAspectObjects(any(), any())).thenReturn(Map.of());
    return aspectRetriever;
  }

  @Bean(name = "systemOperationContext")
  public OperationContext systemOperationContext(final EntityRegistry entityRegistry) {
    return TestOperationContexts.systemContextNoSearchAuthorization(entityRegistry);
  }
}
