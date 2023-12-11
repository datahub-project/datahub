package io.datahubproject.test.search.config;

import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.linkedin.metadata.config.search.CustomConfiguration;
import com.linkedin.metadata.config.search.ExactMatchConfiguration;
import com.linkedin.metadata.config.search.PartialConfiguration;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.config.search.WordGramConfiguration;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistryException;
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
}
