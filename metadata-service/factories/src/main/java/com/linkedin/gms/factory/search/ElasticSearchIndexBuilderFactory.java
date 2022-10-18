package com.linkedin.gms.factory.search;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.linkedin.gms.factory.common.IndexConventionFactory;
import com.linkedin.gms.factory.common.RestHighLevelClientFactory;
import com.linkedin.gms.factory.spring.YamlPropertySourceFactory;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import javax.annotation.Nonnull;

import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.linkedin.gms.factory.common.IndexConventionFactory.INDEX_CONVENTION_BEAN;


@Configuration
@Import({RestHighLevelClientFactory.class, IndexConventionFactory.class})
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
public class ElasticSearchIndexBuilderFactory {

  @Autowired
  @Qualifier("elasticSearchRestHighLevelClient")
  private RestHighLevelClient searchClient;

  @Value("${elasticsearch.index.numShards}")
  private Integer numShards;

  @Value("${elasticsearch.index.numReplicas}")
  private Integer numReplicas;

  @Value("${elasticsearch.index.numRetries}")
  private Integer numRetries;

  @Value("${elasticsearch.index.refreshIntervalSeconds}")
  private Integer refreshIntervalSeconds;

  @Value("${elasticsearch.index.settingsOverrides}")
  private String indexSettingOverrides;

  @Bean(name = "elasticSearchIndexSettingsOverrides")
  @Nonnull
  protected Map<String, Map<String, String>> getIndexSettingsOverrides(
          @Qualifier(INDEX_CONVENTION_BEAN) IndexConvention indexConvention) {
    Optional<Map<String, Map<String, String>>> overrides = Optional.ofNullable(
            new Gson().fromJson(indexSettingOverrides,
                    new TypeToken<Map<String, Map<String, String>>>() { }.getType()));
    return overrides.orElse(Map.of()).entrySet().stream()
            .map(e -> Map.entry(indexConvention.getIndexName(e.getKey()), e.getValue()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  @Bean(name = "elasticSearchIndexBuilder")
  @Nonnull
  protected ESIndexBuilder getInstance(
          @Qualifier("elasticSearchIndexSettingsOverrides") Map<String, Map<String, String>> overrides) {
    return new ESIndexBuilder(searchClient, numShards, numReplicas, numRetries, refreshIntervalSeconds, overrides);
  }
}