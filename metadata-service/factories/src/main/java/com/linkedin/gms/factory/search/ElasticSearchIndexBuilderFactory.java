package com.linkedin.gms.factory.search;

import static com.linkedin.gms.factory.common.IndexConventionFactory.INDEX_CONVENTION_BEAN;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.linkedin.gms.factory.common.GitVersionFactory;
import com.linkedin.gms.factory.common.IndexConventionFactory;
import com.linkedin.gms.factory.common.RestHighLevelClientFactory;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.spring.YamlPropertySourceFactory;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.version.GitVersion;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.opensearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;

@Configuration
@Import({RestHighLevelClientFactory.class, IndexConventionFactory.class, GitVersionFactory.class})
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

  @Value("${elasticsearch.index.entitySettingsOverrides}")
  private String entityIndexSettingOverrides;

  @Value("#{new Boolean('${elasticsearch.index.enableSettingsReindex}')}")
  private boolean enableSettingsReindex;

  @Value("#{new Boolean('${elasticsearch.index.enableMappingsReindex}')}")
  private boolean enableMappingsReindex;

  @Bean(name = "elasticSearchIndexSettingsOverrides")
  @Nonnull
  protected Map<String, Map<String, String>> getIndexSettingsOverrides(
      @Qualifier(INDEX_CONVENTION_BEAN) IndexConvention indexConvention) {

    return Stream.concat(
            parseIndexSettingsMap(indexSettingOverrides).entrySet().stream()
                .map(e -> Map.entry(indexConvention.getIndexName(e.getKey()), e.getValue())),
            parseIndexSettingsMap(entityIndexSettingOverrides).entrySet().stream()
                .map(e -> Map.entry(indexConvention.getEntityIndexName(e.getKey()), e.getValue())))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  @Bean(name = "elasticSearchIndexBuilder")
  @Nonnull
  protected ESIndexBuilder getInstance(
      @Qualifier("elasticSearchIndexSettingsOverrides") Map<String, Map<String, String>> overrides,
      final ConfigurationProvider configurationProvider,
      final GitVersion gitVersion) {
    return new ESIndexBuilder(
        searchClient,
        numShards,
        numReplicas,
        numRetries,
        refreshIntervalSeconds,
        overrides,
        enableSettingsReindex,
        enableMappingsReindex,
        configurationProvider.getElasticSearch(),
        gitVersion);
  }

  @Nonnull
  private static Map<String, Map<String, String>> parseIndexSettingsMap(@Nullable String json) {
    Optional<Map<String, Map<String, String>>> parseOpt =
        Optional.ofNullable(
            new Gson()
                .fromJson(json, new TypeToken<Map<String, Map<String, String>>>() {}.getType()));
    return parseOpt.orElse(Map.of());
  }
}
