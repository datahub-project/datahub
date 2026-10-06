package com.linkedin.gms.factory.search;

import static com.linkedin.gms.factory.common.IndexConventionFactory.INDEX_CONVENTION_BEAN;

import com.datahub.context.OperationFingerprint;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.linkedin.gms.factory.common.GitVersionFactory;
import com.linkedin.gms.factory.common.IndexConventionFactory;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.version.GitVersion;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({IndexConventionFactory.class, GitVersionFactory.class})
public class ElasticSearchIndexBuilderFactory {

  @Autowired
  @Qualifier("searchClientShim")
  private SearchClientShim<?> searchClient;

  @Value("${elasticsearch.index.settingsOverrides}")
  private String indexSettingOverrides;

  @Value("${elasticsearch.index.entitySettingsOverrides}")
  private String entityIndexSettingOverrides;

  @Bean(name = "elasticSearchIndexSettingsOverrides")
  @Nonnull
  protected Map<String, Map<String, Object>> getIndexSettingsOverrides(
      @Qualifier(INDEX_CONVENTION_BEAN) IndexConvention indexConvention) {

    // Bootstrap-time Spring wiring — no per-request OperationContext is obtainable here.
    return Stream.concat(
            parseIndexSettingsMap(indexSettingOverrides).entrySet().stream()
                .map(
                    e ->
                        Map.entry(
                            indexConvention.getIndexName(OperationFingerprint.EMPTY, e.getKey()),
                            e.getValue())),
            parseIndexSettingsMap(entityIndexSettingOverrides).entrySet().stream()
                .map(
                    e ->
                        Map.entry(
                            indexConvention.getEntityIndexName(
                                OperationFingerprint.EMPTY, e.getKey()),
                            e.getValue())))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  @Bean(name = "elasticSearchIndexBuilder")
  @Nonnull
  protected ESIndexBuilder getInstance(
      @Qualifier("elasticSearchIndexSettingsOverrides") Map<String, Map<String, Object>> overrides,
      final ConfigurationProvider configurationProvider,
      final GitVersion gitVersion) {
    ElasticSearchConfiguration esConfig = configurationProvider.getElasticSearch();
    return new ESIndexBuilder(
        searchClient,
        // Shard and replica counts are per cluster, so the builder gets the primary cluster's
        // effective view rather than the shared defaults, which carry no sizing.
        withEffectiveIndex(esConfig, ElasticSearchConfiguration.PRIMARY_CLUSTER),
        configurationProvider.getStructuredProperties(),
        overrides,
        gitVersion);
  }

  /** Returns {@code esConfig} with {@code index} resolved for the named cluster. */
  @Nonnull
  static ElasticSearchConfiguration withEffectiveIndex(
      @Nonnull ElasticSearchConfiguration esConfig, @Nonnull String clusterName) {
    return esConfig.toBuilder()
        .index(esConfig.getCluster(clusterName).effectiveIndex(esConfig.getIndex()))
        .build();
  }

  /**
   * Parses {@code {"<index>": {"<setting>": <value>}}}. A value may be a string (flat setting such
   * as {@code number_of_shards}) or a nested object/array (grouped settings such as {@code
   * analysis}). Scalars are kept as their JSON text so they compare equal to the strings the search
   * engine returns for stored settings (e.g. {@code 2}, not {@code 2.0}). JSON {@code null} entries
   * are dropped: a null target value never equals the stored one and would reindex on every run.
   */
  @Nonnull
  static Map<String, Map<String, Object>> parseIndexSettingsMap(@Nullable String json) {
    if (json == null || json.isBlank()) {
      return Map.of();
    }
    JsonElement root = JsonParser.parseString(json);
    if (root.isJsonNull()) {
      return Map.of();
    }
    Map<String, Map<String, Object>> result = new LinkedHashMap<>();
    for (Map.Entry<String, JsonElement> index : root.getAsJsonObject().entrySet()) {
      Map<String, Object> settings = new LinkedHashMap<>();
      for (Map.Entry<String, JsonElement> setting : index.getValue().getAsJsonObject().entrySet()) {
        if (!setting.getValue().isJsonNull()) {
          settings.put(setting.getKey(), toSettingValue(setting.getValue()));
        }
      }
      result.put(index.getKey(), settings);
    }
    return result;
  }

  private static Object toSettingValue(JsonElement element) {
    if (element.isJsonObject()) {
      Map<String, Object> map = new LinkedHashMap<>();
      element.getAsJsonObject().entrySet().stream()
          .filter(e -> !e.getValue().isJsonNull())
          .forEach(e -> map.put(e.getKey(), toSettingValue(e.getValue())));
      return map;
    }
    if (element.isJsonArray()) {
      List<Object> list = new ArrayList<>();
      element.getAsJsonArray().forEach(e -> list.add(toSettingValue(e)));
      return list;
    }
    return element.isJsonNull() ? null : element.getAsString();
  }
}
