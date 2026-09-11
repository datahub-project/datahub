package com.linkedin.gms.factory.search;

import static com.linkedin.gms.factory.common.IndexConventionFactory.INDEX_CONVENTION_BEAN;

import com.datahub.context.OperationFingerprint;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.linkedin.gms.factory.common.GitVersionFactory;
import com.linkedin.gms.factory.common.IndexConventionFactory;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.EntityMappingLimits;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.version.GitVersion;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({IndexConventionFactory.class, GitVersionFactory.class})
@Slf4j
public class ElasticSearchIndexBuilderFactory {

  @Autowired
  @Qualifier("searchClientShim")
  private SearchClientShim<?> searchClient;

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

  @Value("#{new Boolean('${structuredProperties.systemUpdateEnabled}')}")
  private boolean enableStructuredPropertiesReindex;

  @Value("${elasticsearch.index.maxReindexHours}")
  private Integer maxReindexHours;

  @Bean(name = "elasticSearchIndexSettingsOverrides")
  @Nonnull
  protected Map<String, Map<String, String>> getIndexSettingsOverrides(
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
      @Qualifier("elasticSearchIndexSettingsOverrides") Map<String, Map<String, String>> overrides,
      @Qualifier(INDEX_CONVENTION_BEAN) IndexConvention indexConvention,
      final ConfigurationProvider configurationProvider,
      final GitVersion gitVersion) {
    ESIndexBuilder builder =
        new ESIndexBuilder(
            searchClient,
            configurationProvider.getElasticSearch(),
            configurationProvider.getStructuredProperties(),
            overrides,
            gitVersion);
    builder.setEntityMappingLimits(
        resolveEntityMappingLimits(
            configurationProvider.getElasticSearch().getIndex().getEntityMappingLimits(),
            indexConvention));
    return builder;
  }

  /**
   * Translate {@code elasticsearch.index.entityMappingLimits} (entity name -> limit name -> value)
   * into a resolver keyed by full index name with ES setting paths, plus a defaults map. Unknown
   * limit keys are dropped with a warning so a typo can't silently bypass the code-defined
   * allowlist.
   */
  @Nonnull
  static EntityMappingLimits resolveEntityMappingLimits(
      @Nullable Map<String, Map<String, Integer>> config,
      @Nonnull IndexConvention indexConvention) {
    if (config == null || config.isEmpty()) {
      return EntityMappingLimits.EMPTY;
    }

    Map<String, String> defaults = Map.of();
    Map<String, Map<String, String>> byIndex = new HashMap<>();
    for (Map.Entry<String, Map<String, Integer>> entityEntry : config.entrySet()) {
      String entity = entityEntry.getKey();
      Map<String, String> esSettings = translateLimitKeys(entity, entityEntry.getValue());
      if (esSettings.isEmpty()) {
        continue;
      }
      if (ESIndexBuilder.MAPPING_LIMITS_DEFAULT_KEY.equals(entity)) {
        defaults = esSettings;
      } else {
        // Bootstrap-time Spring wiring — no per-request OperationContext is obtainable here.
        byIndex.put(
            indexConvention.getEntityIndexName(OperationFingerprint.EMPTY, entity), esSettings);
      }
    }
    return new EntityMappingLimits(Map.copyOf(byIndex), defaults);
  }

  @Nonnull
  private static Map<String, String> translateLimitKeys(
      @Nonnull String entity, @Nullable Map<String, Integer> limits) {
    if (limits == null || limits.isEmpty()) {
      return Map.of();
    }
    Map<String, String> out = new HashMap<>();
    // Spring's MapBinder lowercases keys sourced from env vars (e.g. "TOTALFIELDS" ->
    // "totalfields")
    // but preserves case for YAML/property-file keys. Look up case-insensitively so both paths
    // work.
    Map<String, String> caseInsensitiveLimitKeys =
        ESIndexBuilder.MAPPING_LIMIT_SETTING_KEYS.entrySet().stream()
            .collect(
                Collectors.toMap(
                    entry -> entry.getKey().toLowerCase(Locale.ROOT), Map.Entry::getValue));
    for (Map.Entry<String, Integer> e : limits.entrySet()) {
      String configKey = e.getKey() == null ? null : e.getKey().toLowerCase(Locale.ROOT);
      String esKey = configKey == null ? null : caseInsensitiveLimitKeys.get(configKey);
      if (esKey == null || e.getValue() == null) {
        log.warn(
            "Ignoring entityMappingLimits.{}.{} = {} (unsupported limit key; supported: {})",
            entity,
            e.getKey(),
            e.getValue(),
            ESIndexBuilder.MAPPING_LIMIT_SETTING_KEYS.keySet());
        continue;
      }
      out.put(esKey, String.valueOf(e.getValue()));
    }
    return Map.copyOf(out);
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
