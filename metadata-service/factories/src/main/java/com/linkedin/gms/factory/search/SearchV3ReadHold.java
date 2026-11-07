package com.linkedin.gms.factory.search;

import com.datahub.context.OperationFingerprint;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.EntitySearchIndexResolver;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3IndexKeys;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.IndexMigrationStamp;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.SearchV3ReadHoldPolicy;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.client.indices.GetMappingsRequest;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;

/**
 * Keeps a process on V2 reads while a V2 entity index has no complete V3 counterpart.
 *
 * <p>Runs once per process, before any consumer reads the search flags. When V3 reads are
 * configured and some existing V2 index is paired with a V3 index that is not stamped complete, it
 * sets V2 enabled, turns keyword V3 reads off, and turns hybrid reads off, on the live
 * configuration. A process that finds no V2 entity index is left alone, so a fresh install reads
 * V3. The hold lifts on the next start once every paired V3 index is complete. A V2 cluster that
 * cannot be listed fails startup; a paired V3 index that cannot be read keeps its pair held.
 */
@Slf4j
public class SearchV3ReadHold {

  /** Highest-precedence source system info reads, so a hold is visible as the effective flags. */
  static final String PROPERTY_SOURCE = "searchV3ReadHold";

  static final String V2_ENABLED = "elasticsearch.entityIndex.v2.enabled";
  static final String V3_KEYWORD_READ = "elasticsearch.entityIndex.v3.keywordReadEnabled";
  static final String V3_HYBRID_READ = "elasticsearch.entityIndex.v3.hybridReadEnabled";

  private final SearchClusterRegistry searchClusterRegistry;
  private final IndexConvention indexConvention;
  private final EntityRegistry entityRegistry;
  @Nullable private final ConfigurableEnvironment environment;

  public SearchV3ReadHold(
      @Nonnull SearchClusterRegistry searchClusterRegistry,
      @Nonnull IndexConvention indexConvention,
      @Nonnull EntityRegistry entityRegistry,
      @Nullable ConfigurableEnvironment environment) {
    this.searchClusterRegistry = searchClusterRegistry;
    this.indexConvention = indexConvention;
    this.entityRegistry = entityRegistry;
    this.environment = environment;
  }

  public void applyTo(@Nonnull ElasticSearchConfiguration elasticSearchConfiguration) {
    EntityIndexConfiguration entityIndex = elasticSearchConfiguration.getEntityIndex();
    if (entityIndex == null || !EntitySearchIndexResolver.shouldReadV3(entityIndex)) {
      return;
    }
    Set<String> pairedV3Indices = pairedV3Indices();
    if (pairedV3Indices.isEmpty()) {
      return;
    }
    Set<String> incomplete =
        SearchV3ReadHoldPolicy.incompleteV3Indices(
            pairedV3Indices, migrationStates(pairedV3Indices));
    if (incomplete.isEmpty()) {
      log.info("Search V3 read hold not needed: every paired V3 index is stamped complete");
      return;
    }
    hold(entityIndex, "paired V3 indices not stamped complete " + incomplete);
  }

  /**
   * V3 indices paired with an existing V2 entity index. A V2 cluster that cannot be listed fails
   * startup rather than reading as a fresh install.
   */
  private Set<String> pairedV3Indices() {
    SearchClientShim<?> v2Client = searchClusterRegistry.clientFor(SearchComponent.SEARCH_V2);
    Set<String> paired = new TreeSet<>();
    for (EntitySpec spec : entityRegistry.getEntitySpecs().values()) {
      String v2Index =
          indexConvention.getEntityIndexName(OperationFingerprint.EMPTY, spec.getName());
      try {
        if (indexExists(v2Client, v2Index)) {
          paired.add(
              indexConvention.getEntityIndexNameV3(
                  OperationFingerprint.EMPTY, V3IndexKeys.resolve(spec)));
        }
      } catch (IOException e) {
        throw new IllegalStateException(
            "Cannot list V2 entity index "
                + v2Index
                + "; startup stops rather than assume a"
                + " fresh install",
            e);
      }
    }
    return paired;
  }

  /**
   * Migration state per paired V3 index. A V3 index that cannot be read is left without a state, so
   * its pair counts as not ready.
   */
  private Map<String, String> migrationStates(Set<String> v3Indices) {
    SearchClientShim<?> v3Client = searchClusterRegistry.clientFor(SearchComponent.SEARCH_V3);
    Map<String, String> states = new HashMap<>();
    for (String v3Index : v3Indices) {
      try {
        if (indexExists(v3Client, v3Index)) {
          IndexMigrationStamp.read(mappingSource(v3Client, v3Index))
              .ifPresent(state -> states.put(v3Index, state));
        }
      } catch (IOException | RuntimeException e) {
        log.warn(
            "Cannot read the migration stamp of V3 index {}; treating it as not ready", v3Index, e);
      }
    }
    return states;
  }

  private static boolean indexExists(SearchClientShim<?> client, String indexName)
      throws IOException {
    return client.indexExists(
        OperationFingerprint.EMPTY, new GetIndexRequest(indexName), RequestOptions.DEFAULT);
  }

  private static Map<String, Object> mappingSource(SearchClientShim<?> client, String indexName)
      throws IOException {
    return client
        .getIndexMapping(
            OperationFingerprint.EMPTY,
            new GetMappingsRequest().indices(indexName),
            RequestOptions.DEFAULT)
        .mappings()
        .values()
        .stream()
        .map(MappingMetadata::getSourceAsMap)
        .filter(Objects::nonNull)
        .findFirst()
        .orElse(Map.of());
  }

  private void hold(EntityIndexConfiguration entityIndex, String reason) {
    if (entityIndex.getV2() == null) {
      entityIndex.setV2(new EntityIndexVersionConfiguration());
    }
    entityIndex.getV2().setEnabled(true);
    entityIndex.getV3().setKeywordReadEnabled(false);
    entityIndex.getV3().setHybridReadEnabled(false);
    publish(environment);
    log.warn(
        "Search V3 read hold: {}. This process reads and writes V2 and keeps V3 reads off. Reads"
            + " move to V3 on the next start once every paired V3 index is stamped complete.",
        reason);
  }

  /** Conditions are already evaluated. This source is for system info and later property reads. */
  private static void publish(@Nullable ConfigurableEnvironment environment) {
    if (environment == null) {
      return;
    }
    Map<String, Object> flags = new HashMap<>();
    flags.put(V2_ENABLED, "true");
    flags.put(V3_KEYWORD_READ, "false");
    flags.put(V3_HYBRID_READ, "false");
    if (environment.getPropertySources().contains(PROPERTY_SOURCE)) {
      environment.getPropertySources().remove(PROPERTY_SOURCE);
    }
    environment.getPropertySources().addFirst(new MapPropertySource(PROPERTY_SOURCE, flags));
  }
}
