package com.linkedin.metadata.boot.steps;

import com.linkedin.metadata.Constants;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexConfig;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Bootstrap step to ensure Organization search indices are properly created with correct mappings.
 * This step ensures the index exists before any Organization entities are ingested, preventing
 * Elasticsearch from auto-creating the index with dynamic mappings that would cause search
 * failures.
 *
 * <p>The Organization entity was experiencing search failures because: 1. When first Organization
 * entity was created, Elasticsearch auto-created the index 2. Auto-created indices use dynamic
 * field mapping 3. The `urn` field was mapped as `text` instead of `keyword` 4. Search queries
 * failed when trying to sort by `urn` (text fields don't support sorting by default)
 *
 * <p>This bootstrap step pre-creates the Organization index with V2MappingsBuilder settings,
 * ensuring the `urn` field is correctly mapped as `keyword` with text subfields.
 */
@Slf4j
public class RestoreOrganizationIndicesStep implements BootstrapStep {

  private final ElasticSearchService elasticSearchService;

  public RestoreOrganizationIndicesStep(ElasticSearchService elasticSearchService) {
    this.elasticSearchService = elasticSearchService;
  }

  @Override
  public String name() {
    return getClass().getSimpleName();
  }

  @Nonnull
  @Override
  public ExecutionMode getExecutionMode() {
    // Run as BLOCKING to ensure Organization indices exist before any entities are created
    return ExecutionMode.BLOCKING;
  }

  @Override
  public void execute(@Nonnull OperationContext systemOperationContext) throws Exception {
    log.info("{} Creating Organization search indices with proper mappings...", name());

    try {
      // Build reindex configs for all entities to get the Organization config
      List<ReindexConfig> configs =
          elasticSearchService.buildReindexConfigs(systemOperationContext, List.of());

      // Find and build only the Organization index
      boolean organizationIndexBuilt = false;
      for (ReindexConfig config : configs) {
        if (config.name().contains(Constants.ORGANIZATION_ENTITY_NAME.toLowerCase())) {
          log.info("{} Building Organization index: {}", name(), config.name());
          elasticSearchService.getIndexBuilder().buildIndex(config);
          organizationIndexBuilt = true;
          log.info("{} Successfully created Organization index: {}", name(), config.name());
          break;
        }
      }

      if (!organizationIndexBuilt) {
        log.warn(
            "{} No Organization index configuration found - index will be created on first entity ingest",
            name());
      }
    } catch (Exception e) {
      // Log but don't fail bootstrap - index will be created on first entity ingest
      log.warn(
          "{} Failed to pre-create Organization index, it will be created on first entity ingest: {}",
          name(),
          e.getMessage());
    }
  }
}
