package com.linkedin.datahub.upgrade.system.semanticsearch;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.BlockingSystemUpgrade;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Blocking system upgrade that copies documents from base entity indices to semantic search
 * indices. Runs before GMS starts accepting traffic to ensure no MCPs are processed during reindex.
 */
@Slf4j
public class CopyDocumentsToSemanticIndices implements BlockingSystemUpgrade {

  private final List<UpgradeStep> steps;

  public CopyDocumentsToSemanticIndices(
      @Nonnull OperationContext opContext,
      SearchClientShim<?> searchClient,
      EntityService<?> entityService,
      SemanticSearchConfiguration semanticSearchConfiguration,
      IndexConvention indexConvention,
      boolean enabled) {
    if (!enabled) {
      steps = ImmutableList.of();
      return;
    }

    Set<String> enabledEntities = semanticSearchConfiguration.getEnabledEntities();
    if (enabledEntities == null || enabledEntities.isEmpty()) {
      log.info("No entities enabled for copying documents to semantic search indices. Skipping.");
      steps = ImmutableList.of();
      return;
    }

    ImmutableList.Builder<UpgradeStep> builder = ImmutableList.builder();
    for (String entity : enabledEntities) {
      builder.add(
          new CopyDocumentsToSemanticIndexStep(
              opContext, entity, searchClient, entityService, indexConvention));
    }
    steps = builder.build();
  }

  @Override
  public String id() {
    return getClass().getSimpleName();
  }

  @Override
  public List<UpgradeStep> steps() {
    return steps;
  }
}
