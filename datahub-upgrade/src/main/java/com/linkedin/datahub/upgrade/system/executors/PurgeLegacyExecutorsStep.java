package com.linkedin.datahub.upgrade.system.executors;

import static com.linkedin.metadata.AcrylConstants.REMOTE_EXECUTOR_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PurgeLegacyExecutorsStep implements UpgradeStep {

  private static final String UPGRADE_ID = PurgeLegacyExecutors.class.getSimpleName();
  private static final Urn UPGRADE_ID_URN = BootstrapStep.getUpgradeUrn(UPGRADE_ID);

  private final OperationContext systemOpContext;
  private final EntityService<?> entityService;
  private final SearchService searchService;
  private final boolean enabled;
  private final boolean reprocessEnabled;
  private final Integer batchSize;

  public PurgeLegacyExecutorsStep(
      OperationContext systemOpContext,
      EntityService<?> entityService,
      SearchService searchService,
      boolean enabled,
      boolean reprocessEnabled,
      Integer batchSize) {
    this.systemOpContext = systemOpContext;
    this.entityService = entityService;
    this.searchService = searchService;
    this.enabled = enabled;
    this.reprocessEnabled = reprocessEnabled;
    this.batchSize = batchSize;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      String scrollId = null;

      do {
        // 1. Scroll across all remote executor entities that need to be purged
        final ScrollResult scrollResult =
            searchService.scrollAcrossEntities(
                context.opContext(),
                Set.of(REMOTE_EXECUTOR_ENTITY_NAME),
                "*",
                null,
                null,
                scrollId,
                null,
                batchSize,
                null);

        if (scrollResult.getNumEntities() == 0 || scrollResult.getEntities().isEmpty()) {
          break;
        }

        // 2. Try deleting all the urns
        Set<Urn> urns =
            scrollResult.getEntities().stream()
                .map(SearchEntity::getEntity)
                .collect(Collectors.toSet());
        for (Urn urn : urns) {
          try {
            entityService.deleteUrn(context.opContext(), urn);
          } catch (Exception e) {
            log.error(
                "Failed to delete entity with urn: {}. Consider reprocessing this upgrade step.",
                urn,
                e);
          }
        }

        // 3. scroll to next batch
        scrollId = scrollResult.getScrollId();
      } while (scrollId != null);

      return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
    };
  }

  @Override
  public String id() {
    return UPGRADE_ID;
  }

  /**
   * Returns whether the upgrade should proceed if the step fails after exceeding the maximum
   * retries.
   */
  @Override
  public boolean isOptional() {
    return false;
  }

  @Override
  /**
   * Returns whether the upgrade should be skipped. Uses previous run history or the environment
   * variables to determine whether to skip.
   */
  public boolean skip(UpgradeContext context) {
    if (reprocessEnabled && enabled) {
      return false;
    }

    boolean previouslyRun =
        entityService.exists(
            systemOpContext, UPGRADE_ID_URN, DATA_HUB_UPGRADE_RESULT_ASPECT_NAME, true);

    if (previouslyRun) {
      log.info("{} was already run. Skipping.", id());
    }
    return (previouslyRun || !enabled);
  }
}
