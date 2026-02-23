package com.linkedin.datahub.upgrade;

import static com.linkedin.metadata.Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Marker interface for upgrade steps that should only run once and persist their completion state.
 *
 * <p>Steps implementing this interface will have their completion state automatically persisted by
 * the {@link com.linkedin.datahub.upgrade.impl.DefaultUpgradeManager} upon successful execution.
 * This eliminates the error-prone manual call to {@link
 * com.linkedin.metadata.boot.BootstrapStep#setUpgradeResult}.
 *
 * <p><b>Usage:</b>
 *
 * <pre>{@code
 * @Slf4j
 * public class MyUpgradeStep implements PersistentUpgradeStep {
 *   private static final String UPGRADE_ID = "MyUpgradeStep";
 *   private static final Urn UPGRADE_ID_URN = BootstrapStep.getUpgradeUrn(UPGRADE_ID);
 *
 *   private final OperationContext systemOpContext;
 *   private final EntityService<?> entityService;
 *   private final boolean reprocessEnabled;
 *
 *   @Override
 *   public Urn getUpgradeIdUrn() {
 *     return UPGRADE_ID_URN;
 *   }
 *
 *   @Override
 *   public EntityService<?> getEntityService() {
 *     return entityService;
 *   }
 *
 *   @Override
 *   public OperationContext getSystemOpContext() {
 *     return systemOpContext;
 *   }
 *
 *   @Override
 *   public boolean isReprocessEnabled() {
 *     return reprocessEnabled;
 *   }
 *
 *   @Override
 *   public Function<UpgradeContext, UpgradeStepResult> executable() {
 *     return (context) -> {
 *       // ... do work ...
 *       // NO NEED to call BootstrapStep.setUpgradeResult() manually!
 *       return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
 *     };
 *   }
 * }
 * }</pre>
 */
public interface PersistentUpgradeStep extends UpgradeStep {

  Logger log = LoggerFactory.getLogger(PersistentUpgradeStep.class);

  /**
   * Returns the URN for storing this step's completion state.
   *
   * <p>Typically created via {@link
   * com.linkedin.metadata.boot.BootstrapStep#getUpgradeUrn(String)}.
   */
  Urn getUpgradeIdUrn();

  /** Returns the EntityService instance for persisting completion state. */
  EntityService<?> getEntityService();

  /** Returns the system-level OperationContext for persistence operations. */
  OperationContext getSystemOpContext();

  /**
   * Whether to force reprocessing even if the step previously ran.
   *
   * <p>Default: false (skip if previously ran).
   */
  default boolean isReprocessEnabled() {
    return false;
  }

  /**
   * Default skip logic based on persistent state.
   *
   * <p>Steps can override this if they need additional skip conditions, but they should typically
   * call {@code super.skip(context)} to preserve the persistence check.
   */
  @Override
  default boolean skip(UpgradeContext context) {
    if (isReprocessEnabled()) {
      return false;
    }

    boolean previouslyRun =
        getEntityService()
            .exists(
                getSystemOpContext(), getUpgradeIdUrn(), DATA_HUB_UPGRADE_RESULT_ASPECT_NAME, true);

    if (previouslyRun) {
      log.info("{} was already run. Skipping.", id());
    }
    return previouslyRun;
  }
}
