package com.linkedin.datahub.upgrade.restoreaspect;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeCleanupStep;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.util.ArrayList;
import java.util.List;


/**
 * This upgrade restores a specific aspect associated with a specific URN from a specific backup. It should be used to recover
 * data that has been deleted on a per-urn basis, but not to "restore" old versions of a metadata aspect that exist already inside a DB.
 *
 * The job requires the following arguments:
 *
 * - BACKUP_S3_BUCKET: The S3 bucket where the target backup lives, for example
 * - BACKUP_S3_PATH: The S3 path where the target backup lives, for example
 * - URN: The urn associated with the entity to be restored.
 * - ASPECT_NAME: The aspect name to be restored.
 */
public class RestoreAspect implements Upgrade {

  private final List<UpgradeStep> _steps;

  public RestoreAspect(final EntityService entityService, final EntityRegistry entityRegistry) {
    _steps = buildSteps(entityService, entityRegistry);
  }

  @Override
  public String id() {
    return "RestoreAspect";
  }

  @Override
  public List<UpgradeStep> steps() {
    return _steps;
  }

  private List<UpgradeStep> buildSteps(final EntityService entityService, final EntityRegistry entityRegistry) {
    final List<UpgradeStep> steps = new ArrayList<>();
    steps.add(new RestoreAspectStep(entityService, entityRegistry));
    return steps;
  }

  @Override
  public List<UpgradeCleanupStep> cleanupSteps() {
    return ImmutableList.of();
  }
}

