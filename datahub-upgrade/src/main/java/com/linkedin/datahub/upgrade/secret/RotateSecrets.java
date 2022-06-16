package com.linkedin.datahub.upgrade.secret;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeCleanupStep;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.metadata.entity.EntityService;
import java.util.ArrayList;
import java.util.List;


/**
 * This upgrade performs the following steps:
 *
 * 1. Reads previously encrypted secrets
 * 2. Decrypts using the old encryption secret
 * 3. Re-encrypts them using the new encryption secret
 * 4. Writes them back as a new version of the info aspect
 *
 * This upgrade takes the following arguments:
 *
 *  - EXISTING_KEY (required) = The current encryption key to replace
 *  - NEW_KEY (required) = The new encryption key to use.
 *  - MODE (optional) = The execution mode. IGNORE_DECRYPT_FAILURE to ignore decryption failures, GUESS_ON_DECRYPT_FAILURE to
 *    guess the existing encryption key on decryption failure. If not provided, the mode is DEFAULT. This means that
 *    any failure during rotation (decryption, encryption, writing to db) will cause the upgrade to abort with an exception.
 *
 *  Note that this upgrade currently supports a maximum of 10,000 secrets.
 */
public class RotateSecrets implements Upgrade  {

  // Most people should not have more than 1000 secrets.
  public static final Integer BATCH_SIZE = 1000;

  private final List<UpgradeStep> _steps;

  public RotateSecrets(final EntityService entityService) {
    _steps = buildSteps(entityService);
  }

  @Override
  public String id() {
    return "RotateSecrets";
  }

  @Override
  public List<UpgradeStep> steps() {
    return _steps;
  }

  private List<UpgradeStep> buildSteps(final EntityService entityService) {
    final List<UpgradeStep> steps = new ArrayList<>();
    steps.add(new RotateSecretsStep(entityService));
    return steps;
  }

  @Override
  public List<UpgradeCleanupStep> cleanupSteps() {
    return ImmutableList.of();
  }

}
