package com.linkedin.datahub.upgrade.secret;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.entity.EntityResponse;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.query.ListUrnsResult;
import com.linkedin.metadata.secret.SecretService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.secret.DataHubSecretValue;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class RotateSecretsStep implements UpgradeStep {

  private static final String EXISTING_KEY_ARG = "EXISTING_KEY";
  private static final String NEW_KEY_ARG = "NEW_KEY";
  private static final String MODE_ARG = "MODE";

  private static final int MAX_SUPPORTED_SECRETS = 10000;
  private static final int BATCH_SIZE = 1000;
  private static final long BATCH_DELAY = 250;

  private final EntityService _entityService;

  private String _existingKey;
  private String _newKey;
  private SecretService _existingSecretService;
  private SecretService _newSecretService;
  private RotationMode _mode;

  enum RotationMode {
    /**
     * Fail the step on any failures.
     */
    DEFAULT,
    /**
     * Ignore cases where decryption fails. This is useful in partial rotation scenarios.
     */
    IGNORE_DECRYPT_FAILURE,
    /**
     * Guess similar keys on decrypt failure.
     */
    GUESS_ON_DECRYPT_FAILURE
  }

  public RotateSecretsStep(final EntityService entityService) {
    _entityService = entityService;
  }

  @Override
  public String id() {
    return "RotateSecretsStep";
  }

  @Override
  public int retryCount() {
    return 0;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {

      // Verify arguments.
      _existingKey = context.parsedArgs().containsKey(EXISTING_KEY_ARG)
        ? context.parsedArgs().get(EXISTING_KEY_ARG).get()
        : Objects.requireNonNull(System.getenv(EXISTING_KEY_ARG));
      // Will throw if not provided.
      _newKey = context.parsedArgs().containsKey(NEW_KEY_ARG)
          ? context.parsedArgs().get(NEW_KEY_ARG).get()
          : Objects.requireNonNull(System.getenv(NEW_KEY_ARG));
      // Will throw if not provided.
      _existingSecretService = new SecretService(_existingKey);
      _newSecretService = new SecretService(_newKey);
      _mode = context.parsedArgs().containsKey(MODE_ARG) && context.parsedArgs().get(MODE_ARG).isPresent()
          ? RotationMode.valueOf(context.parsedArgs().get(MODE_ARG).get())
          : System.getenv().containsKey(MODE_ARG) ? RotationMode.valueOf(System.getenv(MODE_ARG)) : RotationMode.DEFAULT;


      context.report().addLine("Preparing to rotate secrets...");

      // 1. Fetch urns for all secrets. We support maximum of 10k secrets for this upgrade.
      ListUrnsResult result = _entityService.listUrns(Constants.SECRETS_ENTITY_NAME, 0, MAX_SUPPORTED_SECRETS);

      if (result.getTotal() > MAX_SUPPORTED_SECRETS) {
        // We don't have > 10k, since this is currently unrealistic.
        throw new UnsupportedOperationException("Instance has more than 10,000 secrets. Aborting...");
      }

      int totalRotated = 0;
      int start = 0;
      while (start < result.getTotal()) {

        context.report()
            .addLine(String.format("Reading secrets %s through %s from the aspects table.", 0, BATCH_SIZE));

        try {
          // 2. Fetch the values for each in batches of 1000.
          Map<Urn, EntityResponse> secretResponse =
              _entityService.getEntitiesV2(Constants.SECRETS_ENTITY_NAME, new HashSet<>(result.getEntities()),
                  ImmutableSet.of(Constants.SECRET_VALUE_ASPECT_NAME));

          // 3. For each value, attempt to decrypt the secret, and generate a new secret value.
          Map<Urn, DataHubSecretValue> urnToNewSecret = new HashMap<>();
          for (Map.Entry<Urn, EntityResponse> entry : secretResponse.entrySet()) {
            if (entry.getValue().getAspects().containsKey(Constants.SECRET_VALUE_ASPECT_NAME)) {

              // a. Get existing secret value.
              DataHubSecretValue existingSecretValue = new DataHubSecretValue(
                  entry.getValue().getAspects().get(Constants.SECRET_VALUE_ASPECT_NAME).getValue().data());

              // b. Rotate to create new secret value.
              DataHubSecretValue newSecretValue = createNewSecretValue(entry.getKey(), existingSecretValue);

              if (newSecretValue != null) {
                // Secret has been rotated successfully.
                urnToNewSecret.put(entry.getKey(), newSecretValue);
              }

              // Secret has been skipped without error.

            } else {
              log.warn(String.format("Failed to resolve value aspect for secret with urn %s", entry.getKey().toString()));
            }
          }

          // 4. Write a new version of the secret value aspect for the urn
          for (Map.Entry<Urn, DataHubSecretValue> entry : urnToNewSecret.entrySet()) {
            final MetadataChangeProposal proposal = new MetadataChangeProposal();
            proposal.setEntityUrn(entry.getKey());
            proposal.setEntityType(Constants.SECRETS_ENTITY_NAME);
            proposal.setAspectName(Constants.SECRET_VALUE_ASPECT_NAME);
            proposal.setAspect(GenericRecordUtils.serializeAspect(entry.getValue()));
            proposal.setChangeType(ChangeType.UPSERT);
            try {
              _entityService.ingestProposal(
                  proposal,
                  secretResponse.get(entry.getKey()).getAspects().get(Constants.SECRET_VALUE_ASPECT_NAME).getCreated());
            } catch (Exception e) {
              throw new RuntimeException(String.format("Failed to create rotated secret with urn %s", entry.getKey()), e);
            }
          }
          start = start + BATCH_SIZE;
          Thread.sleep(BATCH_DELAY);
        } catch (Exception e) {
          throw new RuntimeException("Caught exception while attempting to rotate secrets! Exiting..", e);
        }
      }
      context.report().addLine(String.format("Successfully rotated %s / %s total secrets!", totalRotated, result.getTotal()));
      return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.SUCCEEDED);
    };
  }

  private DataHubSecretValue createNewSecretValue(final Urn secretUrn, final DataHubSecretValue value) {

    // 1. Attempt to decrypt the secret
    final String decryptedSecret = decryptExistingSecret(secretUrn, value.getValue());

    if (decryptedSecret == null) {
      // Skip this secret
      return null;
    }

    log.info("Successfully decrypted key with urn {}", secretUrn);

    // 2. Re-encrypt using new key
    final String encryptedKey = _newSecretService.encrypt(decryptedSecret);

    // 3. Create and return new secret value
    DataHubSecretValue newValue = new DataHubSecretValue(value.data());
    newValue.setValue(encryptedKey);
    return newValue;

  }

  private String decryptExistingSecret(final Urn secretUrn, final String secretValue) {
    try {
      return _existingSecretService.decrypt(secretValue);
    } catch (Exception e) {
      log.warn("Failed to decrypt secret with urn {} using existing key.", secretUrn.toString());
      if (RotationMode.GUESS_ON_DECRYPT_FAILURE.equals(_mode)) {
        log.info("Attempting to guess encryption key value...");
        return  guessEncryptionKeyAndDecrypt(secretUrn, secretValue);
      } else if (RotationMode.IGNORE_DECRYPT_FAILURE.equals(_mode)) {
        log.info("Skipping decrypt failure for urn {}..", secretUrn.toString());
        return null;
      }
      // Configured mode is DEFAULT -- throw on decryption failure.
      throw e;
    }
  }

  @Nullable
  private String guessEncryptionKeyAndDecrypt(final Urn secretUrn, final String secretValue) {
    // First, guess empty string
    final String res1 = tryDecryptSecret(secretValue, "");
    if (res1 != null) {
      log.info(String.format("Found empty string decryption key for urn %s", secretUrn));
      return res1;
    }

    // Next, iterate through the old encryption key, delete each character, and try..
    for (int i = 0; i < _existingKey.length(); i++) {
      // Simply try to remove a character at the current index
      final String keyToTry = new StringBuilder(secretValue).deleteCharAt(i).toString();
      final String res2 = tryDecryptSecret(secretValue, keyToTry);
      if (res2 != null) {
        log.info(String.format("Successfully guessed existing encryption key %s for urn %s", keyToTry, secretUrn));
        return res2;
      }
    }

    // No success.
    log.info(String.format("Failed to guess encryption key for urn %s. Continuing...", secretUrn));
    return null;
  }

  @Nullable
  private String tryDecryptSecret(final String secretValue, final String keyToTry) {
    try {
      // If decryption succeeds, then return immediately.
      return new SecretService(keyToTry).decrypt(secretValue);
    } catch (Exception e) {
      // No op - we just keep guessing.
      return null;
    }
  }
}