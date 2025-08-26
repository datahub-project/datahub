package com.datahub.notification.provider;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.secret.DataHubSecretValue;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.services.SecretService;
import java.net.URISyntaxException;
import javax.annotation.Nonnull;

/**
 * A class that resolves DataHub secrets to their decrypted values. SystemEntityClient caches for 10
 * minutes, application.yaml configuration
 */
public class SecretProvider {

  private final SystemEntityClient _entityClient;
  private final SecretService _secretService;

  public SecretProvider(final SystemEntityClient entityClient, final SecretService secretService) {
    _entityClient = entityClient;
    _secretService = secretService;
  }

  /**
   * Retrieves the decrypted value of a secret with a given urn, or null if one cannot be found.
   *
   * @throws {@link Exception} if the secret cannot be resolved.
   */
  public String getSecretValue(@Nonnull OperationContext opContext, final Urn secretUrn)
      throws Exception {
    return _secretService.decrypt(getEncryptedSecret(opContext, secretUrn));
  }

  /** Simply decrypts an encrypted secret string */
  public String decryptSecret(final String secret) {
    return _secretService.decrypt(secret);
  }

  private String getEncryptedSecret(@Nonnull OperationContext opContext, final Urn secretUrn)
      throws RemoteInvocationException, URISyntaxException {
    EntityResponse response =
        _entityClient.getV2(
            opContext,
            Constants.SECRETS_ENTITY_NAME,
            secretUrn,
            ImmutableSet.of(Constants.SECRET_VALUE_ASPECT_NAME));

    if (response == null
        || !response.getAspects().containsKey(Constants.SECRET_VALUE_ASPECT_NAME)) {
      // Secret not found. Return null.
      return null;
    }

    return new DataHubSecretValue(
            response.getAspects().get(Constants.SECRET_VALUE_ASPECT_NAME).getValue().data())
        .getValue();
  }
}
