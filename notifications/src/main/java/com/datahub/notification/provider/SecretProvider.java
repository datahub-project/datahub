package com.datahub.notification.provider;

import com.datahub.authentication.Authentication;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.secret.SecretService;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.secret.DataHubSecretValue;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;


/**
 * A class that resolves DataHub secrets to their decrypted values.
 */
public class SecretProvider {

  private final EntityClient _entityClient;
  private final Authentication _systemAuthentication;
  private final SecretService _secretService;
  private final LoadingCache<Urn, String> _encryptedSecretCache;

  public SecretProvider(final EntityClient entityClient,
      final Authentication systemAuthentication,
      final SecretService secretService) {
    _entityClient = entityClient;
    _systemAuthentication = systemAuthentication;
    _secretService = secretService;
    _encryptedSecretCache = CacheBuilder.newBuilder()
        .maximumSize(10000)
        .expireAfterWrite(10, TimeUnit.MINUTES)
        .build(
            new CacheLoader<Urn, String>() {
              public String load(Urn key) throws Exception {
                return getEncryptedSecret(key);
              }
        });
  }

  /**
   * Retrieves the decrypted value of a secret with a given urn, or null if one cannot be found.
   * @throws {@link Exception} if the secret cannot be resolved.
   */
  public String getSecretValue(final Urn secretUrn) throws Exception {
    String encryptedSecret = _encryptedSecretCache.get(secretUrn);
    if (encryptedSecret == null) {
      return null;
    }
    return _secretService.decrypt(encryptedSecret);
  }

  /**
   * Simply decrypts an encrypted secret string
   */
  public String decryptSecret(final String secret) {
    return _secretService.decrypt(secret);
   }

  private String getEncryptedSecret(final Urn secretUrn) throws RemoteInvocationException, URISyntaxException {
    EntityResponse response = _entityClient.getV2(
        Constants.SECRETS_ENTITY_NAME,
        secretUrn,
        ImmutableSet.of(Constants.SECRET_VALUE_ASPECT_NAME),
        _systemAuthentication
    );

    if (response == null || !response.getAspects().containsKey(Constants.SECRET_VALUE_ASPECT_NAME)) {
      // Secret not found. Return null.
      return null;
    }

    return new DataHubSecretValue(response.getAspects()
        .get(Constants.SECRET_VALUE_ASPECT_NAME)
        .getValue()
        .data()
    ).getValue();
  }
}
