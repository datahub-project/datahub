/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package io.datahubproject.iceberg.catalog.credentials;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class CachingCredentialProvider implements CredentialProvider {
  // this should be lesser than the actual token/cred expiration
  private static final int EXPIRATION_MINUTES = 5;

  private final Cache<CredentialsCacheKey, Map<String, String>> credentialCache;

  private final CredentialProvider credentialProvider;

  public CachingCredentialProvider(CredentialProvider credentialProvider) {
    this.credentialProvider = credentialProvider;
    this.credentialCache =
        CacheBuilder.newBuilder().expireAfterWrite(EXPIRATION_MINUTES, TimeUnit.MINUTES).build();
  }

  public Map<String, String> getCredentials(
      CredentialsCacheKey key, StorageProviderCredentials storageProviderCredentials) {
    try {
      return credentialCache.get(
          key, () -> credentialProvider.getCredentials(key, storageProviderCredentials));
    } catch (ExecutionException e) {
      throw new RuntimeException("Error during cache lookup for credentials", e);
    }
  }
}
