package com.datahub.iceberg.catalog;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.linkedin.metadata.authorization.PoliciesConfig;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;

public abstract class CredentialProvider {
  private static final int CREDS_DURATION_SECS = 15 * 60;

  @EqualsAndHashCode
  @AllArgsConstructor
  public static class CredentialsCacheKey {
    public final String platformInstance;
    public final PoliciesConfig.Privilege privilege;
    public final Set<String> locations;
  }

  @AllArgsConstructor
  public static class StorageProviderCredentials {
    public final String clientId;
    public final String clientSecret;
    public final String role;
    public final String region;
  }

  private final Cache<CredentialsCacheKey, Map<String, String>> credentialCache;

  public CredentialProvider() {
    this.credentialCache =
        CacheBuilder.newBuilder().expireAfterWrite(CREDS_DURATION_SECS, TimeUnit.SECONDS).build();
  }

  public Map<String, String> get(
      CredentialsCacheKey key, StorageProviderCredentials storageProviderCredentials) {
    try {
      return credentialCache.get(key, () -> loadItem(key, storageProviderCredentials));
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  protected abstract Map<String, String> loadItem(
      CredentialsCacheKey key, StorageProviderCredentials storageProviderCredentials);
}
