package com.linkedin.entity.client;

import com.datahub.authentication.Authentication;
import com.linkedin.metadata.config.cache.client.EntityClientCacheConfig;
import com.linkedin.parseq.retry.backoff.BackoffPolicy;
import com.linkedin.restli.client.Client;
import lombok.Getter;

import javax.annotation.Nonnull;

/**
 * Restli backed SystemEntityClient
 */
@Getter
public class SystemRestliEntityClient extends RestliEntityClient implements SystemEntityClient {
    private final EntityClientCache entityClientCache;
    private final Authentication systemAuthentication;

    public SystemRestliEntityClient(@Nonnull final Client restliClient, @Nonnull final BackoffPolicy backoffPolicy, int retryCount,
                                    Authentication systemAuthentication, EntityClientCacheConfig cacheConfig) {
        super(restliClient, backoffPolicy, retryCount);
        this.systemAuthentication = systemAuthentication;
        this.entityClientCache = buildEntityClientCache(SystemRestliEntityClient.class, systemAuthentication, cacheConfig);
    }
}
