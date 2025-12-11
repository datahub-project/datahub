/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.entity.client;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.metadata.config.cache.client.EntityClientCacheConfig;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.Client;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;

/** Restli backed SystemEntityClient */
@Getter
public class SystemRestliEntityClient extends RestliEntityClient implements SystemEntityClient {
  private final EntityClientCache entityClientCache;
  private final Cache<String, OperationContext> operationContextMap;

  public SystemRestliEntityClient(
      @Nonnull final Client restliClient,
      @Nonnull EntityClientConfig clientConfig,
      EntityClientCacheConfig cacheConfig,
      MetricUtils metricUtils) {
    super(restliClient, clientConfig, metricUtils);
    this.operationContextMap = CacheBuilder.newBuilder().maximumSize(500).build();
    this.entityClientCache =
        buildEntityClientCache(metricUtils, SystemRestliEntityClient.class, cacheConfig);
  }

  @Nullable
  @Override
  public EntityResponse getV2(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull Urn urn,
      @Nullable Set<String> aspectNames)
      throws RemoteInvocationException, URISyntaxException {
    return getV2(opContext, urn, aspectNames);
  }

  @Nonnull
  @Override
  public Map<Urn, EntityResponse> batchGetV2(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull Set<Urn> urns,
      @Nullable Set<String> aspectNames)
      throws RemoteInvocationException, URISyntaxException {
    return batchGetV2(opContext, urns, aspectNames);
  }

  @Override
  public Map<Urn, EntityResponse> batchGetV2NoCache(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull Set<Urn> urns,
      @Nullable Set<String> aspectNames)
      throws RemoteInvocationException, URISyntaxException {
    return super.batchGetV2(opContext, entityName, urns, aspectNames, false);
  }
}
