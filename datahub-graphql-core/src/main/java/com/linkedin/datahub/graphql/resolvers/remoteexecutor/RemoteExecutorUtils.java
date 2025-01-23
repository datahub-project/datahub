package com.linkedin.datahub.graphql.resolvers.remoteexecutor;

import static com.linkedin.metadata.AcrylConstants.*;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.executorglobalconfig.RemoteExecutorPoolGlobalConfig;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nullable;

public class RemoteExecutorUtils {

  @Nullable
  public static String tryGetDefaultExecutorPoolId(
      final EntityClient entityClient, OperationContext opContext) throws Exception {
    final EntityResponse maybeGlobalConfig =
        entityClient.getV2(
            opContext,
            REMOTE_EXECUTOR_GLOBAL_CONFIG_ENTITY_NAME,
            UrnUtils.getUrn(REMOTE_EXECUTOR_POOL_GLOBAL_CONFIG_PLATFORM_RESOURCE_URN),
            ImmutableSet.of(REMOTE_EXECUTOR_POOL_GLOBAL_CONFIG_ASPECT_NAME));
    if (maybeGlobalConfig == null) {
      return null;
    }
    final EnvelopedAspect envelopedAspect =
        maybeGlobalConfig.getAspects().get(REMOTE_EXECUTOR_POOL_GLOBAL_CONFIG_ASPECT_NAME);
    if (envelopedAspect == null) {
      return null;
    }
    final RemoteExecutorPoolGlobalConfig config =
        new RemoteExecutorPoolGlobalConfig(envelopedAspect.getValue().data());
    return config.getDefaultPoolName();
  }
}
