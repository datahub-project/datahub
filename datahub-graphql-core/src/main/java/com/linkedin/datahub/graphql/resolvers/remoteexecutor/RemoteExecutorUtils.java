package com.linkedin.datahub.graphql.resolvers.remoteexecutor;

import static com.linkedin.metadata.AcrylConstants.*;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.executorglobalconfig.RemoteExecutorPoolGlobalConfig;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class RemoteExecutorUtils {

  @Nullable
  public static String tryGetDefaultExecutorPoolId(
      final EntityClient entityClient, OperationContext opContext) throws Exception {
    final RemoteExecutorPoolGlobalConfig config =
        tryGetExecutorPoolGlobalConfig(entityClient, opContext);
    return config != null ? config.getDefaultExecutorPoolId() : null;
  }

  @Nullable
  public static RemoteExecutorPoolGlobalConfig tryGetExecutorPoolGlobalConfig(
      final EntityClient entityClient, OperationContext opContext)
      throws RemoteInvocationException, URISyntaxException {
    final EntityResponse maybeGlobalConfig =
        entityClient.getV2(
            opContext,
            REMOTE_EXECUTOR_GLOBAL_CONFIG_ENTITY_NAME,
            UrnUtils.getUrn(REMOTE_EXECUTOR_POOL_GLOBAL_CONFIG_SINGLETON_URN),
            ImmutableSet.of(REMOTE_EXECUTOR_POOL_GLOBAL_CONFIG_ASPECT_NAME));
    if (maybeGlobalConfig == null) {
      return null;
    }
    final EnvelopedAspect envelopedAspect =
        maybeGlobalConfig.getAspects().get(REMOTE_EXECUTOR_POOL_GLOBAL_CONFIG_ASPECT_NAME);
    if (envelopedAspect == null) {
      return null;
    }
    return new RemoteExecutorPoolGlobalConfig(envelopedAspect.getValue().data());
  }

  public static void updateDefaultRemoteExecutorPool(
      @Nonnull final EntityClient entityClient,
      @Nonnull final OperationContext opContext,
      @Nullable final String poolName)
      throws RemoteInvocationException, URISyntaxException {
    RemoteExecutorPoolGlobalConfig globalConfig =
        RemoteExecutorUtils.tryGetExecutorPoolGlobalConfig(entityClient, opContext);
    if (globalConfig == null) {
      globalConfig = new RemoteExecutorPoolGlobalConfig();
    }
    if (poolName == null) {
      globalConfig.removeDefaultExecutorPoolId();
    } else {
      globalConfig.setDefaultExecutorPoolId(poolName);
    }
    entityClient.ingestProposal(
        opContext,
        AspectUtils.buildMetadataChangeProposal(
            UrnUtils.getUrn(REMOTE_EXECUTOR_POOL_GLOBAL_CONFIG_SINGLETON_URN),
            REMOTE_EXECUTOR_POOL_GLOBAL_CONFIG_ASPECT_NAME,
            globalConfig),
        false);
  }
}
