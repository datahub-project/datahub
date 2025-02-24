package com.linkedin.datahub.graphql.types.remoteexecutor;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.RemoteExecutorPool;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.executorpool.RemoteExecutorPoolInfo;
import com.linkedin.metadata.AcrylConstants;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class RemoteExecutorPoolMapper {
  public static RemoteExecutorPool map(
      @Nullable QueryContext context,
      final EntityResponse entityResponse,
      @Nullable final String defaultPoolId) {
    final Urn urn = entityResponse.getUrn();
    final EnvelopedAspectMap aspects = entityResponse.getAspects();

    final RemoteExecutorPool executor = new RemoteExecutorPool();
    executor.setUrn(urn.toString());
    executor.setType(EntityType.REMOTE_EXECUTOR_POOL);
    executor.setPoolName(urn.getId());
    executor.setIsDefault(urn.getId().equals(defaultPoolId));

    MappingHelper<RemoteExecutorPool> helper = new MappingHelper<>(aspects, executor);

    helper.mapToResult(
        AcrylConstants.REMOTE_EXECUTOR_POOL_INFO_ASPECT_NAME, RemoteExecutorPoolMapper::mapInfo);

    return executor;
  }

  private static void mapInfo(@Nonnull RemoteExecutorPool executor, @Nonnull DataMap dataMap) {
    final RemoteExecutorPoolInfo info = new RemoteExecutorPoolInfo(dataMap);
    executor.setCreatedAt(info.getCreatedAt());
    if (info.hasDescription()) {
      executor.setDescription(info.getDescription());
    }
  }
}
