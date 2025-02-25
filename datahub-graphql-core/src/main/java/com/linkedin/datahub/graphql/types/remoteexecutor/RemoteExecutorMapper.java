package com.linkedin.datahub.graphql.types.remoteexecutor;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.RemoteExecutor;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.executor.RemoteExecutorStatus;
import com.linkedin.metadata.AcrylConstants;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class RemoteExecutorMapper {
  public static RemoteExecutor map(
      @Nullable QueryContext context, final EntityResponse entityResponse) {
    final Urn urn = entityResponse.getUrn();
    final EnvelopedAspectMap aspects = entityResponse.getAspects();

    final RemoteExecutor executor = new RemoteExecutor();
    executor.setUrn(urn.toString());
    executor.setType(EntityType.REMOTE_EXECUTOR);
    MappingHelper<RemoteExecutor> mappingHelper = new MappingHelper<>(aspects, executor);

    mappingHelper.mapToResult(
        AcrylConstants.REMOTE_EXECUTOR_STATUS_ASPECT_NAME, RemoteExecutorMapper::mapStatus);

    return mappingHelper.getResult();
  }

  private static void mapStatus(@Nonnull RemoteExecutor executor, @Nonnull DataMap dataMap) {
    final RemoteExecutorStatus status = new RemoteExecutorStatus(dataMap);
    executor.setExecutorPoolId(status.getExecutorPoolId());
    executor.setExecutorReleaseVersion(status.getExecutorReleaseVersion());
    executor.setExecutorAddress(status.getExecutorAddress());
    executor.setExecutorHostname(status.getExecutorHostname());
    executor.setExecutorUptime(status.getExecutorUptime());
    executor.setExecutorExpired(status.isExecutorExpired());
    executor.setExecutorStopped(status.isExecutorStopped());
    executor.setExecutorEmbedded(status.isExecutorEmbedded());
    executor.setExecutorInternal(status.isExecutorInternal());
    executor.setLogDeliveryEnabled(status.isLogDeliveryEnabled());
    executor.setReportedAt(status.getReportedAt());
  }
}
