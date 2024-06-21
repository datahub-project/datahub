package com.linkedin.datahub.graphql.types.common.mappers;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataHubConnection;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.Share;
import com.linkedin.datahub.graphql.generated.ShareConfig;
import com.linkedin.datahub.graphql.generated.ShareResult;
import com.linkedin.datahub.graphql.generated.ShareResultState;
import com.linkedin.datahub.graphql.types.mappers.MapperUtils;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class ShareMapper implements ModelMapper<com.linkedin.common.Share, Share> {

  public static final ShareMapper INSTANCE = new ShareMapper();

  public static Share map(
      @Nullable final QueryContext context, @Nonnull final com.linkedin.common.Share metadata) {
    return INSTANCE.apply(context, metadata);
  }

  @Override
  public Share apply(
      @Nullable final QueryContext context, @Nonnull final com.linkedin.common.Share input) {
    final Share result = new Share();
    result.setLastShareResults(
        input.getLastShareResults().stream()
            .map(r -> mapShareResult(context, r))
            .collect(Collectors.toList()));
    if (input.getLastUnshareResults() != null) {
      result.setLastUnshareResults(
          input.getLastUnshareResults().stream()
              .map(r -> mapShareResult(context, r))
              .collect(Collectors.toList()));
    }
    return result;
  }

  private ShareResult mapShareResult(
      @Nullable final QueryContext context,
      @Nonnull final com.linkedin.common.ShareResult shareResult) {
    final ShareResult result = new ShareResult();
    result.setDestination(mapConnectionEntity(shareResult.getDestination()));
    result.setLastAttempt(MapperUtils.mapResolvedAuditStamp(shareResult.getLastAttempt()));
    result.setCreated(MapperUtils.mapResolvedAuditStamp(shareResult.getCreated()));
    if (shareResult.getLastSuccess() != null) {
      result.setLastSuccess(MapperUtils.mapResolvedAuditStamp(shareResult.getLastSuccess()));
    }
    result.setStatus(ShareResultState.valueOf(shareResult.getStatus().toString()));
    if (shareResult.getImplicitShareEntity() != null) {
      result.setImplicitShareEntity(
          UrnToEntityMapper.map(context, shareResult.getImplicitShareEntity()));
    }
    if (shareResult.getMessage() != null) {
      result.setMessage(shareResult.getMessage());
    }
    if (shareResult.getShareConfig() != null) {
      result.setShareConfig(mapShareConfig(shareResult.getShareConfig()));
    }
    if (shareResult.getStatusLastUpdated() != null) {
      result.setStatusLastUpdated(shareResult.getStatusLastUpdated());
    }
    return result;
  }

  private DataHubConnection mapConnectionEntity(@Nonnull final Urn urn) {
    DataHubConnection connection = new DataHubConnection();
    connection.setUrn(urn.toString());
    connection.setType(EntityType.DATAHUB_CONNECTION);
    return connection;
  }

  private ShareConfig mapShareConfig(@Nonnull final com.linkedin.common.ShareConfig config) {
    final ShareConfig resultConfig = new ShareConfig();
    if (config.hasEnableUpstreamLineage()) {
      resultConfig.setEnableUpstreamLineage((config.isEnableUpstreamLineage()));
    }
    if (config.hasEnableDownstreamLineage()) {
      resultConfig.setEnableDownstreamLineage((config.isEnableDownstreamLineage()));
    }
    return resultConfig;
  }
}
