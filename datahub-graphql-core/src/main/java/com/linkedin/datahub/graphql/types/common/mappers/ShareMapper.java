package com.linkedin.datahub.graphql.types.common.mappers;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.DataHubConnection;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.Share;
import com.linkedin.datahub.graphql.generated.ShareResult;
import com.linkedin.datahub.graphql.generated.ShareResultState;
import com.linkedin.datahub.graphql.types.mappers.MapperUtils;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class ShareMapper implements ModelMapper<com.linkedin.common.Share, Share> {

  public static final ShareMapper INSTANCE = new ShareMapper();

  public static Share map(@Nonnull final com.linkedin.common.Share metadata) {
    return INSTANCE.apply(metadata);
  }

  @Override
  public Share apply(@Nonnull final com.linkedin.common.Share input) {
    final Share result = new Share();
    result.setLastShareResults(
        input.getLastShareResults().stream()
            .map(this::mapShareResult)
            .collect(Collectors.toList()));
    return result;
  }

  private ShareResult mapShareResult(@Nonnull final com.linkedin.common.ShareResult shareResult) {
    final ShareResult result = new ShareResult();
    result.setDestination(mapConnectionEntity(shareResult.getDestination()));
    result.setLastAttempt(MapperUtils.mapResolvedAuditStamp(shareResult.getLastAttempt()));
    result.setCreated(MapperUtils.mapResolvedAuditStamp(shareResult.getCreated()));
    if (shareResult.getLastSuccess() != null) {
      result.setLastSuccess(MapperUtils.mapResolvedAuditStamp(shareResult.getLastSuccess()));
    }
    result.setStatus(ShareResultState.valueOf(shareResult.getStatus().toString()));
    if (shareResult.getImplicitShareEntity() != null) {
      result.setImplicitShareEntity(UrnToEntityMapper.map(shareResult.getImplicitShareEntity()));
    }
    if (shareResult.getMessage() != null) {
      result.setMessage(shareResult.getMessage());
    }
    return result;
  }

  private DataHubConnection mapConnectionEntity(@Nonnull final Urn urn) {
    DataHubConnection connection = new DataHubConnection();
    connection.setUrn(urn.toString());
    connection.setType(EntityType.DATAHUB_CONNECTION);
    return connection;
  }
}
