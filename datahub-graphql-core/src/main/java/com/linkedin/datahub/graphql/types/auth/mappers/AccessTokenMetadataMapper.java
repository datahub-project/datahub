package com.linkedin.datahub.graphql.types.auth.mappers;

import com.linkedin.access.token.DataHubAccessTokenInfo;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.generated.AccessTokenMetadata;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import javax.annotation.Nonnull;

public class AccessTokenMetadataMapper implements ModelMapper<EntityResponse, AccessTokenMetadata> {

  public static final AccessTokenMetadataMapper INSTANCE = new AccessTokenMetadataMapper();

  public static AccessTokenMetadata map(@Nonnull final EntityResponse entityResponse) {
    return INSTANCE.apply(entityResponse);
  }

  @Override
  public AccessTokenMetadata apply(@Nonnull final EntityResponse input) {

    final AccessTokenMetadata metadata = new AccessTokenMetadata();
    metadata.setUrn(input.getUrn().toString());
    metadata.setId(input.getUrn().getId());
    metadata.setType(EntityType.ACCESS_TOKEN);

    final EnvelopedAspectMap aspectMap = input.getAspects();
    final MappingHelper<AccessTokenMetadata> mappingHelper =
        new MappingHelper<>(aspectMap, metadata);
    mappingHelper.mapToResult(Constants.ACCESS_TOKEN_INFO_NAME, this::mapTokenInfo);

    return mappingHelper.getResult();
  }

  private void mapTokenInfo(
      @Nonnull final AccessTokenMetadata accessTokenMetadata, @Nonnull final DataMap dataMap) {
    final DataHubAccessTokenInfo tokenInfo = new DataHubAccessTokenInfo(dataMap);

    accessTokenMetadata.setName(tokenInfo.getName());
    accessTokenMetadata.setActorUrn(tokenInfo.getActorUrn().toString());
    accessTokenMetadata.setOwnerUrn(tokenInfo.getOwnerUrn().toString());
    accessTokenMetadata.setCreatedAt(tokenInfo.getCreatedAt());
    accessTokenMetadata.setExpiresAt(tokenInfo.getExpiresAt());
    accessTokenMetadata.setDescription(tokenInfo.getDescription());
  }
}
