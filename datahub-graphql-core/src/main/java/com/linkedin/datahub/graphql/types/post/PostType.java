package com.linkedin.datahub.graphql.types.post;

import static com.linkedin.metadata.Constants.POST_INFO_ASPECT_NAME;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.Post;
import com.linkedin.datahub.graphql.types.EntityType;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import graphql.execution.DataFetcherResult;
import io.datahubproject.metadata.services.RestrictedService;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class PostType implements EntityType<Post, String> {
  public static final Set<String> ASPECTS_TO_FETCH = ImmutableSet.of(POST_INFO_ASPECT_NAME);

  private final EntityClient _entityClient;
  @Nullable private final RestrictedService _restrictedService;

  public PostType(final EntityClient entityClient) {
    this(entityClient, null);
  }

  public PostType(
      final EntityClient entityClient, @Nullable final RestrictedService restrictedService) {
    _entityClient = entityClient;
    _restrictedService = restrictedService;
  }

  @Override
  public com.linkedin.datahub.graphql.generated.EntityType type() {
    return com.linkedin.datahub.graphql.generated.EntityType.POST;
  }

  @Override
  public Function<Entity, String> getKeyProvider() {
    return Entity::getUrn;
  }

  @Override
  public Class<Post> objectClass() {
    return Post.class;
  }

  @Override
  public RestrictedService getRestrictedService() {
    return _restrictedService;
  }

  @Override
  public List<DataFetcherResult<Post>> batchLoadWithoutAuthorization(
      @Nonnull List<String> urnStrs, @Nonnull QueryContext context) throws Exception {
    try {
      final Set<Urn> urns = urnStrs.stream().map(UrnUtils::getUrn).collect(Collectors.toSet());

      final Map<Urn, EntityResponse> postMap =
          _entityClient.batchGetV2(
              context.getOperationContext(), Constants.POST_ENTITY_NAME, urns, ASPECTS_TO_FETCH);

      return mapResponsesToBatchResults(urnStrs, postMap, PostMapper::map, context);
    } catch (Exception e) {
      throw new RuntimeException("Failed to batch load Posts", e);
    }
  }
}
