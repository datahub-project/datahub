package com.linkedin.datahub.graphql.types.post;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.canView;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class PostType implements EntityType<Post, String> {
  public static final Set<String> ASPECTS_TO_FETCH = ImmutableSet.of(POST_INFO_ASPECT_NAME);

  private final EntityClient _entityClient;

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
  public List<DataFetcherResult<Post>> batchLoad(
      @Nonnull List<String> urns, @Nonnull QueryContext context) throws Exception {
    try {
      final List<Urn> postUrns = urns.stream().map(UrnUtils::getUrn).collect(Collectors.toList());

      final Map<Urn, EntityResponse> postMap =
          _entityClient.batchGetV2(
              context.getOperationContext(),
              Constants.POST_ENTITY_NAME,
              postUrns.stream()
                  .filter(urn -> canView(context.getOperationContext(), urn))
                  .collect(Collectors.toSet()),
              ASPECTS_TO_FETCH);

      final List<EntityResponse> gmsResults = new ArrayList<>(urns.size());
      for (Urn urn : postUrns) {
        gmsResults.add(postMap.getOrDefault(urn, null));
      }

      return gmsResults.stream()
          .map(
              gmsPost ->
                  gmsPost == null
                      ? null
                      : DataFetcherResult.<Post>newResult()
                          .data(PostMapper.map(context, gmsPost))
                          .build())
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new RuntimeException("Failed to batch load Queries", e);
    }
  }
}
