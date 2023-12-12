package com.linkedin.datahub.graphql.resolvers.post;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.post.PostService;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class DeletePostResolver implements DataFetcher<CompletableFuture<Boolean>> {
  private final PostService _postService;

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();

    if (!AuthorizationUtils.canManageGlobalAnnouncements(context)) {
      throw new AuthorizationException(
          "Unauthorized to delete posts. Please contact your DataHub administrator if this needs corrective action.");
    }

    final Urn postUrn = UrnUtils.getUrn(environment.getArgument("urn"));
    final Authentication authentication = context.getAuthentication();

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            return _postService.deletePost(postUrn, authentication);
          } catch (Exception e) {
            throw new RuntimeException("Failed to create a new post", e);
          }
        });
  }
}
