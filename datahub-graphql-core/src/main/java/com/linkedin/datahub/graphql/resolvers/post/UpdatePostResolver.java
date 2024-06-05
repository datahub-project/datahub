package com.linkedin.datahub.graphql.resolvers.post;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.post.PostService;
import com.linkedin.common.Media;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.PostContentType;
import com.linkedin.datahub.graphql.generated.PostType;
import com.linkedin.datahub.graphql.generated.UpdateMediaInput;
import com.linkedin.datahub.graphql.generated.UpdatePostContentInput;
import com.linkedin.datahub.graphql.generated.UpdatePostInput;
import com.linkedin.post.PostContent;
import graphql.GraphQLException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class UpdatePostResolver implements DataFetcher<CompletableFuture<Boolean>> {
  private final PostService postService;

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();

    if (!AuthorizationUtils.canCreateGlobalAnnouncements(context)) {
      throw new AuthorizationException(
          "Unauthorized to update posts. Please contact your DataHub administrator if this needs corrective action.");
    }

    final UpdatePostInput input =
        bindArgument(environment.getArgument("input"), UpdatePostInput.class);
    final Urn postUrn = Urn.createFromString(input.getUrn());

    final PostType type = input.getPostType();
    final UpdatePostContentInput content = input.getContent();
    final PostContentType contentType = content.getContentType();
    final String title = content.getTitle();
    final String link = content.getLink();
    final String description = content.getDescription();
    final UpdateMediaInput updateMediaInput = content.getMedia();
    final Authentication authentication = context.getAuthentication();

    Media media =
        updateMediaInput == null
            ? null
            : postService.mapMedia(
                updateMediaInput.getType().toString(), updateMediaInput.getLocation());
    PostContent postContent =
        postService.mapPostContent(contentType.toString(), title, description, link, media);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            return postService.updatePost(
                context.getOperationContext(), postUrn, type.toString(), postContent);
          } catch (Exception e) {
            throw new GraphQLException("Failed to update or edit post", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
