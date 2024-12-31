package com.linkedin.datahub.graphql.resolvers.post;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.post.PostService;
import com.linkedin.common.Media;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.*;
import com.linkedin.metadata.utils.SchemaFieldUtils;
import com.linkedin.post.PostContent;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class CreatePostResolver implements DataFetcher<CompletableFuture<Boolean>> {
  private final PostService _postService;

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();

    if (!AuthorizationUtils.canCreateGlobalAnnouncements(context)) {
      throw new AuthorizationException(
          "Unauthorized to create posts. Please contact your DataHub administrator if this needs corrective action.");
    }

    final CreatePostInput input =
        bindArgument(environment.getArgument("input"), CreatePostInput.class);
    final PostType type = input.getPostType();
    final UpdatePostContentInput content = input.getContent();
    final PostContentType contentType = content.getContentType();
    final String title = content.getTitle();
    final String link = content.getLink();
    final String description = content.getDescription();
    final UpdateMediaInput updateMediaInput = content.getMedia();
    final Authentication authentication = context.getAuthentication();
    final String targetResource = input.getResourceUrn();
    final String targetSubresource = input.getSubResource();

    String targetUrn;
    if (targetSubresource != null) {
      targetUrn =
          SchemaFieldUtils.generateSchemaFieldUrn(
                  UrnUtils.getUrn(targetResource), targetSubresource)
              .toString();
    } else {
      targetUrn = targetResource;
    }

    Media media =
        updateMediaInput == null
            ? null
            : _postService.mapMedia(
                updateMediaInput.getType().toString(), updateMediaInput.getLocation());
    PostContent postContent =
        _postService.mapPostContent(contentType.toString(), title, description, link, media);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            return _postService.createPost(
                context.getOperationContext(), type.toString(), postContent, targetUrn);
          } catch (Exception e) {
            throw new RuntimeException("Failed to create a new post", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
