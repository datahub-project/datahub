package com.linkedin.datahub.graphql.types.post;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AuditStamp;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.Media;
import com.linkedin.datahub.graphql.generated.MediaType;
import com.linkedin.datahub.graphql.generated.Post;
import com.linkedin.datahub.graphql.generated.PostContent;
import com.linkedin.datahub.graphql.generated.PostContentType;
import com.linkedin.datahub.graphql.generated.PostType;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.datahub.graphql.types.common.mappers.util.UnknownEnumValues;
import com.linkedin.datahub.graphql.types.common.mappers.util.UnrepresentableValueException;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.utils.UnknownDataGuard;
import com.linkedin.post.PostInfo;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class PostMapper implements ModelMapper<EntityResponse, Post> {

  public static final PostMapper INSTANCE = new PostMapper();

  private static final UnknownDataGuard UNREPRESENTABLE =
      UnknownDataGuard.forSite(PostMapper.class, "post");

  public static Post map(
      @Nullable final QueryContext context, @Nonnull final EntityResponse entityResponse) {
    return INSTANCE.apply(context, entityResponse);
  }

  /**
   * As {@link #map}, or null for a post whose type or content type only a newer version defines
   * (read after a rollback). Both are required, so the post is left out (and logged) instead of
   * failing every post in the request.
   */
  @Nullable
  public static Post mapIfRepresentable(
      @Nullable final QueryContext context, @Nonnull final EntityResponse entityResponse) {
    try {
      return map(context, entityResponse);
    } catch (UnrepresentableValueException e) {
      UNREPRESENTABLE.skippedBecause(
          Optional.empty(), "post", e.getMessage(), entityResponse.getUrn());
      return null;
    }
  }

  @Override
  public Post apply(
      @Nullable final QueryContext context, @Nonnull final EntityResponse entityResponse) {
    final Post result = new Post();

    result.setUrn(entityResponse.getUrn().toString());
    result.setType(EntityType.POST);
    EnvelopedAspectMap aspectMap = entityResponse.getAspects();
    MappingHelper<Post> mappingHelper = new MappingHelper<>(aspectMap, result);
    mappingHelper.mapToResult(POST_INFO_ASPECT_NAME, this::mapPostInfo);
    return mappingHelper.getResult();
  }

  private void mapPostInfo(@Nonnull Post post, @Nonnull DataMap dataMap) {
    PostInfo postInfo = new PostInfo(dataMap);
    post.setPostType(UnknownEnumValues.require(PostType.class, postInfo.getType()));
    post.setContent(mapPostContent(postInfo.getContent()));
    AuditStamp lastModified = new AuditStamp();
    lastModified.setTime(postInfo.getLastModified());
    if (postInfo.hasAuditStamp()
        && postInfo.getAuditStamp() != null
        && postInfo.getAuditStamp().hasActor()) {
      lastModified.setActor(postInfo.getAuditStamp().getActor().toString());
    }
    post.setLastModified(lastModified);
  }

  @Nonnull
  private com.linkedin.datahub.graphql.generated.PostContent mapPostContent(
      @Nonnull com.linkedin.post.PostContent postContent) {
    PostContent result = new PostContent();
    result.setContentType(UnknownEnumValues.require(PostContentType.class, postContent.getType()));
    result.setTitle(postContent.getTitle());
    if (postContent.hasDescription()) {
      result.setDescription(postContent.getDescription());
    }
    if (postContent.hasLink()) {
      result.setLink(postContent.getLink().toString());
    }
    if (postContent.hasMedia()) {
      // Media.type is required; media of a type only a newer version defines is left out.
      result.setMedia(mapPostMedia(postContent.getMedia()));
    }
    return result;
  }

  @Nullable
  private Media mapPostMedia(@Nonnull com.linkedin.common.Media postMedia) {
    final MediaType type = UnknownEnumValues.mapOrNull(MediaType.class, postMedia.getType());
    if (type == null) {
      return null;
    }
    Media result = new Media();
    result.setType(type);
    result.setLocation(postMedia.getLocation().toString());
    return result;
  }
}
