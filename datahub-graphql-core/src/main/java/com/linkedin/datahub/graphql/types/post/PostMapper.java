package com.linkedin.datahub.graphql.types.post;

import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.generated.AuditStamp;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.Media;
import com.linkedin.datahub.graphql.generated.MediaType;
import com.linkedin.datahub.graphql.generated.Post;
import com.linkedin.datahub.graphql.generated.PostContent;
import com.linkedin.datahub.graphql.generated.PostContentType;
import com.linkedin.datahub.graphql.generated.PostType;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.post.PostInfo;
import javax.annotation.Nonnull;

import static com.linkedin.metadata.Constants.*;


public class PostMapper implements ModelMapper<EntityResponse, Post> {

  public static final PostMapper INSTANCE = new PostMapper();

  public static Post map(@Nonnull final EntityResponse entityResponse) {
    return INSTANCE.apply(entityResponse);
  }

  @Override
  public Post apply(@Nonnull final EntityResponse entityResponse) {
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
    post.setPostType(PostType.valueOf(postInfo.getType().toString()));
    post.setContent(mapPostContent(postInfo.getContent()));
    AuditStamp lastModified = new AuditStamp();
    lastModified.setTime(postInfo.getLastModified());
    post.setLastModified(lastModified);
  }

  @Nonnull
  private com.linkedin.datahub.graphql.generated.PostContent mapPostContent(
      @Nonnull com.linkedin.post.PostContent postContent) {
    PostContent result = new PostContent();
    result.setContentType(PostContentType.valueOf(postContent.getType().toString()));
    result.setTitle(postContent.getTitle());
    if (postContent.hasDescription()) {
      result.setDescription(postContent.getDescription());
    }
    if (postContent.hasLink()) {
      result.setLink(postContent.getLink().toString());
    }
    if (postContent.hasMedia()) {
      result.setMedia(mapPostMedia(postContent.getMedia()));
    }
    return result;
  }

  @Nonnull
  private Media mapPostMedia(@Nonnull com.linkedin.common.Media postMedia) {
    Media result = new Media();
    result.setType(MediaType.valueOf(postMedia.getType().toString()));
    result.setLocation(postMedia.getLocation().toString());
    return result;
  }
}
