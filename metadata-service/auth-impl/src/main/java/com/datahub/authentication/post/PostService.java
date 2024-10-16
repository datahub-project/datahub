package com.datahub.authentication.post;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.entity.AspectUtils.*;
import static com.linkedin.metadata.utils.SystemMetadataUtils.createDefaultSystemMetadata;

import com.linkedin.common.Media;
import com.linkedin.common.MediaType;
import com.linkedin.common.url.Url;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.entity.validation.ValidationException;
import com.linkedin.metadata.key.PostKey;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.post.PostContent;
import com.linkedin.post.PostContentType;
import com.linkedin.post.PostInfo;
import com.linkedin.post.PostType;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class PostService {
  private final EntityClient _entityClient;

  @Nonnull
  public Media mapMedia(@Nonnull String type, @Nonnull String location) {
    final Media media = new Media();
    media.setType(MediaType.valueOf(type));
    media.setLocation(new Url(location));
    return media;
  }

  @Nonnull
  public PostContent mapPostContent(
      @Nonnull String contentType,
      @Nonnull String title,
      @Nullable String description,
      @Nullable String link,
      @Nullable Media media) {
    final PostContent postContent =
        new PostContent().setType(PostContentType.valueOf(contentType)).setTitle(title);
    if (description != null) {
      postContent.setDescription(description);
    }
    if (link != null) {
      postContent.setLink(new Url(link));
    }
    if (media != null) {
      postContent.setMedia(media);
    }
    return postContent;
  }

  public boolean createPost(
      @Nonnull OperationContext opContext,
      @Nonnull String postType,
      @Nonnull PostContent postContent,
      @Nullable String targetUrn)
      throws RemoteInvocationException, URISyntaxException {
    final String uuid = UUID.randomUUID().toString();
    final PostKey postKey = new PostKey().setId(uuid);
    final long currentTimeMillis = Instant.now().toEpochMilli();
    final PostInfo postInfo =
        new PostInfo()
            .setType(PostType.valueOf(postType))
            .setContent(postContent)
            .setCreated(currentTimeMillis)
            .setAuditStamp(
                new com.linkedin.common.AuditStamp()
                    .setTime(currentTimeMillis)
                    .setActor(
                        Urn.createFromString(
                            opContext.getSessionAuthentication().getActor().toUrnStr())))
            .setLastModified(currentTimeMillis);

    if (targetUrn != null) {
      try {
        postInfo.setTarget(Urn.createFromString(targetUrn));
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }

    final MetadataChangeProposal proposal =
        buildMetadataChangeProposal(POST_ENTITY_NAME, postKey, POST_INFO_ASPECT_NAME, postInfo);

    proposal.setSystemMetadata(createDefaultSystemMetadata());

    _entityClient.ingestProposal(opContext, proposal);

    return true;
  }

  public boolean deletePost(@Nonnull OperationContext opContext, @Nonnull Urn postUrn)
      throws RemoteInvocationException {
    if (!_entityClient.exists(opContext, postUrn)) {
      throw new RuntimeException("Post does not exist");
    }
    _entityClient.deleteEntity(opContext, postUrn);
    return true;
  }

  public boolean updatePost(
      @Nonnull OperationContext opContext,
      @Nonnull Urn postUrn,
      @Nonnull String postType,
      @Nonnull PostContent updatedContent)
      throws RemoteInvocationException, URISyntaxException {

    final EntityResponse response =
        _entityClient.getV2(
            opContext, postUrn.getEntityType(), postUrn, Set.of(POST_INFO_ASPECT_NAME));
    if (response == null || !response.getAspects().containsKey(POST_INFO_ASPECT_NAME)) {
      throw new ValidationException(
          String.format("Failed to edit/update post for urn %s as post doesn't exist", postUrn));
    }

    final DataMap dataMap = response.getAspects().get(POST_INFO_ASPECT_NAME).getValue().data();
    final PostInfo existingPost = new PostInfo(dataMap);

    // update/edit existing post
    existingPost.setContent(updatedContent);
    existingPost.setType(PostType.valueOf(postType));
    final long currentTimeMillis = Instant.now().toEpochMilli();
    existingPost.setLastModified(currentTimeMillis);

    final MetadataChangeProposal proposal =
        buildMetadataChangeProposal(postUrn, POST_INFO_ASPECT_NAME, existingPost);
    _entityClient.ingestProposal(opContext, proposal, false);

    return true;
  }
}
