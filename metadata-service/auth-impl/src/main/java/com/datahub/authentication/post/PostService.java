package com.datahub.authentication.post;

import com.datahub.authentication.Authentication;
import com.linkedin.common.Media;
import com.linkedin.common.MediaType;
import com.linkedin.common.url.Url;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.key.PostKey;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.post.PostContent;
import com.linkedin.post.PostContentType;
import com.linkedin.post.PostInfo;
import com.linkedin.post.PostType;
import com.linkedin.r2.RemoteInvocationException;
import java.time.Instant;
import java.util.UUID;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.entity.AspectUtils.*;


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
  public PostContent mapPostContent(@Nonnull String contentType, @Nonnull String title, @Nullable String description, @Nullable String link,
      @Nullable Media media) {
    final PostContent postContent = new PostContent().setType(PostContentType.valueOf(contentType)).setTitle(title);
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

  public boolean createPost(@Nonnull String postType, @Nonnull PostContent postContent,
      @Nonnull Authentication authentication) throws RemoteInvocationException {
    final String uuid = UUID.randomUUID().toString();
    final PostKey postKey = new PostKey().setId(uuid);
    final long currentTimeMillis = Instant.now().toEpochMilli();
    final PostInfo postInfo = new PostInfo().setType(PostType.valueOf(postType))
        .setContent(postContent)
        .setCreated(currentTimeMillis)
        .setLastModified(currentTimeMillis);

    final MetadataChangeProposal proposal =
        buildMetadataChangeProposal(POST_ENTITY_NAME, postKey, POST_INFO_ASPECT_NAME, postInfo);
    _entityClient.ingestProposal(proposal, authentication);

    return true;
  }
}
