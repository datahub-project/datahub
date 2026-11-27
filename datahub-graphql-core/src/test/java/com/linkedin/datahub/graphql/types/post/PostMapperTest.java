package com.linkedin.datahub.graphql.types.post;

import static com.linkedin.metadata.Constants.*;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.generated.Post;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.utils.StrictHandling;
import com.linkedin.metadata.utils.TolerantHandling;
import com.linkedin.metadata.utils.UnknownDataHandlingsForTests;
import com.linkedin.post.PostContent;
import com.linkedin.post.PostContentType;
import com.linkedin.post.PostInfo;
import com.linkedin.post.PostType;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;

public class PostMapperTest {

  private static final Urn POST_URN = UrnUtils.getUrn("urn:li:post:test-123");
  private static final Urn ACTOR_URN = UrnUtils.getUrn("urn:li:corpuser:testUser");
  private static final long CREATED_TIME = 1_000L;
  private static final long LAST_MODIFIED_TIME = 2_000L;

  private EntityResponse buildResponse(PostInfo postInfo) throws Exception {
    Map<String, EnvelopedAspect> aspects = new HashMap<>();
    aspects.put(POST_INFO_ASPECT_NAME, new EnvelopedAspect().setValue(new Aspect(postInfo.data())));
    return new EntityResponse().setUrn(POST_URN).setAspects(new EnvelopedAspectMap(aspects));
  }

  @Test
  public void testPostWithTypeFromNewerVersionIsLeftOut() throws Exception {
    // The post type is required, so a post whose type only a newer version defines is left out
    // instead of failing every post in the request; media of an unknown type is left out alone.
    UnknownDataHandlingsForTests.use(TolerantHandling.INSTANCE);
    try {
      PostInfo newerType = basicPostInfo();
      newerType.data().put("type", "POST_TYPE_FROM_NEWER_BUILD");
      assertNull(PostMapper.mapIfRepresentable(null, buildResponse(newerType)));

      PostInfo newerMedia = basicPostInfo();
      newerMedia
          .getContent()
          .setMedia(
              new com.linkedin.common.Media()
                  .setType(com.linkedin.common.MediaType.IMAGE)
                  .setLocation(new com.linkedin.common.url.Url("https://example.com/a.png")));
      newerMedia.getContent().getMedia().data().put("type", "MEDIA_TYPE_FROM_NEWER_BUILD");
      Post post = PostMapper.mapIfRepresentable(null, buildResponse(newerMedia));
      assertNotNull(post);
      assertNull(post.getContent().getMedia());
    } finally {
      UnknownDataHandlingsForTests.reset();
    }
  }

  @Test
  public void testPostWithTypeFromNewerVersionStrictHandlingFails() throws Exception {
    UnknownDataHandlingsForTests.use(StrictHandling.INSTANCE);
    try {
      PostInfo newerType = basicPostInfo();
      newerType.data().put("type", "POST_TYPE_FROM_NEWER_BUILD");
      EntityResponse response = buildResponse(newerType);

      expectThrows(
          IllegalArgumentException.class, () -> PostMapper.mapIfRepresentable(null, response));
    } finally {
      UnknownDataHandlingsForTests.reset();
    }
  }

  private static PostInfo basicPostInfo() {
    return new PostInfo()
        .setType(PostType.HOME_PAGE_ANNOUNCEMENT)
        .setContent(new PostContent().setType(PostContentType.TEXT).setTitle("Hello"))
        .setCreated(CREATED_TIME)
        .setLastModified(LAST_MODIFIED_TIME);
  }
}
