package com.datahub.authentication.post;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.linkedin.common.Media;
import com.linkedin.common.MediaType;
import com.linkedin.common.url.Url;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.post.PostContent;
import com.linkedin.post.PostContentType;
import com.linkedin.post.PostType;
import com.linkedin.r2.RemoteInvocationException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class PostServiceTest {
  private static final MediaType POST_MEDIA_TYPE = MediaType.IMAGE;
  private static final String POST_MEDIA_LOCATION =
      "https://datahubproject.io/img/datahub-logo-color-light-horizontal.svg";
  private static final PostContentType POST_CONTENT_TYPE = PostContentType.LINK;
  private static final String POST_TITLE = "title";
  private static final String POST_DESCRIPTION = "description";
  private static final String POST_LINK = "https://datahubproject.io";
  private static final Media MEDIA = new Media().setType(POST_MEDIA_TYPE).setLocation(new Url(POST_MEDIA_LOCATION));
  private static final PostContent POST_CONTENT = new PostContent().setType(POST_CONTENT_TYPE)
      .setTitle(POST_TITLE)
      .setDescription(POST_DESCRIPTION)
      .setLink(new Url(POST_LINK))
      .setMedia(MEDIA);
  private static final PostType POST_TYPE = PostType.HOME_PAGE_ANNOUNCEMENT;
  private static final String DATAHUB_SYSTEM_CLIENT_ID = "__datahub_system";
  private static final Authentication SYSTEM_AUTHENTICATION =
      new Authentication(new Actor(ActorType.USER, DATAHUB_SYSTEM_CLIENT_ID), "");
  private EntityClient _entityClient;
  private PostService _postService;

  @BeforeMethod
  public void setupTest() {
    _entityClient = mock(EntityClient.class);
    _postService = new PostService(_entityClient);
  }

  @Test
  public void testMapMedia() {
    Media media = _postService.mapMedia(POST_MEDIA_TYPE.toString(), POST_MEDIA_LOCATION);
    assertEquals(MEDIA, media);
  }

  @Test
  public void testMapPostContent() {
    PostContent postContent =
        _postService.mapPostContent(POST_CONTENT_TYPE.toString(), POST_TITLE, POST_DESCRIPTION, POST_LINK, MEDIA);
    assertEquals(POST_CONTENT, postContent);
  }

  @Test
  public void testCreatePost() throws RemoteInvocationException {
    _postService.createPost(POST_TYPE.toString(), POST_CONTENT, SYSTEM_AUTHENTICATION);
    verify(_entityClient, times(1)).ingestProposal(any(), eq(SYSTEM_AUTHENTICATION));
  }
}
