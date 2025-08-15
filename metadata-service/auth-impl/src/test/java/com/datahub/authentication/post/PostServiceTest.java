package com.datahub.authentication.post;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.linkedin.common.Media;
import com.linkedin.common.MediaType;
import com.linkedin.common.url.Url;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.post.PostContent;
import com.linkedin.post.PostContentType;
import com.linkedin.post.PostType;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.net.URISyntaxException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PostServiceTest {
  private static final Urn POST_URN = UrnUtils.getUrn("urn:li:post:123");
  private static final Urn ENTITY_URN = UrnUtils.getUrn("urn:li:domain:123");
  private static final MediaType POST_MEDIA_TYPE = MediaType.IMAGE;
  private static final String POST_MEDIA_LOCATION =
      "https://docs.datahub.com/img/datahub-logo-color-light-horizontal.svg";
  private static final PostContentType POST_CONTENT_TYPE = PostContentType.LINK;
  private static final String POST_TITLE = "title";
  private static final String POST_DESCRIPTION = "description";
  private static final String POST_LINK = "https://docs.datahub.com";
  private static final Media MEDIA =
      new Media().setType(POST_MEDIA_TYPE).setLocation(new Url(POST_MEDIA_LOCATION));
  private static final PostContent POST_CONTENT =
      new PostContent()
          .setType(POST_CONTENT_TYPE)
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
  private OperationContext opContext =
      TestOperationContexts.userContextNoSearchAuthorization(SYSTEM_AUTHENTICATION);

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
        _postService.mapPostContent(
            POST_CONTENT_TYPE.toString(), POST_TITLE, POST_DESCRIPTION, POST_LINK, MEDIA);
    assertEquals(POST_CONTENT, postContent);
  }

  @Test
  public void testCreatePost() throws RemoteInvocationException, URISyntaxException {
    _postService.createPost(opContext, POST_TYPE.toString(), POST_CONTENT, ENTITY_URN.toString());
    verify(_entityClient, times(1)).ingestProposal(any(OperationContext.class), any());
  }

  @Test
  public void testDeletePostDoesNotExist() throws RemoteInvocationException {
    when(_entityClient.exists(any(OperationContext.class), eq(POST_URN))).thenReturn(false);
    assertThrows(() -> _postService.deletePost(mock(OperationContext.class), POST_URN));
  }

  @Test
  public void testDeletePost() throws RemoteInvocationException {
    when(_entityClient.exists(any(OperationContext.class), eq(POST_URN))).thenReturn(true);
    assertTrue(_postService.deletePost(mock(OperationContext.class), POST_URN));
  }
}
