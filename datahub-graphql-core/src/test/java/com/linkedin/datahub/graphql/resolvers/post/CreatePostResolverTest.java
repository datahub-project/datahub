package com.linkedin.datahub.graphql.resolvers.post;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.post.PostService;
import com.linkedin.common.Media;
import com.linkedin.common.url.Url;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.CreatePostInput;
import com.linkedin.datahub.graphql.generated.MediaType;
import com.linkedin.datahub.graphql.generated.PostContentType;
import com.linkedin.datahub.graphql.generated.PostType;
import com.linkedin.datahub.graphql.generated.UpdateMediaInput;
import com.linkedin.datahub.graphql.generated.UpdatePostContentInput;
import graphql.schema.DataFetchingEnvironment;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class CreatePostResolverTest {
  private static final MediaType POST_MEDIA_TYPE = MediaType.IMAGE;
  private static final String POST_MEDIA_LOCATION =
      "https://datahubproject.io/img/datahub-logo-color-light-horizontal.svg";
  private static final PostContentType POST_CONTENT_TYPE = PostContentType.LINK;
  private static final String POST_TITLE = "title";
  private static final String POST_DESCRIPTION = "description";
  private static final String POST_LINK = "https://datahubproject.io";
  private PostService _postService;
  private CreatePostResolver _resolver;
  private DataFetchingEnvironment _dataFetchingEnvironment;
  private Authentication _authentication;

  @BeforeMethod
  public void setupTest() throws Exception {
    _postService = mock(PostService.class);
    _dataFetchingEnvironment = mock(DataFetchingEnvironment.class);
    _authentication = mock(Authentication.class);

    _resolver = new CreatePostResolver(_postService);
  }

  @Test
  public void testNotAuthorizedFails() {
    QueryContext mockContext = getMockDenyContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    assertThrows(() -> _resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testCreatePost() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(mockContext.getAuthentication()).thenReturn(_authentication);

    UpdateMediaInput media = new UpdateMediaInput();
    media.setType(POST_MEDIA_TYPE);
    media.setLocation(POST_MEDIA_LOCATION);
    Media mediaObj = new Media().setType(com.linkedin.common.MediaType.valueOf(POST_MEDIA_TYPE.toString()))
        .setLocation(new Url(POST_MEDIA_LOCATION));
    when(_postService.mapMedia(eq(POST_MEDIA_TYPE.toString()), eq(POST_MEDIA_LOCATION))).thenReturn(mediaObj);

    UpdatePostContentInput content = new UpdatePostContentInput();
    content.setTitle(POST_TITLE);
    content.setDescription(POST_DESCRIPTION);
    content.setLink(POST_LINK);
    content.setContentType(POST_CONTENT_TYPE);
    content.setMedia(media);
    com.linkedin.post.PostContent postContentObj = new com.linkedin.post.PostContent().setType(
            com.linkedin.post.PostContentType.valueOf(POST_CONTENT_TYPE.toString()))
        .setTitle(POST_TITLE)
        .setDescription(POST_DESCRIPTION)
        .setLink(new Url(POST_LINK))
        .setMedia(new Media().setType(com.linkedin.common.MediaType.valueOf(POST_MEDIA_TYPE.toString()))
            .setLocation(new Url(POST_MEDIA_LOCATION)));
    when(_postService.mapPostContent(eq(POST_CONTENT_TYPE.toString()), eq(POST_TITLE), eq(POST_DESCRIPTION),
        eq(POST_LINK), any(Media.class))).thenReturn(postContentObj);

    CreatePostInput input = new CreatePostInput();
    input.setPostType(PostType.HOME_PAGE_ANNOUNCEMENT);
    input.setContent(content);
    when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);
    when(_postService.createPost(eq(PostType.HOME_PAGE_ANNOUNCEMENT.toString()), eq(postContentObj),
        eq(_authentication))).thenReturn(true);

    assertTrue(_resolver.get(_dataFetchingEnvironment).join());
  }
}
