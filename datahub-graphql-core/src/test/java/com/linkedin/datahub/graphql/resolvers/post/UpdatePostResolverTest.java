package com.linkedin.datahub.graphql.resolvers.post;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.datahub.graphql.TestUtils.getMockDenyContext;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.post.PostService;
import com.linkedin.common.Media;
import com.linkedin.common.url.Url;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.MediaType;
import com.linkedin.datahub.graphql.generated.PostContentType;
import com.linkedin.datahub.graphql.generated.PostType;
import com.linkedin.datahub.graphql.generated.UpdateMediaInput;
import com.linkedin.datahub.graphql.generated.UpdatePostContentInput;
import com.linkedin.datahub.graphql.generated.UpdatePostInput;
import graphql.schema.DataFetchingEnvironment;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class UpdatePostResolverTest {

  private static final Urn TEST_URN = UrnUtils.getUrn("urn:li:post:post-id");
  private static final MediaType POST_MEDIA_TYPE = MediaType.IMAGE;
  private static final String POST_MEDIA_LOCATION =
      "https://datahubproject.io/img/datahub-logo-color-light-horizontal.svg";
  private static final PostContentType POST_CONTENT_TYPE = PostContentType.LINK;
  private static final String POST_TITLE = "title";
  private static final String POST_DESCRIPTION = "description";
  private static final String POST_LINK = "https://datahubproject.io";
  private PostService postService;
  private UpdatePostResolver resolver;
  private DataFetchingEnvironment dataFetchingEnvironment;
  private Authentication authentication;

  @BeforeMethod
  public void setupTest() throws Exception {
    postService = mock(PostService.class);
    dataFetchingEnvironment = mock(DataFetchingEnvironment.class);
    authentication = mock(Authentication.class);

    resolver = new UpdatePostResolver(postService);
  }

  @Test
  public void testNotAuthorizedFails() {
    QueryContext mockContext = getMockDenyContext();
    when(dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    assertThrows(() -> resolver.get(dataFetchingEnvironment).join());
  }

  @Test
  public void testUpdatePost() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(mockContext.getAuthentication()).thenReturn(authentication);

    UpdateMediaInput media = new UpdateMediaInput();
    media.setType(POST_MEDIA_TYPE);
    media.setLocation(POST_MEDIA_LOCATION);
    Media mediaObj =
        new Media()
            .setType(com.linkedin.common.MediaType.valueOf(POST_MEDIA_TYPE.toString()))
            .setLocation(new Url(POST_MEDIA_LOCATION));
    when(postService.mapMedia(POST_MEDIA_TYPE.toString(), POST_MEDIA_LOCATION))
        .thenReturn(mediaObj);

    UpdatePostContentInput content = new UpdatePostContentInput();
    content.setTitle(POST_TITLE);
    content.setDescription(POST_DESCRIPTION);
    content.setLink(POST_LINK);
    content.setContentType(POST_CONTENT_TYPE);
    content.setMedia(media);
    com.linkedin.post.PostContent postContentObj =
        new com.linkedin.post.PostContent()
            .setType(com.linkedin.post.PostContentType.valueOf(POST_CONTENT_TYPE.toString()))
            .setTitle(POST_TITLE)
            .setDescription(POST_DESCRIPTION)
            .setLink(new Url(POST_LINK))
            .setMedia(
                new Media()
                    .setType(com.linkedin.common.MediaType.valueOf(POST_MEDIA_TYPE.toString()))
                    .setLocation(new Url(POST_MEDIA_LOCATION)));
    when(postService.mapPostContent(
            eq(POST_CONTENT_TYPE.toString()),
            eq(POST_TITLE),
            eq(POST_DESCRIPTION),
            eq(POST_LINK),
            any(Media.class)))
        .thenReturn(postContentObj);

    UpdatePostInput input = new UpdatePostInput();
    input.setUrn(TEST_URN.toString());
    input.setPostType(PostType.HOME_PAGE_ANNOUNCEMENT);
    input.setContent(content);
    when(dataFetchingEnvironment.getArgument("input")).thenReturn(input);
    when(postService.updatePost(
            any(),
            eq(TEST_URN),
            eq(PostType.HOME_PAGE_ANNOUNCEMENT.toString()),
            eq(postContentObj)))
        .thenReturn(true);

    assertTrue(resolver.get(dataFetchingEnvironment).join());
    verify(postService, times(1)).updatePost(any(), any(), any(), any());
  }
}
