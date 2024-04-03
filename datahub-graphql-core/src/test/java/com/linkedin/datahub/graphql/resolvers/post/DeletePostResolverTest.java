package com.linkedin.datahub.graphql.resolvers.post;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.post.PostService;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import graphql.schema.DataFetchingEnvironment;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DeletePostResolverTest {
  private static final String POST_URN_STRING = "urn:li:post:123";
  private PostService _postService;
  private DeletePostResolver _resolver;
  private DataFetchingEnvironment _dataFetchingEnvironment;
  private Authentication _authentication;

  @BeforeMethod
  public void setupTest() throws Exception {
    _postService = mock(PostService.class);
    _dataFetchingEnvironment = mock(DataFetchingEnvironment.class);
    _authentication = mock(Authentication.class);

    _resolver = new DeletePostResolver(_postService);
  }

  @Test
  public void testNotAuthorizedFails() {
    QueryContext mockContext = getMockDenyContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    assertThrows(() -> _resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testDeletePost() throws Exception {
    Urn postUrn = UrnUtils.getUrn(POST_URN_STRING);

    QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(mockContext.getAuthentication()).thenReturn(_authentication);
    when(_dataFetchingEnvironment.getArgument(eq("urn"))).thenReturn(POST_URN_STRING);
    when(_postService.deletePost(eq(postUrn), eq(_authentication))).thenReturn(true);

    assertTrue(_resolver.get(_dataFetchingEnvironment).join());
  }
}
