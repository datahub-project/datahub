package com.linkedin.datahub.graphql.resolvers.post;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.Media;
import com.linkedin.common.MediaType;
import com.linkedin.common.url.Url;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.ListPostsInput;
import com.linkedin.datahub.graphql.generated.ListPostsResult;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.policy.DataHubRoleInfo;
import com.linkedin.post.PostContent;
import com.linkedin.post.PostContentType;
import com.linkedin.post.PostInfo;
import com.linkedin.post.PostType;
import graphql.schema.DataFetchingEnvironment;
import java.net.URISyntaxException;
import java.util.Map;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class ListPostsResolverTest {
  private static Map<Urn, EntityResponse> _entityResponseMap;
  private static final String POST_URN_STRING = "urn:li:post:examplePost";
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

  private EntityClient _entityClient;
  private ListPostsResolver _resolver;
  private DataFetchingEnvironment _dataFetchingEnvironment;
  private Authentication _authentication;

  private Map<Urn, EntityResponse> getMockPostsEntityResponse() throws URISyntaxException {
    Urn postUrn = Urn.createFromString(POST_URN_STRING);

    EntityResponse entityResponse = new EntityResponse().setUrn(postUrn);
    PostInfo postInfo = new PostInfo();
    postInfo.setType(POST_TYPE);
    postInfo.setContent(POST_CONTENT);
    DataHubRoleInfo dataHubRoleInfo = new DataHubRoleInfo();
    dataHubRoleInfo.setDescription(postUrn.toString());
    dataHubRoleInfo.setName(postUrn.toString());
    entityResponse.setAspects(new EnvelopedAspectMap(ImmutableMap.of(DATAHUB_ROLE_INFO_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(dataHubRoleInfo.data())))));

    return ImmutableMap.of(postUrn, entityResponse);
  }

  @BeforeMethod
  public void setupTest() throws Exception {
    _entityResponseMap = getMockPostsEntityResponse();

    _entityClient = mock(EntityClient.class);
    _dataFetchingEnvironment = mock(DataFetchingEnvironment.class);
    _authentication = mock(Authentication.class);

    _resolver = new ListPostsResolver(_entityClient);
  }

  @Test
  public void testNotAuthorizedFails() {
    QueryContext mockContext = getMockDenyContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    assertThrows(() -> _resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testListPosts() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(mockContext.getAuthentication()).thenReturn(_authentication);

    ListPostsInput input = new ListPostsInput();
    when(_dataFetchingEnvironment.getArgument("input")).thenReturn(input);
    final SearchResult roleSearchResult =
        new SearchResult().setMetadata(new SearchResultMetadata()).setFrom(0).setPageSize(10).setNumEntities(1);
    roleSearchResult.setEntities(
        new SearchEntityArray(ImmutableList.of(new SearchEntity().setEntity(Urn.createFromString(POST_URN_STRING)))));

    when(_entityClient.search(eq(POST_ENTITY_NAME), any(), eq(null), any(), anyInt(), anyInt(),
        eq(_authentication))).thenReturn(roleSearchResult);
    when(_entityClient.batchGetV2(eq(POST_ENTITY_NAME), any(), any(), any())).thenReturn(_entityResponseMap);

    ListPostsResult result = _resolver.get(_dataFetchingEnvironment).join();
    assertEquals(result.getStart(), 0);
    assertEquals(result.getCount(), 10);
    assertEquals(result.getTotal(), 1);
    assertEquals(result.getPosts().size(), 1);
  }
}
