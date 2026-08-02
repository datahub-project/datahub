package com.linkedin.datahub.graphql.resolvers.timeline;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.metadata.Constants.VERSION_PROPERTIES_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.VersionProperties;
import com.linkedin.common.VersionTag;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.ChangeCategoryType;
import com.linkedin.datahub.graphql.generated.GetTimelineInput;
import com.linkedin.datahub.graphql.generated.GetTimelineResult;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.authorization.EntityAuthorizationUtils;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.metadata.timeline.TimelineFetchResult;
import com.linkedin.metadata.timeline.TimelineService;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Map;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.testng.annotations.Test;

public class GetTimelineResolverTest {

  private static final String TEST_DATASET_URN =
      "urn:li:dataset:(urn:li:dataPlatform:kafka,test-timeline-dataset,PROD)";

  @Test
  public void testAllGraphqlCategoriesMatchBackendEnum() {
    // Since we removed the mapping tables, the GraphQL enum names must match the
    // backend ChangeCategory enum names exactly. Verify every GraphQL value can
    // be resolved via ChangeCategory.valueOf.
    for (ChangeCategoryType graphqlType : ChangeCategoryType.values()) {
      ChangeCategory backendCategory = ChangeCategory.valueOf(graphqlType.toString());
      assertNotNull(
          backendCategory,
          "GraphQL ChangeCategoryType."
              + graphqlType
              + " has no matching ChangeCategory enum value");
    }
  }

  @Test
  public void testOwnershipCategoryMatchesDirectly() {
    // Verify OWNERSHIP is now a direct match (no OWNER -> OWNERSHIP mapping needed)
    assertEquals(
        ChangeCategory.valueOf(ChangeCategoryType.OWNERSHIP.toString()), ChangeCategory.OWNERSHIP);
  }

  @Test
  public void testGetUnauthorizedThrowsAndDoesNotQueryDb() {
    TimelineService mockTimelineService = mock(TimelineService.class);
    EntityClient mockEntityClient = mock(EntityClient.class);
    GetTimelineResolver resolver = new GetTimelineResolver(mockTimelineService, mockEntityClient);

    QueryContext denyContext = getMockDenyContextWithOperationContext();

    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    when(mockEnv.getContext()).thenReturn(denyContext);

    GetTimelineInput input = new GetTimelineInput();
    input.setUrn(TEST_DATASET_URN);
    when(mockEnv.getArgument("input")).thenReturn(input);

    assertThrows(AuthorizationException.class, () -> resolver.get(mockEnv));
    verifyNoInteractions(mockTimelineService);
  }

  @Test
  public void testGetAuthorizedReturnsResult() throws Exception {
    TimelineService mockTimelineService = mock(TimelineService.class);
    when(mockTimelineService.getTimeline(
            any(OperationContext.class), any(), any(), anyInt(), anyBoolean()))
        .thenReturn(List.of());

    EntityClient mockEntityClient = mock(EntityClient.class);
    GetTimelineResolver resolver = new GetTimelineResolver(mockTimelineService, mockEntityClient);

    QueryContext allowContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    when(mockEnv.getContext()).thenReturn(allowContext);

    GetTimelineInput input = new GetTimelineInput();
    input.setUrn(TEST_DATASET_URN);
    when(mockEnv.getArgument("input")).thenReturn(input);

    assertNotNull(resolver.get(mockEnv).get());
    verify(mockTimelineService, times(1))
        .getTimeline(any(OperationContext.class), any(), any(), anyInt(), anyBoolean());
  }

  // ── includeVersionSet tests ───────────────────────────────────────────────

  @Test
  public void testIncludeVersionSet_noVersionProperties_fallsBackToSingleton() throws Exception {
    TimelineService mockTimelineService = mock(TimelineService.class);
    when(mockTimelineService.getTimeline(
            any(OperationContext.class), any(), any(), anyInt(), anyBoolean()))
        .thenReturn(List.of());

    EntityClient mockEntityClient = mock(EntityClient.class);
    // getV2 returns null → no VersionProperties → singleton fallback
    when(mockEntityClient.getV2(any(), anyString(), any(), any())).thenReturn(null);

    GetTimelineResolver resolver = new GetTimelineResolver(mockTimelineService, mockEntityClient);

    QueryContext allowContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    when(mockEnv.getContext()).thenReturn(allowContext);

    GetTimelineInput input = new GetTimelineInput();
    input.setUrn(TEST_DATASET_URN);
    input.setIncludeVersionSet(true);
    when(mockEnv.getArgument("input")).thenReturn(input);

    // When falling back to singleton, getTimelineForUrns is called with a 1-element list
    when(mockTimelineService.getTimelineForUrns(any(), any(), any(), anyBoolean()))
        .thenReturn(
            TimelineFetchResult.builder().transactions(List.of()).skippedUrnCount(0).build());

    GetTimelineResult result = resolver.get(mockEnv).get();
    assertNotNull(result);
    assertEquals((int) result.getSkippedVersionCount(), 0);
    verify(mockTimelineService, times(1))
        .getTimelineForUrns(any(OperationContext.class), any(), any(), anyBoolean());
    verify(mockTimelineService, never())
        .getTimeline(any(OperationContext.class), any(), any(), anyInt(), anyBoolean());
  }

  @Test
  public void testIncludeVersionSet_withSiblings_callsGetTimelineForUrns() throws Exception {
    TimelineService mockTimelineService = mock(TimelineService.class);

    String versionSetUrn = "urn:li:versionSet:(urn:li:dataPlatform:kafka,test-vs,PROD)";
    String siblingUrn = "urn:li:dataset:(urn:li:dataPlatform:kafka,test-sibling,PROD)";

    VersionProperties vp = buildVersionProperties(versionSetUrn, "v1.0");
    EntityResponse entityResponse = buildEntityResponse(TEST_DATASET_URN, vp);

    SearchResult searchResult =
        new SearchResult()
            .setEntities(
                new SearchEntityArray(
                    new SearchEntity().setEntity(UrnUtils.getUrn(TEST_DATASET_URN)),
                    new SearchEntity().setEntity(UrnUtils.getUrn(siblingUrn))))
            .setNumEntities(2)
            .setFrom(0)
            .setPageSize(50)
            .setMetadata(new SearchResultMetadata());

    EntityClient mockEntityClient = mock(EntityClient.class);
    when(mockEntityClient.getV2(any(), anyString(), any(), any())).thenReturn(entityResponse);
    when(mockEntityClient.search(any(), anyString(), anyString(), any(), any(), anyInt(), anyInt()))
        .thenReturn(searchResult);

    when(mockTimelineService.getTimelineForUrns(any(), any(), any(), anyBoolean()))
        .thenReturn(
            TimelineFetchResult.builder().transactions(List.of()).skippedUrnCount(0).build());

    GetTimelineResolver resolver = new GetTimelineResolver(mockTimelineService, mockEntityClient);

    QueryContext allowContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    when(mockEnv.getContext()).thenReturn(allowContext);

    GetTimelineInput input = new GetTimelineInput();
    input.setUrn(TEST_DATASET_URN);
    input.setIncludeVersionSet(true);
    when(mockEnv.getArgument("input")).thenReturn(input);

    GetTimelineResult result = resolver.get(mockEnv).get();
    assertNotNull(result);
    // skippedVersionCount = skippedUrnCount(0) + truncatedCount(0)
    assertEquals((int) result.getSkippedVersionCount(), 0);
    verify(mockTimelineService, times(1))
        .getTimelineForUrns(any(OperationContext.class), any(), any(), anyBoolean());
  }

  @Test
  public void testIncludeVersionSet_unauthorizedSiblingIsSkipped() throws Exception {
    TimelineService mockTimelineService = mock(TimelineService.class);

    String versionSetUrn = "urn:li:versionSet:(urn:li:dataPlatform:kafka,test-vs,PROD)";
    String siblingUrn = "urn:li:dataset:(urn:li:dataPlatform:kafka,test-sibling,PROD)";
    Urn rootUrn = UrnUtils.getUrn(TEST_DATASET_URN);
    Urn sibling = UrnUtils.getUrn(siblingUrn);

    VersionProperties vp = buildVersionProperties(versionSetUrn, "v1.0");
    EntityResponse entityResponse = buildEntityResponse(TEST_DATASET_URN, vp);

    SearchResult searchResult =
        new SearchResult()
            .setEntities(
                new SearchEntityArray(
                    new SearchEntity().setEntity(rootUrn), new SearchEntity().setEntity(sibling)))
            .setNumEntities(2)
            .setFrom(0)
            .setPageSize(50)
            .setMetadata(new SearchResultMetadata());

    EntityClient mockEntityClient = mock(EntityClient.class);
    when(mockEntityClient.getV2(any(), anyString(), any(), any())).thenReturn(entityResponse);
    when(mockEntityClient.search(any(), anyString(), anyString(), any(), any(), anyInt(), anyInt()))
        .thenReturn(searchResult);

    when(mockTimelineService.getTimelineForUrns(any(), any(), any(), anyBoolean()))
        .thenReturn(
            TimelineFetchResult.builder().transactions(List.of()).skippedUrnCount(0).build());

    GetTimelineResolver resolver = new GetTimelineResolver(mockTimelineService, mockEntityClient);

    QueryContext allowContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    when(mockEnv.getContext()).thenReturn(allowContext);

    GetTimelineInput input = new GetTimelineInput();
    input.setUrn(TEST_DATASET_URN);
    input.setIncludeVersionSet(true);
    when(mockEnv.getArgument("input")).thenReturn(input);

    try (MockedStatic<EntityAuthorizationUtils> entityAuth =
        mockStatic(EntityAuthorizationUtils.class, CALLS_REAL_METHODS)) {
      entityAuth
          .when(() -> EntityAuthorizationUtils.canViewEntity(any(), eq(rootUrn)))
          .thenReturn(true);
      entityAuth
          .when(() -> EntityAuthorizationUtils.canViewEntity(any(), eq(sibling)))
          .thenReturn(false);

      GetTimelineResult result = resolver.get(mockEnv).get();
      assertNotNull(result);
      assertEquals((int) result.getSkippedVersionCount(), 1);

      ArgumentCaptor<List> urnsCaptor = ArgumentCaptor.forClass(List.class);
      verify(mockTimelineService, times(1))
          .getTimelineForUrns(
              any(OperationContext.class), urnsCaptor.capture(), any(), anyBoolean());
      assertEquals(urnsCaptor.getValue(), List.of(rootUrn));
    }
  }

  @Test
  public void testIncludeVersionSet_searchReturnsEmpty_fallsBackToSingleton() throws Exception {
    TimelineService mockTimelineService = mock(TimelineService.class);

    String versionSetUrn = "urn:li:versionSet:(urn:li:dataPlatform:kafka,test-vs,PROD)";
    VersionProperties vp = buildVersionProperties(versionSetUrn, "v1.0");
    EntityResponse entityResponse = buildEntityResponse(TEST_DATASET_URN, vp);

    // Empty search result
    SearchResult emptyResult =
        new SearchResult()
            .setEntities(new SearchEntityArray())
            .setNumEntities(0)
            .setFrom(0)
            .setPageSize(50)
            .setMetadata(new SearchResultMetadata());

    EntityClient mockEntityClient = mock(EntityClient.class);
    when(mockEntityClient.getV2(any(), anyString(), any(), any())).thenReturn(entityResponse);
    when(mockEntityClient.search(any(), anyString(), anyString(), any(), any(), anyInt(), anyInt()))
        .thenReturn(emptyResult);

    when(mockTimelineService.getTimelineForUrns(any(), any(), any(), anyBoolean()))
        .thenReturn(
            TimelineFetchResult.builder().transactions(List.of()).skippedUrnCount(0).build());

    GetTimelineResolver resolver = new GetTimelineResolver(mockTimelineService, mockEntityClient);

    QueryContext allowContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    when(mockEnv.getContext()).thenReturn(allowContext);

    GetTimelineInput input = new GetTimelineInput();
    input.setUrn(TEST_DATASET_URN);
    input.setIncludeVersionSet(true);
    when(mockEnv.getArgument("input")).thenReturn(input);

    GetTimelineResult result = resolver.get(mockEnv).get();
    assertNotNull(result);
    // Falls back to singleton — still uses getTimelineForUrns with 1-element list
    verify(mockTimelineService, times(1))
        .getTimelineForUrns(any(OperationContext.class), any(), any(), anyBoolean());
  }

  @Test
  public void testIncludeVersionSet_getV2Throws_fallsBackToSingleton() throws Exception {
    TimelineService mockTimelineService = mock(TimelineService.class);
    EntityClient mockEntityClient = mock(EntityClient.class);
    when(mockEntityClient.getV2(any(), anyString(), any(), any()))
        .thenThrow(new RuntimeException("Network error"));

    when(mockTimelineService.getTimelineForUrns(any(), any(), any(), anyBoolean()))
        .thenReturn(
            TimelineFetchResult.builder().transactions(List.of()).skippedUrnCount(0).build());

    GetTimelineResolver resolver = new GetTimelineResolver(mockTimelineService, mockEntityClient);

    QueryContext allowContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    when(mockEnv.getContext()).thenReturn(allowContext);

    GetTimelineInput input = new GetTimelineInput();
    input.setUrn(TEST_DATASET_URN);
    input.setIncludeVersionSet(true);
    when(mockEnv.getArgument("input")).thenReturn(input);

    // Exception is swallowed and falls back — result should still be returned
    GetTimelineResult result = resolver.get(mockEnv).get();
    assertNotNull(result);
    verify(mockTimelineService, times(1))
        .getTimelineForUrns(any(OperationContext.class), any(), any(), anyBoolean());
  }

  @Test
  public void testIncludeVersionSet_truncatedVersionsReflectedInSkippedCount() throws Exception {
    TimelineService mockTimelineService = mock(TimelineService.class);

    String versionSetUrn = "urn:li:versionSet:(urn:li:dataPlatform:kafka,test-vs,PROD)";
    VersionProperties vp = buildVersionProperties(versionSetUrn, "v1.0");
    EntityResponse entityResponse = buildEntityResponse(TEST_DATASET_URN, vp);

    // Search returns 2 entities but claims 10 total (8 truncated)
    SearchResult searchResult =
        new SearchResult()
            .setEntities(
                new SearchEntityArray(
                    new SearchEntity().setEntity(UrnUtils.getUrn(TEST_DATASET_URN)),
                    new SearchEntity()
                        .setEntity(
                            UrnUtils.getUrn(
                                "urn:li:dataset:(urn:li:dataPlatform:kafka,sibling,PROD)"))))
            .setNumEntities(10)
            .setFrom(0)
            .setPageSize(50)
            .setMetadata(new SearchResultMetadata());

    EntityClient mockEntityClient = mock(EntityClient.class);
    when(mockEntityClient.getV2(any(), anyString(), any(), any())).thenReturn(entityResponse);
    when(mockEntityClient.search(any(), anyString(), anyString(), any(), any(), anyInt(), anyInt()))
        .thenReturn(searchResult);

    when(mockTimelineService.getTimelineForUrns(any(), any(), any(), anyBoolean()))
        .thenReturn(
            TimelineFetchResult.builder().transactions(List.of()).skippedUrnCount(0).build());

    GetTimelineResolver resolver = new GetTimelineResolver(mockTimelineService, mockEntityClient);

    QueryContext allowContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    when(mockEnv.getContext()).thenReturn(allowContext);

    GetTimelineInput input = new GetTimelineInput();
    input.setUrn(TEST_DATASET_URN);
    input.setIncludeVersionSet(true);
    when(mockEnv.getArgument("input")).thenReturn(input);

    GetTimelineResult result = resolver.get(mockEnv).get();
    // 10 total − 2 returned = 8 truncated; skippedUrnCount from fetch = 0; total = 8
    assertEquals((int) result.getSkippedVersionCount(), 8);
  }

  // ── Helpers ───────────────────────────────────────────────────────────────

  private static VersionProperties buildVersionProperties(String versionSetUrn, String tag) {
    VersionTag vt = new VersionTag().setVersionTag(tag);
    return new VersionProperties()
        .setVersionSet(UrnUtils.getUrn(versionSetUrn))
        .setVersion(vt)
        .setSortId("00000001");
  }

  private static EntityResponse buildEntityResponse(String urn, VersionProperties vp) {
    EnvelopedAspect envelopedAspect =
        new EnvelopedAspect().setValue(new com.linkedin.entity.Aspect(vp.data()));
    return new EntityResponse()
        .setUrn(UrnUtils.getUrn(urn))
        .setEntityName("dataset")
        .setAspects(
            new EnvelopedAspectMap(Map.of(VERSION_PROPERTIES_ASPECT_NAME, envelopedAspect)));
  }
}
