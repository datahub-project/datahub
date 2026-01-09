package com.linkedin.metadata.boot.steps;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Media;
import com.linkedin.common.MediaType;
import com.linkedin.common.url.Url;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.IngestResult;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.post.PostContent;
import com.linkedin.post.PostContentType;
import com.linkedin.post.PostInfo;
import com.linkedin.post.PostType;
import com.linkedin.template.DataHubPageTemplateProperties;
import com.linkedin.template.DataHubPageTemplateRowArray;
import com.linkedin.template.DataHubPageTemplateSurface;
import com.linkedin.template.DataHubPageTemplateVisibility;
import com.linkedin.template.PageTemplateScope;
import com.linkedin.template.PageTemplateSurfaceType;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collections;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class MigrateHomePageLinksStepTest {

  @Test
  public void testConstructor() {
    EntityService<?> entityService = Mockito.mock(EntityService.class);
    EntitySearchService searchService = Mockito.mock(EntitySearchService.class);

    MigrateHomePageLinksStep step = new MigrateHomePageLinksStep(entityService, searchService);

    assertNotNull(step);
  }

  @Test
  public void testExecutionMode() {
    EntityService<?> entityService = Mockito.mock(EntityService.class);
    EntitySearchService searchService = Mockito.mock(EntitySearchService.class);

    MigrateHomePageLinksStep step = new MigrateHomePageLinksStep(entityService, searchService);

    assertEquals(step.getExecutionMode(), MigrateHomePageLinksStep.ExecutionMode.ASYNC);
  }

  @Test
  public void testUpgradeWithNoPosts() throws Exception {
    EntityService<?> mockEntityService = mock(EntityService.class);
    EntitySearchService mockSearchService = mock(EntitySearchService.class);
    EntityRegistry testEntityRegistry = getTestEntityRegistry();
    OperationContext mockContext =
        TestOperationContexts.systemContextNoSearchAuthorization(testEntityRegistry);

    // Mock search to return no posts
    when(mockSearchService.search(
            any(), eq(List.of(Constants.POST_ENTITY_NAME)), eq("*"), any(), any(), eq(0), eq(1000)))
        .thenReturn(createEmptySearchResult());

    MigrateHomePageLinksStep step =
        new MigrateHomePageLinksStep(mockEntityService, mockSearchService);
    step.upgrade(mockContext);

    // Verify that no further processing happens when no posts are found
    verify(mockEntityService, times(0)).getEntityV2(any(), any(), any(), anySet());
    verify(mockEntityService, times(0)).ingestProposal(any(), any(), eq(false));
  }

  @Test
  public void testUpgradeWithNoTemplate() throws Exception {
    EntityService<?> mockEntityService = mock(EntityService.class);
    EntitySearchService mockSearchService = mock(EntitySearchService.class);
    EntityRegistry testEntityRegistry = getTestEntityRegistry();
    OperationContext mockContext =
        TestOperationContexts.systemContextNoSearchAuthorization(testEntityRegistry);

    Urn postUrn = UrnUtils.getUrn("urn:li:post:test-post");
    Urn templateUrn = UrnUtils.getUrn("urn:li:dataHubPageTemplate:home_default_1");

    // Mock search to return posts
    when(mockSearchService.search(
            any(), eq(List.of(Constants.POST_ENTITY_NAME)), eq("*"), any(), any(), eq(0), eq(1000)))
        .thenReturn(createSearchResultWithPosts(ImmutableList.of(postUrn)));

    // Mock template fetch to return null (template not found)
    when(mockEntityService.getEntityV2(
            any(),
            eq(Constants.DATAHUB_PAGE_TEMPLATE_ENTITY_NAME),
            eq(templateUrn),
            eq(Collections.singleton(Constants.DATAHUB_PAGE_TEMPLATE_PROPERTIES_ASPECT_NAME))))
        .thenReturn(null);

    MigrateHomePageLinksStep step =
        new MigrateHomePageLinksStep(mockEntityService, mockSearchService);
    step.upgrade(mockContext);

    // Verify template fetch was attempted but no ingestion happened
    verify(mockEntityService, times(1))
        .getEntityV2(
            any(),
            eq(Constants.DATAHUB_PAGE_TEMPLATE_ENTITY_NAME),
            eq(templateUrn),
            eq(Collections.singleton(Constants.DATAHUB_PAGE_TEMPLATE_PROPERTIES_ASPECT_NAME)));
    verify(mockEntityService, times(0)).ingestProposal(any(), any(), eq(false));
  }

  @Test
  public void testUpgradeWithValidPostsAndTemplate() throws Exception {
    EntityService<?> mockEntityService = mock(EntityService.class);
    EntitySearchService mockSearchService = mock(EntitySearchService.class);
    EntityRegistry testEntityRegistry = getTestEntityRegistry();
    OperationContext mockContext =
        TestOperationContexts.systemContextNoSearchAuthorization(testEntityRegistry);

    Urn postUrn = UrnUtils.getUrn("urn:li:post:test-post");
    Urn templateUrn = UrnUtils.getUrn("urn:li:dataHubPageTemplate:home_default_1");

    // Mock search to return posts
    when(mockSearchService.search(
            any(), eq(List.of(Constants.POST_ENTITY_NAME)), eq("*"), any(), any(), eq(0), eq(1000)))
        .thenReturn(createSearchResultWithPosts(ImmutableList.of(postUrn)));

    // Mock template fetch
    when(mockEntityService.getEntityV2(
            any(),
            eq(Constants.DATAHUB_PAGE_TEMPLATE_ENTITY_NAME),
            eq(templateUrn),
            eq(Collections.singleton(Constants.DATAHUB_PAGE_TEMPLATE_PROPERTIES_ASPECT_NAME))))
        .thenReturn(createTemplateResponse(templateUrn));

    // Mock post info fetch
    when(mockEntityService.getEntitiesV2(
            any(),
            eq(Constants.POST_ENTITY_NAME),
            anySet(),
            eq(Collections.singleton(Constants.POST_INFO_ASPECT_NAME))))
        .thenReturn(ImmutableMap.of(postUrn, createValidPostResponse(postUrn)));

    // Mock ingest to return successfully
    when(mockEntityService.ingestProposal(
            any(OperationContext.class), any(AspectsBatchImpl.class), eq(false)))
        .thenReturn(ImmutableList.of(mock(IngestResult.class)));

    MigrateHomePageLinksStep step =
        new MigrateHomePageLinksStep(mockEntityService, mockSearchService);

    // This should complete successfully if our business logic works
    step.upgrade(mockContext);

    // Verify that ingestion was attempted for the module and template
    verify(mockEntityService, times(2))
        .ingestProposal(any(OperationContext.class), any(AspectsBatchImpl.class), eq(false));
  }

  @Test
  public void testUpgradeWithNoValidPosts() throws Exception {
    EntityService<?> mockEntityService = mock(EntityService.class);
    EntitySearchService mockSearchService = mock(EntitySearchService.class);
    EntityRegistry testEntityRegistry = getTestEntityRegistry();
    OperationContext mockContext =
        TestOperationContexts.systemContextNoSearchAuthorization(testEntityRegistry);

    Urn invalidPostUrn = UrnUtils.getUrn("urn:li:post:invalid-post");
    Urn templateUrn = UrnUtils.getUrn("urn:li:dataHubPageTemplate:home_default_1");

    // Mock search to return posts
    when(mockSearchService.search(
            any(), eq(List.of(Constants.POST_ENTITY_NAME)), eq("*"), any(), any(), eq(0), eq(1000)))
        .thenReturn(createSearchResultWithPosts(ImmutableList.of(invalidPostUrn)));

    // Mock template fetch
    when(mockEntityService.getEntityV2(
            any(),
            eq(Constants.DATAHUB_PAGE_TEMPLATE_ENTITY_NAME),
            eq(templateUrn),
            eq(Collections.singleton(Constants.DATAHUB_PAGE_TEMPLATE_PROPERTIES_ASPECT_NAME))))
        .thenReturn(createTemplateResponse(templateUrn));

    // Mock post info fetch - only invalid posts
    when(mockEntityService.getEntitiesV2(
            any(),
            eq(Constants.POST_ENTITY_NAME),
            anySet(),
            eq(Collections.singleton(Constants.POST_INFO_ASPECT_NAME))))
        .thenReturn(ImmutableMap.of(invalidPostUrn, createInvalidPostResponse(invalidPostUrn)));

    // Mock ingest to return successfully (should not be called)
    when(mockEntityService.ingestProposal(
            any(OperationContext.class), any(AspectsBatchImpl.class), eq(false)))
        .thenReturn(ImmutableList.of(mock(IngestResult.class)));

    MigrateHomePageLinksStep step =
        new MigrateHomePageLinksStep(mockEntityService, mockSearchService);

    // This should complete successfully but not call ingest (no valid posts)
    step.upgrade(mockContext);

    // Verify that ingestion was NOT attempted (no valid posts)
    verify(mockEntityService, times(0))
        .ingestProposal(any(OperationContext.class), any(AspectsBatchImpl.class), eq(false));
  }

  // Helper methods

  @NotNull
  private ConfigEntityRegistry getTestEntityRegistry() {
    PathSpecBasedSchemaAnnotationVisitor.class
        .getClassLoader()
        .setClassAssertionStatus(PathSpecBasedSchemaAnnotationVisitor.class.getName(), false);
    return new ConfigEntityRegistry(
        IngestDataPlatformInstancesStepTest.class
            .getClassLoader()
            .getResourceAsStream("test-entity-registry.yaml"));
  }

  private SearchResult createEmptySearchResult() {
    SearchResult result = new SearchResult();
    result.setEntities(new SearchEntityArray());
    result.setNumEntities(0);
    return result;
  }

  private SearchResult createSearchResultWithPosts(List<Urn> postUrns) {
    SearchResult result = new SearchResult();
    SearchEntityArray entities = new SearchEntityArray();

    for (Urn urn : postUrns) {
      SearchEntity entity = new SearchEntity();
      entity.setEntity(urn);
      entities.add(entity);
    }

    result.setEntities(entities);
    result.setNumEntities(postUrns.size());
    return result;
  }

  private EntityResponse createTemplateResponse(Urn templateUrn) {
    DataHubPageTemplateProperties properties = new DataHubPageTemplateProperties();
    properties.setRows(new DataHubPageTemplateRowArray());
    DataHubPageTemplateVisibility visibility = new DataHubPageTemplateVisibility();
    visibility.setScope(PageTemplateScope.GLOBAL);
    properties.setVisibility(visibility);
    AuditStamp created = new AuditStamp();
    created.setTime(System.currentTimeMillis());
    created.setActor(UrnUtils.getUrn("urn:li:corpuser:admin"));
    properties.setCreated(created);
    properties.setLastModified(created);
    DataHubPageTemplateSurface surface = new DataHubPageTemplateSurface();
    surface.setSurfaceType(PageTemplateSurfaceType.HOME_PAGE);
    properties.setSurface(surface);

    EntityResponse response = new EntityResponse();
    response.setUrn(templateUrn);

    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    EnvelopedAspect aspect = new EnvelopedAspect();
    aspect.setValue(new Aspect(properties.data()));
    aspectMap.put(Constants.DATAHUB_PAGE_TEMPLATE_PROPERTIES_ASPECT_NAME, aspect);
    response.setAspects(aspectMap);

    return response;
  }

  private EntityResponse createValidPostResponse(Urn postUrn) {
    try {
      // Create valid post content (HOME_PAGE_ANNOUNCEMENT + LINK)
      PostContent content = new PostContent();
      content.setType(PostContentType.LINK);
      content.setTitle("Test Post");
      content.setDescription("Test Description");
      content.setLink(new Url("https://example.com"));

      Media media = new Media();
      media.setType(MediaType.IMAGE);
      media.setLocation(new Url("https://example.com/image.png"));
      content.setMedia(media);

      PostInfo postInfo = new PostInfo();
      postInfo.setType(PostType.HOME_PAGE_ANNOUNCEMENT);
      postInfo.setContent(content);
      postInfo.setCreated(System.currentTimeMillis());
      postInfo.setLastModified(System.currentTimeMillis());

      EntityResponse response = new EntityResponse();
      response.setUrn(postUrn);

      EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
      EnvelopedAspect aspect = new EnvelopedAspect();
      aspect.setValue(new Aspect(postInfo.data()));
      aspectMap.put(Constants.POST_INFO_ASPECT_NAME, aspect);
      response.setAspects(aspectMap);

      return response;
    } catch (Exception e) {
      throw new RuntimeException("Error creating test post response", e);
    }
  }

  private EntityResponse createInvalidPostResponse(Urn postUrn) {
    try {
      // Create invalid post content (not HOME_PAGE_ANNOUNCEMENT or not LINK)
      PostContent content = new PostContent();
      content.setType(PostContentType.TEXT); // Not LINK
      content.setTitle("Invalid Post");

      PostInfo postInfo = new PostInfo();
      postInfo.setType(PostType.ENTITY_ANNOUNCEMENT); // Not HOME_PAGE_ANNOUNCEMENT
      postInfo.setContent(content);
      postInfo.setCreated(System.currentTimeMillis());
      postInfo.setLastModified(System.currentTimeMillis());

      EntityResponse response = new EntityResponse();
      response.setUrn(postUrn);

      EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
      EnvelopedAspect aspect = new EnvelopedAspect();
      aspect.setValue(new Aspect(postInfo.data()));
      aspectMap.put(Constants.POST_INFO_ASPECT_NAME, aspect);
      response.setAspects(aspectMap);

      return response;
    } catch (Exception e) {
      throw new RuntimeException("Error creating test invalid post response", e);
    }
  }
}
