package com.linkedin.datahub.upgrade.system.homepagelinks;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.url.Url;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
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
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collections;
import java.util.Map;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MigrateHomePageLinksStepTest {

  private static final OperationContext OP_CONTEXT =
      TestOperationContexts.systemContextNoSearchAuthorization();

  @Mock private EntityService<?> mockEntityService;
  @Mock private EntitySearchService mockEntitySearchService;
  @Mock private UpgradeContext mockUpgradeContext;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    when(mockUpgradeContext.opContext()).thenReturn(OP_CONTEXT);
  }

  @Test
  public void testSkipReturnsFalseWhenNoPriorRun() throws Exception {
    when(mockEntityService.getEntityV2(any(), any(), any(), any())).thenReturn(null);
    MigrateHomePageLinksStep step =
        new MigrateHomePageLinksStep(mockEntityService, mockEntitySearchService);
    assertFalse(step.skip(mockUpgradeContext));
  }

  @Test
  public void testSkipReturnsTrueWhenAlreadyRan() throws Exception {
    DataHubUpgradeResult upgradeResult = new DataHubUpgradeResult().setTimestampMs(0L);
    EnvelopedAspect aspect = new EnvelopedAspect().setValue(new Aspect(upgradeResult.data()));
    EnvelopedAspectMap aspectMap =
        new EnvelopedAspectMap(Map.of(Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME, aspect));
    EntityResponse response = new EntityResponse().setAspects(aspectMap);
    when(mockEntityService.getEntityV2(
            any(), eq(Constants.DATA_HUB_UPGRADE_ENTITY_NAME), any(), any()))
        .thenReturn(response);
    MigrateHomePageLinksStep step =
        new MigrateHomePageLinksStep(mockEntityService, mockEntitySearchService);
    assertTrue(step.skip(mockUpgradeContext));
  }

  @Test
  public void testExecutableSucceedsWhenNoPosts() {
    SearchResult emptyResult =
        new SearchResult()
            .setEntities(new SearchEntityArray())
            .setNumEntities(0)
            .setFrom(0)
            .setPageSize(0);
    when(mockEntitySearchService.search(any(), any(), any(), any(), any(), eq(0), any()))
        .thenReturn(emptyResult);

    MigrateHomePageLinksStep step =
        new MigrateHomePageLinksStep(mockEntityService, mockEntitySearchService);
    UpgradeStepResult result = step.executable().apply(mockUpgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  public void testExecutableSucceedsWhenTemplateNotFound() throws Exception {
    Urn postUrn = Urn.createFromString("urn:li:post:test-post");
    SearchResult searchResult =
        new SearchResult()
            .setEntities(new SearchEntityArray(new SearchEntity().setEntity(postUrn)))
            .setNumEntities(1)
            .setFrom(0)
            .setPageSize(1000);
    when(mockEntitySearchService.search(any(), any(), any(), any(), any(), eq(0), any()))
        .thenReturn(searchResult);
    when(mockEntityService.getEntityV2(
            any(), eq(Constants.DATAHUB_PAGE_TEMPLATE_ENTITY_NAME), any(), any()))
        .thenReturn(null);

    MigrateHomePageLinksStep step =
        new MigrateHomePageLinksStep(mockEntityService, mockEntitySearchService);
    UpgradeStepResult result = step.executable().apply(mockUpgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  public void testExecutableReturnsFailedOnException() {
    when(mockEntitySearchService.search(any(), any(), any(), any(), any(), eq(0), any()))
        .thenThrow(new RuntimeException("search error"));

    MigrateHomePageLinksStep step =
        new MigrateHomePageLinksStep(mockEntityService, mockEntitySearchService);
    UpgradeStepResult result = step.executable().apply(mockUpgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.FAILED);
    verify(mockEntityService).deleteUrn(any(), any(Urn.class));
  }

  @Test
  public void testExecutableIngestsUpgradeRecordsOnSuccess() {
    SearchResult emptyResult =
        new SearchResult()
            .setEntities(new SearchEntityArray())
            .setNumEntities(0)
            .setFrom(0)
            .setPageSize(0);
    when(mockEntitySearchService.search(any(), any(), any(), any(), any(), eq(0), any()))
        .thenReturn(emptyResult);

    MigrateHomePageLinksStep step =
        new MigrateHomePageLinksStep(mockEntityService, mockEntitySearchService);
    UpgradeStepResult result = step.executable().apply(mockUpgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(mockEntityService)
        .ingestProposal(
            any(),
            argThat(p -> Constants.DATA_HUB_UPGRADE_REQUEST_ASPECT_NAME.equals(p.getAspectName())),
            any(AuditStamp.class),
            eq(false));
    verify(mockEntityService)
        .ingestProposal(
            any(),
            argThat(p -> Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME.equals(p.getAspectName())),
            any(AuditStamp.class),
            eq(false));
  }

  @Test
  public void testExecutableWithValidPostsAndTemplate() throws Exception {
    Urn postUrn = Urn.createFromString("urn:li:post:test-post");

    SearchResult searchResult =
        new SearchResult()
            .setEntities(new SearchEntityArray(new SearchEntity().setEntity(postUrn)))
            .setNumEntities(1)
            .setFrom(0)
            .setPageSize(1000);
    when(mockEntitySearchService.search(any(), any(), any(), any(), any(), eq(0), any()))
        .thenReturn(searchResult);

    when(mockEntityService.getEntityV2(
            any(), eq(Constants.DATAHUB_PAGE_TEMPLATE_ENTITY_NAME), any(), any()))
        .thenReturn(createTemplateEntityResponse());

    when(mockEntityService.getEntitiesV2(any(), eq(Constants.POST_ENTITY_NAME), any(), any()))
        .thenReturn(Map.of(postUrn, createValidPostEntityResponse(postUrn)));

    when(mockEntityService.ingestProposal(any(), any(AspectsBatchImpl.class), eq(false)))
        .thenReturn(Collections.emptyList());

    MigrateHomePageLinksStep step =
        new MigrateHomePageLinksStep(mockEntityService, mockEntitySearchService);
    UpgradeStepResult result = step.executable().apply(mockUpgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    // one call for module creation, one for template update
    verify(mockEntityService, times(2))
        .ingestProposal(any(), any(AspectsBatchImpl.class), eq(false));
  }

  @Test
  public void testExecutableWithNoValidPosts() throws Exception {
    Urn postUrn = Urn.createFromString("urn:li:post:test-post");

    SearchResult searchResult =
        new SearchResult()
            .setEntities(new SearchEntityArray(new SearchEntity().setEntity(postUrn)))
            .setNumEntities(1)
            .setFrom(0)
            .setPageSize(1000);
    when(mockEntitySearchService.search(any(), any(), any(), any(), any(), eq(0), any()))
        .thenReturn(searchResult);

    when(mockEntityService.getEntityV2(
            any(), eq(Constants.DATAHUB_PAGE_TEMPLATE_ENTITY_NAME), any(), any()))
        .thenReturn(createTemplateEntityResponse());

    when(mockEntityService.getEntitiesV2(any(), eq(Constants.POST_ENTITY_NAME), any(), any()))
        .thenReturn(Map.of(postUrn, createInvalidPostEntityResponse(postUrn)));

    MigrateHomePageLinksStep step =
        new MigrateHomePageLinksStep(mockEntityService, mockEntitySearchService);
    UpgradeStepResult result = step.executable().apply(mockUpgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(mockEntityService, never())
        .ingestProposal(any(), any(AspectsBatchImpl.class), eq(false));
  }

  private EntityResponse createTemplateEntityResponse() {
    AuditStamp auditStamp =
        new AuditStamp().setActor(UrnUtils.getUrn(Constants.SYSTEM_ACTOR)).setTime(0L);
    DataHubPageTemplateVisibility visibility =
        new DataHubPageTemplateVisibility().setScope(PageTemplateScope.GLOBAL);
    DataHubPageTemplateProperties props =
        new DataHubPageTemplateProperties()
            .setRows(new DataHubPageTemplateRowArray())
            .setSurface(
                new DataHubPageTemplateSurface().setSurfaceType(PageTemplateSurfaceType.HOME_PAGE))
            .setVisibility(visibility)
            .setCreated(auditStamp)
            .setLastModified(auditStamp);
    EnvelopedAspect aspect = new EnvelopedAspect().setValue(new Aspect(props.data()));
    EnvelopedAspectMap aspectMap =
        new EnvelopedAspectMap(
            Map.of(Constants.DATAHUB_PAGE_TEMPLATE_PROPERTIES_ASPECT_NAME, aspect));
    return new EntityResponse().setAspects(aspectMap);
  }

  private EntityResponse createValidPostEntityResponse(Urn postUrn) throws Exception {
    PostContent content =
        new PostContent()
            .setType(PostContentType.LINK)
            .setTitle("Test Link")
            .setLink(new Url("https://example.com"));
    PostInfo postInfo = new PostInfo().setType(PostType.HOME_PAGE_ANNOUNCEMENT).setContent(content);
    EnvelopedAspect aspect = new EnvelopedAspect().setValue(new Aspect(postInfo.data()));
    EnvelopedAspectMap aspectMap =
        new EnvelopedAspectMap(Map.of(Constants.POST_INFO_ASPECT_NAME, aspect));
    return new EntityResponse().setUrn(postUrn).setAspects(aspectMap);
  }

  private EntityResponse createInvalidPostEntityResponse(Urn postUrn) throws Exception {
    // link=null causes the step to skip this post (fails the PostContentType.LINK + link!=null
    // check)
    PostContent content = new PostContent().setType(PostContentType.LINK).setTitle("Test Text");
    PostInfo postInfo = new PostInfo().setType(PostType.HOME_PAGE_ANNOUNCEMENT).setContent(content);
    EnvelopedAspect aspect = new EnvelopedAspect().setValue(new Aspect(postInfo.data()));
    EnvelopedAspectMap aspectMap =
        new EnvelopedAspectMap(Map.of(Constants.POST_INFO_ASPECT_NAME, aspect));
    return new EntityResponse().setUrn(postUrn).setAspects(aspectMap);
  }
}
