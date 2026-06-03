package com.linkedin.datahub.upgrade.system.homepagelinks;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.upgrade.DataHubUpgradeRequest;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Map;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MigrateHomePageLinksStepTest {

  @Mock private EntityService<?> mockEntityService;
  @Mock private EntitySearchService mockEntitySearchService;
  @Mock private UpgradeContext mockUpgradeContext;
  @Mock private OperationContext mockOpContext;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    when(mockUpgradeContext.opContext()).thenReturn(mockOpContext);
    when(mockOpContext.withSearchFlags(any())).thenReturn(mockOpContext);
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
    DataHubUpgradeRequest upgradeRequest =
        new DataHubUpgradeRequest().setVersion("1").setTimestampMs(0L);
    EnvelopedAspect aspect = new EnvelopedAspect().setValue(new Aspect(upgradeRequest.data()));
    EnvelopedAspectMap aspectMap =
        new EnvelopedAspectMap(Map.of(Constants.DATA_HUB_UPGRADE_REQUEST_ASPECT_NAME, aspect));
    EntityResponse response = new EntityResponse().setAspects(aspectMap);
    when(mockEntityService.getEntityV2(
            any(), eq(Constants.DATA_HUB_UPGRADE_ENTITY_NAME), any(), any()))
        .thenReturn(response);
    MigrateHomePageLinksStep step =
        new MigrateHomePageLinksStep(mockEntityService, mockEntitySearchService);
    assertTrue(step.skip(mockUpgradeContext));
  }

  @Test
  public void testSkipReturnsFalseWhenVersionMismatch() throws Exception {
    DataHubUpgradeRequest upgradeRequest =
        new DataHubUpgradeRequest().setVersion("0").setTimestampMs(0L);
    EnvelopedAspect aspect = new EnvelopedAspect().setValue(new Aspect(upgradeRequest.data()));
    EnvelopedAspectMap aspectMap =
        new EnvelopedAspectMap(Map.of(Constants.DATA_HUB_UPGRADE_REQUEST_ASPECT_NAME, aspect));
    EntityResponse response = new EntityResponse().setAspects(aspectMap);
    when(mockEntityService.getEntityV2(
            any(), eq(Constants.DATA_HUB_UPGRADE_ENTITY_NAME), any(), any()))
        .thenReturn(response);
    MigrateHomePageLinksStep step =
        new MigrateHomePageLinksStep(mockEntityService, mockEntitySearchService);
    assertFalse(step.skip(mockUpgradeContext));
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
}
