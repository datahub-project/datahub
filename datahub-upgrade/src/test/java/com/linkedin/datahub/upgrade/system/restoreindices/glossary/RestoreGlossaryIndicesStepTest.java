package com.linkedin.datahub.upgrade.system.restoreindices.glossary;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
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
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.glossary.GlossaryTermInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.upgrade.DataHubUpgradeRequest;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RestoreGlossaryIndicesStepTest {

  @Mock private EntityService<?> mockEntityService;
  @Mock private EntitySearchService mockEntitySearchService;
  @Mock private UpgradeContext mockUpgradeContext;
  @Mock private OperationContext mockOpContext;
  @Mock private EntityRegistry mockEntityRegistry;
  @Mock private EntitySpec mockEntitySpec;
  @Mock private AspectSpec mockAspectSpec;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    when(mockUpgradeContext.opContext()).thenReturn(mockOpContext);
    when(mockOpContext.getEntityRegistry()).thenReturn(mockEntityRegistry);
    when(mockOpContext.withSearchFlags(any())).thenReturn(mockOpContext);
    when(mockEntityRegistry.getEntitySpec(any())).thenReturn(mockEntitySpec);
    when(mockEntitySpec.getAspectSpec(any())).thenReturn(mockAspectSpec);
  }

  @Test
  public void testSkipReturnsFalseWhenNoPriorRun() throws Exception {
    when(mockEntityService.getEntityV2(any(), any(), any(), any())).thenReturn(null);
    RestoreGlossaryIndicesStep step =
        new RestoreGlossaryIndicesStep(mockEntityService, mockEntitySearchService);
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
    RestoreGlossaryIndicesStep step =
        new RestoreGlossaryIndicesStep(mockEntityService, mockEntitySearchService);
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
    RestoreGlossaryIndicesStep step =
        new RestoreGlossaryIndicesStep(mockEntityService, mockEntitySearchService);
    assertFalse(step.skip(mockUpgradeContext));
  }

  @Test
  public void testExecutableSucceedsWhenNoTermsOrNodes() {
    when(mockEntitySearchService.search(any(), any(), any(), any(), any(), eq(0), any()))
        .thenReturn(emptySearchResult());

    RestoreGlossaryIndicesStep step =
        new RestoreGlossaryIndicesStep(mockEntityService, mockEntitySearchService);
    UpgradeStepResult result = step.executable().apply(mockUpgradeContext);

    verify(mockEntityService, never())
        .alwaysProduceMCLAsync(
            any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void testExecutableEmitsMCLForEachGlossaryTerm() throws Exception {
    Urn termUrn = Urn.createFromString("urn:li:glossaryTerm:Revenue");
    GlossaryTermInfo termInfo = new GlossaryTermInfo().setDefinition("definition");

    SearchResult termsResult =
        new SearchResult()
            .setEntities(new SearchEntityArray(new SearchEntity().setEntity(termUrn)))
            .setNumEntities(1)
            .setFrom(0)
            .setPageSize(1000);

    when(mockEntitySearchService.search(
            any(),
            eq(List.of(Constants.GLOSSARY_TERM_ENTITY_NAME)),
            any(),
            any(),
            any(),
            eq(0),
            any()))
        .thenReturn(termsResult);
    when(mockEntitySearchService.search(
            any(),
            eq(List.of(Constants.GLOSSARY_NODE_ENTITY_NAME)),
            any(),
            any(),
            any(),
            eq(0),
            any()))
        .thenReturn(emptySearchResult());

    EnvelopedAspect envelopedAspect =
        new EnvelopedAspect().setValue(new Aspect(new com.linkedin.data.DataMap()));
    EnvelopedAspectMap aspectMap =
        new EnvelopedAspectMap(Map.of(Constants.GLOSSARY_TERM_INFO_ASPECT_NAME, envelopedAspect));
    EntityResponse entityResponse = new EntityResponse().setUrn(termUrn).setAspects(aspectMap);

    when(mockEntityService.getEntitiesV2(
            any(), eq(Constants.GLOSSARY_TERM_ENTITY_NAME), any(), any()))
        .thenReturn(Map.of(termUrn, entityResponse));

    com.linkedin.util.Pair mockPair =
        com.linkedin.util.Pair.of(CompletableFuture.completedFuture(null), null);
    when(mockEntityService.alwaysProduceMCLAsync(
            any(),
            eq(termUrn),
            eq(Constants.GLOSSARY_TERM_ENTITY_NAME),
            eq(Constants.GLOSSARY_TERM_INFO_ASPECT_NAME),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(AuditStamp.class),
            eq(ChangeType.RESTATE)))
        .thenReturn(mockPair);

    RestoreGlossaryIndicesStep step =
        new RestoreGlossaryIndicesStep(mockEntityService, mockEntitySearchService);
    UpgradeStepResult stepResult = step.executable().apply(mockUpgradeContext);

    verify(mockEntityService)
        .alwaysProduceMCLAsync(
            any(),
            eq(termUrn),
            eq(Constants.GLOSSARY_TERM_ENTITY_NAME),
            eq(Constants.GLOSSARY_TERM_INFO_ASPECT_NAME),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(AuditStamp.class),
            eq(ChangeType.RESTATE));
    assertEquals(stepResult.result(), DataHubUpgradeState.SUCCEEDED);
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
  public void testExecutableReturnsFailedOnException() {
    when(mockEntitySearchService.search(any(), any(), any(), any(), any(), eq(0), any()))
        .thenThrow(new RuntimeException("search error"));

    RestoreGlossaryIndicesStep step =
        new RestoreGlossaryIndicesStep(mockEntityService, mockEntitySearchService);
    UpgradeStepResult result = step.executable().apply(mockUpgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.FAILED);
    verify(mockEntityService).deleteUrn(any(), any(Urn.class));
  }

  private static SearchResult emptySearchResult() {
    return new SearchResult()
        .setEntities(new SearchEntityArray())
        .setNumEntities(0)
        .setFrom(0)
        .setPageSize(0);
  }
}
