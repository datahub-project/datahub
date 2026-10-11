package com.linkedin.datahub.upgrade.system.documents;

import static com.linkedin.metadata.Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.knowledge.DocumentInfo;
import com.linkedin.knowledge.ParentDocument;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.service.DocumentDeleteLimitException;
import com.linkedin.metadata.service.DocumentDeleteResult;
import com.linkedin.metadata.service.DocumentService;
import com.linkedin.metadata.service.SearchIndexMode;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DeleteOrphanedDocumentChildrenStepTest {

  private static final OperationContext OP_CONTEXT =
      TestOperationContexts.systemContextNoSearchAuthorization();

  private static final Urn SOFT_DELETED_PARENT =
      UrnUtils.getUrn("urn:li:document:soft-deleted-parent");
  private static final Urn MISSING_PARENT = UrnUtils.getUrn("urn:li:document:missing-parent");
  private static final Urn LIVE_PARENT = UrnUtils.getUrn("urn:li:document:live-parent");
  private static final Urn ORPHAN_OF_SOFT_DELETED =
      UrnUtils.getUrn("urn:li:document:orphan-of-soft-deleted");
  private static final Urn GRANDCHILD = UrnUtils.getUrn("urn:li:document:grandchild");
  private static final Urn ORPHAN_OF_MISSING = UrnUtils.getUrn("urn:li:document:orphan-of-missing");
  private static final Urn LIVE_CHILD = UrnUtils.getUrn("urn:li:document:live-child");
  private static final Urn OVER_CAP = UrnUtils.getUrn("urn:li:document:over-cap");
  private static final Urn OTHER_ORPHAN = UrnUtils.getUrn("urn:li:document:other-orphan");

  @Mock private EntityService<?> mockEntityService;
  @Mock private SearchService mockSearchService;
  @Mock private DocumentService mockDocumentService;
  @Mock private UpgradeContext mockUpgradeContext;

  private final Map<Urn, DocumentInfo> infoByUrn = new HashMap<>();
  private final Set<Urn> liveUrns = new HashSet<>();

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    infoByUrn.clear();
    liveUrns.clear();
    when(mockUpgradeContext.opContext()).thenReturn(OP_CONTEXT);
    when(mockEntityService.getLatestAspects(any(), anySet(), anySet(), eq(false)))
        .thenAnswer(
            invocation -> {
              final Collection<Urn> urns = invocation.getArgument(1);
              final Map<Urn, List<RecordTemplate>> stored = new HashMap<>();
              for (Urn urn : urns) {
                final DocumentInfo info = infoByUrn.get(urn);
                if (info != null) {
                  stored.put(urn, List.of(info));
                }
              }
              return stored;
            });
    when(mockEntityService.exists(any(), anyCollection(), eq(false)))
        .thenAnswer(
            invocation -> {
              final Collection<Urn> urns = invocation.getArgument(1);
              final Set<Urn> live = new HashSet<>();
              for (Urn urn : urns) {
                if (liveUrns.contains(urn)) {
                  live.add(urn);
                }
              }
              return live;
            });
  }

  @Test
  public void testDeletesOrphansOfSoftDeletedAndMissingParentsAndSkipsLiveParent()
      throws Exception {
    stubScroll(
        ORPHAN_OF_SOFT_DELETED.toString(),
        GRANDCHILD.toString(),
        ORPHAN_OF_MISSING.toString(),
        LIVE_CHILD.toString());
    stubParent(ORPHAN_OF_SOFT_DELETED, SOFT_DELETED_PARENT);
    stubParent(GRANDCHILD, ORPHAN_OF_SOFT_DELETED);
    stubParent(ORPHAN_OF_MISSING, MISSING_PARENT);
    stubParent(LIVE_CHILD, LIVE_PARENT);
    stubLive(LIVE_PARENT);
    stubLive(ORPHAN_OF_SOFT_DELETED);
    when(mockDocumentService.deleteDocument(
            any(), eq(ORPHAN_OF_SOFT_DELETED), eq(SearchIndexMode.SYNC)))
        .thenReturn(new DocumentDeleteResult(List.of(GRANDCHILD, ORPHAN_OF_SOFT_DELETED), 1));
    when(mockDocumentService.deleteDocument(any(), eq(ORPHAN_OF_MISSING), eq(SearchIndexMode.SYNC)))
        .thenReturn(new DocumentDeleteResult(List.of(ORPHAN_OF_MISSING), 0));

    final UpgradeStepResult result = newStep(false).executable().apply(mockUpgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(mockDocumentService)
        .deleteDocument(any(), eq(ORPHAN_OF_SOFT_DELETED), eq(SearchIndexMode.SYNC));
    verify(mockDocumentService)
        .deleteDocument(any(), eq(ORPHAN_OF_MISSING), eq(SearchIndexMode.SYNC));
    verify(mockDocumentService, never())
        .deleteDocument(any(), eq(GRANDCHILD), eq(SearchIndexMode.SYNC));
    verify(mockDocumentService, never())
        .deleteDocument(any(), eq(LIVE_CHILD), eq(SearchIndexMode.SYNC));
    assertUpgradeResult(DataHubUpgradeState.SUCCEEDED, "2", "3", "0", "");
    assertLiveSubtreeFlags(captureScrollContext());
  }

  @Test
  public void testSecondRunSkipsWhenResultAspectExists() {
    when(mockEntityService.exists(
            any(), any(Urn.class), eq(DATA_HUB_UPGRADE_RESULT_ASPECT_NAME), anyBoolean()))
        .thenReturn(true);

    assertTrue(newStep(false).skip(mockUpgradeContext));
  }

  @Test
  public void testReprocessRunsAgainWhenResultAspectExists() {
    when(mockEntityService.exists(
            any(), any(Urn.class), eq(DATA_HUB_UPGRADE_RESULT_ASPECT_NAME), anyBoolean()))
        .thenReturn(true);

    assertFalse(newStep(true).skip(mockUpgradeContext));
  }

  @Test
  public void testOverCapTreeStillWritesMarkerAndProcessesOtherOrphans() throws Exception {
    stubScroll(OVER_CAP.toString(), OTHER_ORPHAN.toString());
    stubParent(OVER_CAP, SOFT_DELETED_PARENT);
    stubParent(OTHER_ORPHAN, MISSING_PARENT);
    when(mockDocumentService.deleteDocument(any(), eq(OVER_CAP), eq(SearchIndexMode.SYNC)))
        .thenThrow(new DocumentDeleteLimitException(OVER_CAP, "live subtree exceeds the cap"));
    when(mockDocumentService.deleteDocument(any(), eq(OTHER_ORPHAN), eq(SearchIndexMode.SYNC)))
        .thenReturn(new DocumentDeleteResult(List.of(OTHER_ORPHAN), 0));

    final UpgradeStepResult result = newStep(false).executable().apply(mockUpgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.FAILED);
    verify(mockDocumentService).deleteDocument(any(), eq(OVER_CAP), eq(SearchIndexMode.SYNC));
    verify(mockDocumentService).deleteDocument(any(), eq(OTHER_ORPHAN), eq(SearchIndexMode.SYNC));
    assertUpgradeResult(DataHubUpgradeState.FAILED, "1", "1", "1", OVER_CAP.toString());
  }

  @Test
  public void testSkipsCandidateWithNoStoredDocumentInfoAndStillDeletesFollowingOrphan()
      throws Exception {
    final Urn noInfo = UrnUtils.getUrn("urn:li:document:no-info");
    stubScroll(noInfo.toString(), ORPHAN_OF_MISSING.toString());
    stubParent(ORPHAN_OF_MISSING, MISSING_PARENT);
    when(mockDocumentService.deleteDocument(any(), eq(ORPHAN_OF_MISSING), eq(SearchIndexMode.SYNC)))
        .thenReturn(new DocumentDeleteResult(List.of(ORPHAN_OF_MISSING), 0));

    final UpgradeStepResult result = newStep(false).executable().apply(mockUpgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(mockDocumentService, never())
        .deleteDocument(any(), eq(noInfo), eq(SearchIndexMode.SYNC));
    verify(mockDocumentService)
        .deleteDocument(any(), eq(ORPHAN_OF_MISSING), eq(SearchIndexMode.SYNC));
    assertUpgradeMarkerWritten();
  }

  @Test
  public void testSkipsCandidateWhoseStoredParentFieldIsAbsentAndStillDeletesFollowingOrphan()
      throws Exception {
    final Urn noParent = UrnUtils.getUrn("urn:li:document:no-parent-field");
    stubScroll(noParent.toString(), ORPHAN_OF_MISSING.toString());
    infoByUrn.put(noParent, new DocumentInfo());
    stubParent(ORPHAN_OF_MISSING, MISSING_PARENT);
    when(mockDocumentService.deleteDocument(any(), eq(ORPHAN_OF_MISSING), eq(SearchIndexMode.SYNC)))
        .thenReturn(new DocumentDeleteResult(List.of(ORPHAN_OF_MISSING), 0));

    final UpgradeStepResult result = newStep(false).executable().apply(mockUpgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(mockDocumentService, never())
        .deleteDocument(any(), eq(noParent), eq(SearchIndexMode.SYNC));
    verify(mockDocumentService)
        .deleteDocument(any(), eq(ORPHAN_OF_MISSING), eq(SearchIndexMode.SYNC));
    assertUpgradeMarkerWritten();
  }

  @Test
  public void testScrollsASecondPageWithThePreviousScrollId() throws Exception {
    final Urn secondPage = UrnUtils.getUrn("urn:li:document:second-page-orphan");
    when(mockSearchService.scrollAcrossEntities(
            any(OperationContext.class),
            any(),
            anyString(),
            any(Filter.class),
            nullable(List.class),
            nullable(String.class),
            nullable(String.class),
            any(),
            nullable(List.class)))
        .thenAnswer(
            invocation -> {
              final String scrollId = invocation.getArgument(5);
              if (scrollId == null) {
                return scrollOf(ORPHAN_OF_MISSING.toString()).setScrollId("s1");
              }
              assertEquals(scrollId, "s1");
              return scrollOf(secondPage.toString());
            });
    stubParent(ORPHAN_OF_MISSING, MISSING_PARENT);
    stubParent(secondPage, MISSING_PARENT);
    when(mockDocumentService.deleteDocument(any(), eq(ORPHAN_OF_MISSING), eq(SearchIndexMode.SYNC)))
        .thenReturn(new DocumentDeleteResult(List.of(ORPHAN_OF_MISSING), 0));
    when(mockDocumentService.deleteDocument(any(), eq(secondPage), eq(SearchIndexMode.SYNC)))
        .thenReturn(new DocumentDeleteResult(List.of(secondPage), 0));

    final UpgradeStepResult result = newStep(false).executable().apply(mockUpgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(mockDocumentService)
        .deleteDocument(any(), eq(ORPHAN_OF_MISSING), eq(SearchIndexMode.SYNC));
    verify(mockDocumentService).deleteDocument(any(), eq(secondPage), eq(SearchIndexMode.SYNC));
    final ArgumentCaptor<String> scrollIds = ArgumentCaptor.forClass(String.class);
    verify(mockSearchService, times(2))
        .scrollAcrossEntities(
            any(OperationContext.class),
            any(),
            anyString(),
            any(Filter.class),
            nullable(List.class),
            scrollIds.capture(),
            nullable(String.class),
            any(),
            nullable(List.class));
    assertEquals(scrollIds.getAllValues().size(), 2);
    assertEquals(scrollIds.getAllValues().get(0), null);
    assertEquals(scrollIds.getAllValues().get(1), "s1");
  }

  @Test
  public void testNullScrollPageDoesNotWriteMarker() {
    when(mockSearchService.scrollAcrossEntities(
            any(OperationContext.class),
            any(),
            anyString(),
            any(Filter.class),
            nullable(List.class),
            nullable(String.class),
            nullable(String.class),
            any(),
            nullable(List.class)))
        .thenReturn(null);

    expectThrows(
        IllegalStateException.class, () -> newStep(false).executable().apply(mockUpgradeContext));
    verify(mockEntityService, never()).ingestProposal(any(), any(), any(), anyBoolean());
  }

  @Test
  public void testDisabledUpgradeHasNoSteps() {
    assertTrue(
        new DeleteOrphanedDocumentChildren(
                OP_CONTEXT,
                mockEntityService,
                mockSearchService,
                mockDocumentService,
                false,
                false,
                DocumentDeleteResult.SCROLL_PAGE_SIZE)
            .steps()
            .isEmpty());
  }

  private DeleteOrphanedDocumentChildrenStep newStep(boolean reprocessEnabled) {
    return new DeleteOrphanedDocumentChildrenStep(
        OP_CONTEXT,
        mockEntityService,
        mockSearchService,
        mockDocumentService,
        reprocessEnabled,
        DocumentDeleteResult.SCROLL_PAGE_SIZE);
  }

  private void stubScroll(String... urns) {
    when(mockSearchService.scrollAcrossEntities(
            any(OperationContext.class),
            any(),
            anyString(),
            any(Filter.class),
            nullable(List.class),
            nullable(String.class),
            nullable(String.class),
            any(),
            nullable(List.class)))
        .thenReturn(scrollOf(urns));
  }

  private void stubParent(Urn document, Urn parent) {
    infoByUrn.put(
        document, new DocumentInfo().setParentDocument(new ParentDocument().setDocument(parent)));
  }

  private void stubLive(Urn urn) {
    liveUrns.add(urn);
  }

  private OperationContext captureScrollContext() {
    final ArgumentCaptor<OperationContext> captor = ArgumentCaptor.forClass(OperationContext.class);
    verify(mockSearchService)
        .scrollAcrossEntities(
            captor.capture(),
            any(),
            anyString(),
            any(Filter.class),
            nullable(List.class),
            nullable(String.class),
            nullable(String.class),
            eq(DocumentDeleteResult.SCROLL_PAGE_SIZE),
            nullable(List.class));
    final Filter filter = scrollFilter();
    assertEquals(filter.getOr().get(0).getAnd().get(0).getField(), "parentDocument");
    assertEquals(filter.getOr().get(0).getAnd().get(0).getCondition().toString(), "EXISTS");
    return captor.getValue();
  }

  private Filter scrollFilter() {
    final ArgumentCaptor<Filter> captor = ArgumentCaptor.forClass(Filter.class);
    verify(mockSearchService)
        .scrollAcrossEntities(
            any(OperationContext.class),
            any(),
            anyString(),
            captor.capture(),
            nullable(List.class),
            nullable(String.class),
            nullable(String.class),
            any(),
            nullable(List.class));
    return captor.getValue();
  }

  private void assertUpgradeMarkerWritten() throws Exception {
    captureUpgradeResult();
  }

  private void assertUpgradeResult(
      DataHubUpgradeState state,
      String deletedRootCount,
      String deletedUrnCount,
      String overCapRootCount,
      String skippedRoots)
      throws Exception {
    final DataHubUpgradeResult upgradeResult = captureUpgradeResult();
    assertEquals(upgradeResult.getState(), state);
    assertEquals(upgradeResult.getResult().get("deletedRootCount"), deletedRootCount);
    assertEquals(upgradeResult.getResult().get("deletedUrnCount"), deletedUrnCount);
    assertEquals(upgradeResult.getResult().get("overCapRootCount"), overCapRootCount);
    assertEquals(upgradeResult.getResult().get("skippedRoots"), skippedRoots);
  }

  private DataHubUpgradeResult captureUpgradeResult() throws Exception {
    final ArgumentCaptor<MetadataChangeProposal> captor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(mockEntityService).ingestProposal(any(), captor.capture(), any(), eq(false));
    assertEquals(captor.getValue().getAspectName(), DATA_HUB_UPGRADE_RESULT_ASPECT_NAME);
    assertEquals(
        captor.getValue().getEntityUrn().getEntityKey().get(0),
        DeleteOrphanedDocumentChildrenStep.UPGRADE_ID);
    return GenericRecordUtils.deserializeAspect(
        captor.getValue().getAspect().getValue(),
        captor.getValue().getAspect().getContentType(),
        DataHubUpgradeResult.class);
  }

  private static void assertLiveSubtreeFlags(OperationContext scrollContext) {
    final SearchFlags flags = scrollContext.getSearchContext().getSearchFlags();
    assertFalse(flags.isIncludeSoftDeleted());
    assertTrue(flags.isIncludeHiddenLifecycleStages());
    assertFalse(flags.isRewriteQuery());
    assertTrue(flags.isSkipCache());
    assertTrue(flags.isSkipHighlighting());
    assertTrue(flags.isSkipAggregates());
  }

  private static ScrollResult scrollOf(String... urns) {
    final SearchEntityArray entities = new SearchEntityArray();
    for (String urn : urns) {
      entities.add(new SearchEntity().setEntity(UrnUtils.getUrn(urn)));
    }
    return new ScrollResult().setNumEntities(urns.length).setEntities(entities).setPageSize(1000);
  }
}
