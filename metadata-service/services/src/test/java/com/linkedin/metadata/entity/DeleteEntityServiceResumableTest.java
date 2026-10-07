package com.linkedin.metadata.entity;

import static com.linkedin.metadata.aspect.validation.ConditionalWriteValidator.HTTP_HEADER_IF_VERSION_MATCH;
import static com.linkedin.metadata.search.utils.QueryUtils.EMPTY_FILTER;
import static com.linkedin.metadata.search.utils.QueryUtils.newFilter;
import static com.linkedin.metadata.search.utils.QueryUtils.newRelationshipFilter;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.FormAssociation;
import com.linkedin.common.FormAssociationArray;
import com.linkedin.common.FormVerificationAssociationArray;
import com.linkedin.common.Forms;
import com.linkedin.common.Status;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.container.Container;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.domain.Domains;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.models.graph.RelatedEntities;
import com.linkedin.metadata.aspect.models.graph.RelatedEntitiesScrollResult;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.query.filter.RelationshipFilter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Covers {@link DeleteEntityService#removeReferencesResumable}: references-first, conditional,
 * failing fast on a conflict, idempotent, and resumable at a phase.
 */
public class DeleteEntityServiceResumableTest {

  private static final Urn DOMAIN = UrnUtils.getUrn("urn:li:domain:resumable-domain");
  private static final Urn OTHER_DOMAIN = UrnUtils.getUrn("urn:li:domain:resumable-other");
  private static final Urn CONTAINER = UrnUtils.getUrn("urn:li:container:resumable-container");
  private static final Urn OTHER_CONTAINER = UrnUtils.getUrn("urn:li:container:resumable-other");
  private static final Urn DATASET_A = UrnUtils.toDatasetUrn("hive", "resumable_a", "PROD");
  private static final Urn DATASET_B = UrnUtils.toDatasetUrn("hive", "resumable_b", "PROD");
  private static final String ASSOCIATED_WITH = "AssociatedWith";
  private static final String IS_PART_OF = "IsPartOf";
  private static final String PAGE_TWO = "scroll-page-2";
  private static final Urn FORM = UrnUtils.getUrn("urn:li:form:resumable-form");
  private static final Urn LIVE_FILE = UrnUtils.getUrn("urn:li:dataHubFile:resumable-live");
  private static final Urn DELETED_FILE = UrnUtils.getUrn("urn:li:dataHubFile:resumable-deleted");

  private OperationContext opContext;
  private EntityService<?> entityService;
  private GraphService graphService;
  private EntitySearchService searchService;
  private DeleteEntityService service;
  private List<DeleteCascadeCheckpoint> checkpoints;

  @BeforeMethod
  public void setUp() {
    opContext = TestOperationContexts.systemContextNoSearchAuthorization();
    entityService = mock(EntityService.class);
    graphService = mock(GraphService.class);
    searchService = mock(EntitySearchService.class);
    service = new DeleteEntityService(entityService, graphService, searchService, null, null);
    checkpoints = new ArrayList<>();

    // Defaults: no incoming edges, empty search scans, every write commits.
    when(graphService.scrollRelatedEntities(
            any(OperationContext.class),
            nullable(Set.class),
            any(Filter.class),
            nullable(Set.class),
            any(Filter.class),
            anySet(),
            any(RelationshipFilter.class),
            anyList(),
            nullable(String.class),
            anyString(),
            anyInt(),
            nullable(Long.class),
            nullable(Long.class)))
        .thenReturn(edges(null, IS_PART_OF, CONTAINER));
    when(searchService.structuredScroll(
            any(OperationContext.class),
            anyList(),
            anyString(),
            any(),
            anyList(),
            nullable(String.class),
            anyString(),
            anyInt()))
        .thenReturn(scrollOf(null));
    when(entityService.ingestProposal(
            any(OperationContext.class),
            any(MetadataChangeProposal.class),
            any(AuditStamp.class),
            eq(false)))
        .thenReturn(IngestResult.builder().sqlCommitted(true).build());
  }

  @Test
  public void graphReferenceIsRemovedWithAVersionPrecondition() throws Exception {
    stubIncomingEdges(DOMAIN, null, edges(null, ASSOCIATED_WITH, DOMAIN, DATASET_A));
    when(entityService.getEntityV2(
            any(OperationContext.class), eq("dataset"), eq(DATASET_A), anySet()))
        .thenReturn(entity(DATASET_A, domains(3, DOMAIN, OTHER_DOMAIN)));

    final int removed =
        service.removeReferencesResumable(opContext, DOMAIN, null, recordingListener());

    final ArgumentCaptor<MetadataChangeProposal> written =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(entityService)
        .ingestProposal(
            any(OperationContext.class), written.capture(), any(AuditStamp.class), eq(false));
    assertEquals(written.getValue().getEntityUrn(), DATASET_A);
    assertEquals(written.getValue().getHeaders().get(HTTP_HEADER_IF_VERSION_MATCH), "3");
    assertEquals(domainsIn(written.getValue()).getDomains(), new UrnArray(List.of(OTHER_DOMAIN)));
    assertEquals(removed, 1);
  }

  /**
   * A referrer written since it was read: the conditional write is not committed (a system context
   * has no RequestContext, so ingestProposal returns null instead of throwing). The cascade stops
   * there, with no re-read and no later referrer touched; running it again finishes the work.
   */
  @Test
  public void aConflictingWriteFailsFast() throws Exception {
    stubIncomingEdges(DOMAIN, null, edges(null, ASSOCIATED_WITH, DOMAIN, DATASET_A, DATASET_B));
    when(entityService.getEntityV2(
            any(OperationContext.class), eq("dataset"), any(Urn.class), anySet()))
        .thenAnswer(invocation -> entity(invocation.getArgument(2), domains(3, DOMAIN)));
    when(entityService.ingestProposal(
            any(OperationContext.class),
            any(MetadataChangeProposal.class),
            any(AuditStamp.class),
            eq(false)))
        .thenReturn(null);

    expectThrows(
        IllegalStateException.class,
        () -> service.removeReferencesResumable(opContext, DOMAIN, null, recordingListener()));

    verify(entityService)
        .ingestProposal(
            any(OperationContext.class),
            any(MetadataChangeProposal.class),
            any(AuditStamp.class),
            eq(false));
    verify(entityService, never())
        .getEntityV2(any(OperationContext.class), eq("dataset"), eq(DATASET_B), anySet());
  }

  @Test
  public void requiredFieldReferenceDeletesTheAspectUpToTheVersionRead() throws Exception {
    stubIncomingEdges(CONTAINER, null, edges(null, IS_PART_OF, CONTAINER, DATASET_A));
    when(entityService.getEntityV2(
            any(OperationContext.class), eq("dataset"), eq(DATASET_A), anySet()))
        .thenReturn(entity(DATASET_A, container(5, CONTAINER)));
    when(entityService.deleteAspectUpToVersion(
            any(OperationContext.class),
            eq(DATASET_A),
            eq(Constants.CONTAINER_ASPECT_NAME),
            eq(5L)))
        .thenReturn(ConditionalDeleteOutcome.DELETED);

    final int removed =
        service.removeReferencesResumable(opContext, CONTAINER, null, recordingListener());

    verify(entityService)
        .deleteAspectUpToVersion(
            any(OperationContext.class),
            eq(DATASET_A),
            eq(Constants.CONTAINER_ASPECT_NAME),
            eq(5L));
    verify(entityService, never())
        .ingestProposal(
            any(OperationContext.class),
            any(MetadataChangeProposal.class),
            any(AuditStamp.class),
            eq(false));
    assertEquals(removed, 1);
  }

  /** The aspect advanced past the version read; the newer value may still hold the reference. */
  @Test
  public void aPartialAspectDeleteFailsFast() throws Exception {
    stubIncomingEdges(CONTAINER, null, edges(null, IS_PART_OF, CONTAINER, DATASET_A));
    when(entityService.getEntityV2(
            any(OperationContext.class), eq("dataset"), eq(DATASET_A), anySet()))
        .thenReturn(entity(DATASET_A, container(2, CONTAINER)));
    when(entityService.deleteAspectUpToVersion(
            any(OperationContext.class), eq(DATASET_A), anyString(), anyLong()))
        .thenReturn(ConditionalDeleteOutcome.PARTIAL);

    expectThrows(
        IllegalStateException.class,
        () -> service.removeReferencesResumable(opContext, CONTAINER, null, recordingListener()));

    verify(entityService)
        .deleteAspectUpToVersion(
            any(OperationContext.class), eq(DATASET_A), anyString(), anyLong());
  }

  /** A rejection (e.g. a delete-time validator) reaches the caller as it was thrown. */
  @Test
  public void aRejectedWritePropagates() throws Exception {
    stubIncomingEdges(CONTAINER, null, edges(null, IS_PART_OF, CONTAINER, DATASET_A));
    when(entityService.getEntityV2(
            any(OperationContext.class), eq("dataset"), eq(DATASET_A), anySet()))
        .thenReturn(entity(DATASET_A, container(2, CONTAINER)));
    final IllegalArgumentException rejected =
        new IllegalArgumentException("rejected by a delete-time validator");
    when(entityService.deleteAspectUpToVersion(
            any(OperationContext.class), eq(DATASET_A), anyString(), anyLong()))
        .thenThrow(rejected);

    final IllegalArgumentException thrown =
        expectThrows(
            IllegalArgumentException.class,
            () ->
                service.removeReferencesResumable(opContext, CONTAINER, null, recordingListener()));

    assertSame(thrown, rejected);
  }

  /** Edges whose source is gone or no longer holds the reference are left to node removal. */
  @Test
  public void aStaleEdgeIsSkippedWithoutAnyWrite() throws Exception {
    stubIncomingEdges(CONTAINER, null, edges(null, IS_PART_OF, CONTAINER, DATASET_A, DATASET_B));
    // A is gone entirely (getEntityV2 -> null); B now points at another container.
    when(entityService.getEntityV2(
            any(OperationContext.class), eq("dataset"), eq(DATASET_B), anySet()))
        .thenReturn(entity(DATASET_B, container(4, OTHER_CONTAINER)));

    final int removed =
        service.removeReferencesResumable(opContext, CONTAINER, null, recordingListener());

    verify(entityService, never())
        .ingestProposal(
            any(OperationContext.class),
            any(MetadataChangeProposal.class),
            any(AuditStamp.class),
            eq(false));
    verify(entityService, never())
        .deleteAspectUpToVersion(
            any(OperationContext.class), any(Urn.class), anyString(), anyLong());
    assertEquals(removed, 0);
  }

  @Test
  public void everyGraphScanIncludesSoftDeletedEdges() throws Exception {
    assertNotEquals(
        opContext.getSearchContext().getSearchFlags().isIncludeSoftDeleted(), Boolean.TRUE);
    stubIncomingEdges(CONTAINER, null, edges(null, IS_PART_OF, CONTAINER, DATASET_A));

    service.removeReferencesResumable(opContext, CONTAINER, null, recordingListener());

    final ArgumentCaptor<OperationContext> scanContext =
        ArgumentCaptor.forClass(OperationContext.class);
    verify(graphService, atLeastOnce())
        .scrollRelatedEntities(
            scanContext.capture(),
            nullable(Set.class),
            any(Filter.class),
            nullable(Set.class),
            any(Filter.class),
            anySet(),
            any(RelationshipFilter.class),
            anyList(),
            nullable(String.class),
            anyString(),
            anyInt(),
            nullable(Long.class),
            nullable(Long.class));
    scanContext.getAllValues().forEach(ctx -> assertTrue(softDeletedIncluded(ctx)));
  }

  /**
   * Stopping before any page and running again (from the last phase reached) ends in the same data
   * as one straight run, and no referrer is written twice.
   */
  @Test
  public void aRerunAfterAStopAtAnyPageEndsInTheStraightRunsState() throws Exception {
    final Map<Urn, List<Urn>> cleaned =
        Map.of(DATASET_A, List.of(OTHER_DOMAIN), DATASET_B, List.of());
    final Map<Urn, Integer> oneWriteEach = Map.of(DATASET_A, 1, DATASET_B, 1);
    final ReferrerStore straight = twoPageDomainScenario();
    service.removeReferencesResumable(opContext, DOMAIN, null, recordingListener());
    assertEquals(straight.currentDomains(), cleaned);
    assertEquals(straight.writeCounts(), oneWriteEach);
    final int pageCount = checkpoints.size();

    for (int stopAt = 1; stopAt <= pageCount; stopAt++) {
      setUp();
      final ReferrerStore store = twoPageDomainScenario();
      final List<DeleteCascadeCheckpoint> saved = new ArrayList<>();
      final int limit = stopAt;
      final DeleteCascadeListener stopping =
          current -> {
            saved.add(current);
            if (saved.size() == limit) {
              throw new IllegalStateException("stopped at page " + limit);
            }
          };
      expectThrows(
          IllegalStateException.class,
          () -> service.removeReferencesResumable(opContext, DOMAIN, null, stopping));

      service.removeReferencesResumable(
          opContext, DOMAIN, saved.get(saved.size() - 1), DeleteCascadeListener.NOOP);

      assertEquals(store.currentDomains(), cleaned, "stopped at page " + stopAt);
      assertEquals(store.writeCounts(), oneWriteEach, "stopped at page " + stopAt);
    }
  }

  /** A caller relies on the listener's own exception reaching it unwrapped (cancellation). */
  @Test
  public void aListenerStopBeforeTheFirstPageWritesNothingAndPropagatesUnwrapped() {
    final IllegalStateException cancel = new IllegalStateException("cancelled");
    final DeleteCascadeListener cancelled =
        current -> {
          throw cancel;
        };

    final IllegalStateException thrown =
        expectThrows(
            IllegalStateException.class,
            () -> service.removeReferencesResumable(opContext, CONTAINER, null, cancelled));

    assertSame(thrown, cancel);
    verifyNoInteractions(graphService);
    verify(entityService, never())
        .ingestProposal(
            any(OperationContext.class),
            any(MetadataChangeProposal.class),
            any(AuditStamp.class),
            eq(false));
  }

  @Test
  public void edgeWithNoRegistryAspectIsSkipped() throws Exception {
    // Structured-property relationship edges use the property urn as their type.
    stubIncomingEdges(
        DOMAIN, null, edges(null, "urn:li:structuredProperty:test.ref", DOMAIN, DATASET_A));

    final int removed =
        service.removeReferencesResumable(opContext, DOMAIN, null, recordingListener());

    assertEquals(removed, 0);
    verify(entityService, never())
        .getEntityV2(any(OperationContext.class), anyString(), any(Urn.class), anySet());
  }

  @Test
  public void searchOnlyFormReferenceIsRemovedConditionally() throws Exception {
    // The graph knows nothing about the form; only the search index does.
    stubFormScan(scrollOf(null, DATASET_A));
    final Forms forms = formsReferencing(FORM);
    when(entityService.getEntityV2(
            any(OperationContext.class), eq("dataset"), eq(DATASET_A), anySet()))
        .thenReturn(entity(DATASET_A, aspect(Constants.FORMS_ASPECT_NAME, forms, 5)));
    when(entityService.getLatestAspect(
            any(OperationContext.class), eq(DATASET_A), eq(Constants.FORMS_ASPECT_NAME)))
        .thenReturn(forms);

    final int removed = service.removeReferencesResumable(opContext, FORM, null, recordingListener());

    final ArgumentCaptor<MetadataChangeProposal> written =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(entityService)
        .ingestProposal(
            any(OperationContext.class), written.capture(), any(AuditStamp.class), eq(false));
    assertEquals(written.getValue().getEntityUrn(), DATASET_A);
    assertEquals(written.getValue().getAspectName(), Constants.FORMS_ASPECT_NAME);
    assertEquals(written.getValue().getHeaders().get(HTTP_HEADER_IF_VERSION_MATCH), "5");
    assertEquals(removed, 1);
  }

  @Test
  public void everySearchScanIncludesSoftDeletedAndSortsByUrnOnly() {
    service.removeReferencesResumable(opContext, FORM, null, DeleteCascadeListener.NOOP);

    final ArgumentCaptor<OperationContext> scanContext =
        ArgumentCaptor.forClass(OperationContext.class);
    @SuppressWarnings({"unchecked", "rawtypes"})
    final ArgumentCaptor<List<SortCriterion>> sort = ArgumentCaptor.forClass((Class) List.class);
    verify(searchService, atLeast(2))
        .structuredScroll(
            scanContext.capture(),
            anyList(),
            anyString(),
            any(),
            sort.capture(),
            nullable(String.class),
            anyString(),
            anyInt());
    scanContext.getAllValues().forEach(ctx -> assertTrue(softDeletedIncluded(ctx)));
    final List<SortCriterion> urnOnly =
        List.of(new SortCriterion().setField("urn").setOrder(SortOrder.ASCENDING));
    sort.getAllValues().forEach(criteria -> assertEquals(criteria, urnOnly));
  }

  @Test
  public void liveFileIsSoftDeletedAndAnAlreadyDeletedFileIsSkipped() throws Exception {
    stubFileScan(scrollOf(null, LIVE_FILE, DELETED_FILE));
    stubFile(LIVE_FILE, false);
    stubFile(DELETED_FILE, true);

    final int removed =
        service.removeReferencesResumable(opContext, DATASET_A, null, DeleteCascadeListener.NOOP);

    verify(entityService)
        .ingestProposal(
            any(OperationContext.class),
            argThat(
                (MetadataChangeProposal mcp) ->
                    LIVE_FILE.equals(mcp.getEntityUrn())
                        && Constants.STATUS_ASPECT_NAME.equals(mcp.getAspectName())),
            any(AuditStamp.class),
            eq(false));
    verify(entityService, never())
        .ingestProposal(
            any(OperationContext.class),
            argThat((MetadataChangeProposal mcp) -> DELETED_FILE.equals(mcp.getEntityUrn())),
            any(AuditStamp.class),
            eq(false));
    assertEquals(removed, 1);
  }

  /** The listener sees each page's phase in order; a form has a search-reference phase too. */
  @Test
  public void phasesRunInOrderAndAResumeSkipsTheCompletedOnes() {
    service.removeReferencesResumable(opContext, FORM, null, recordingListener());
    assertEquals(
        checkpoints.stream().map(DeleteCascadeCheckpoint::phase).toList(),
        List.of(
            DeleteCascadeCheckpoint.PHASE_GRAPH,
            DeleteCascadeCheckpoint.PHASE_SEARCH_REFERENCES,
            DeleteCascadeCheckpoint.PHASE_FILES));

    setUp();
    service.removeReferencesResumable(
        opContext,
        FORM,
        new DeleteCascadeCheckpoint(DeleteCascadeCheckpoint.PHASE_SEARCH_REFERENCES),
        recordingListener());

    verifyNoInteractions(graphService);
    assertEquals(
        checkpoints.stream().map(DeleteCascadeCheckpoint::phase).toList(),
        List.of(DeleteCascadeCheckpoint.PHASE_SEARCH_REFERENCES, DeleteCascadeCheckpoint.PHASE_FILES));
    // The resumed phase starts over from its first page.
    verify(searchService)
        .structuredScroll(
            any(OperationContext.class),
            argThat((List<String> names) -> names != null && names.contains("dataset")),
            anyString(),
            any(),
            anyList(),
            isNull(),
            anyString(),
            anyInt());
  }

  @Test
  public void unknownCheckpointPhaseRestartsFromTheGraphPhase() {
    service.removeReferencesResumable(
        opContext,
        CONTAINER,
        new DeleteCascadeCheckpoint("retired-phase"),
        DeleteCascadeListener.NOOP);

    verify(graphService)
        .scrollRelatedEntities(
            any(OperationContext.class),
            nullable(Set.class),
            any(Filter.class),
            nullable(Set.class),
            any(Filter.class),
            anySet(),
            any(RelationshipFilter.class),
            anyList(),
            isNull(),
            anyString(),
            anyInt(),
            nullable(Long.class),
            nullable(Long.class));
  }

  // ---- helpers ----

  private DeleteCascadeListener recordingListener() {
    return checkpoints::add;
  }

  private void stubIncomingEdges(
      Urn deleted, @Nullable String scrollId, RelatedEntitiesScrollResult page) {
    when(graphService.scrollRelatedEntities(
            any(OperationContext.class),
            nullable(Set.class),
            eq(newFilter("urn", deleted.toString())),
            nullable(Set.class),
            eq(EMPTY_FILTER),
            anySet(),
            eq(newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING)),
            anyList(),
            scrollId == null ? isNull() : eq(scrollId),
            anyString(),
            anyInt(),
            nullable(Long.class),
            nullable(Long.class)))
        .thenReturn(page);
  }

  private void stubFormScan(ScrollResult page) {
    when(searchService.structuredScroll(
            any(OperationContext.class),
            argThat((List<String> names) -> names != null && names.contains("dataset")),
            anyString(),
            any(),
            anyList(),
            nullable(String.class),
            anyString(),
            anyInt()))
        .thenReturn(page);
  }

  private void stubFileScan(ScrollResult page) {
    when(searchService.structuredScroll(
            any(OperationContext.class),
            argThat(
                (List<String> names) ->
                    names != null && names.contains(Constants.DATAHUB_FILE_ENTITY_NAME)),
            anyString(),
            any(),
            anyList(),
            nullable(String.class),
            anyString(),
            anyInt()))
        .thenReturn(page);
  }

  private void stubFile(Urn file, boolean removed) throws Exception {
    when(entityService.getEntityV2(
            any(OperationContext.class),
            eq(Constants.DATAHUB_FILE_ENTITY_NAME),
            eq(file),
            anySet()))
        .thenReturn(
            entity(
                file,
                new EnvelopedAspect()
                    .setName(Constants.DATAHUB_FILE_INFO_ASPECT_NAME)
                    .setVersion(0L)
                    .setValue(new Aspect()),
                aspect(Constants.STATUS_ASPECT_NAME, new Status().setRemoved(removed), 1)));
  }

  private static Forms formsReferencing(Urn form) {
    return new Forms()
        .setIncompleteForms(new FormAssociationArray(List.of(new FormAssociation().setUrn(form))))
        .setCompletedForms(new FormAssociationArray())
        .setVerifications(new FormVerificationAssociationArray());
  }

  private static RelatedEntitiesScrollResult edges(
      @Nullable String nextScrollId, String relationshipType, Urn deleted, Urn... sources) {
    final List<RelatedEntities> list = new ArrayList<>();
    for (Urn source : sources) {
      list.add(
          new RelatedEntities(
              relationshipType,
              source.toString(),
              deleted.toString(),
              RelationshipDirection.INCOMING,
              null));
    }
    return RelatedEntitiesScrollResult.builder()
        .numResults(list.size())
        .pageSize(list.size())
        .scrollId(nextScrollId)
        .entities(list)
        .build();
  }

  private static ScrollResult scrollOf(@Nullable String nextScrollId, Urn... urns) {
    final SearchEntityArray entities = new SearchEntityArray();
    for (Urn urn : urns) {
      entities.add(new SearchEntity().setEntity(urn));
    }
    final ScrollResult result = new ScrollResult();
    result.setEntities(entities);
    result.setNumEntities(urns.length);
    if (nextScrollId != null) {
      result.setScrollId(nextScrollId);
    }
    return result;
  }

  private static EntityResponse entity(Urn urn, EnvelopedAspect... aspects) {
    final Map<String, EnvelopedAspect> map = new HashMap<>();
    for (EnvelopedAspect aspect : aspects) {
      map.put(aspect.getName(), aspect);
    }
    return new EntityResponse()
        .setUrn(urn)
        .setEntityName(urn.getEntityType())
        .setAspects(new EnvelopedAspectMap(map));
  }

  private static EnvelopedAspect aspect(String name, RecordTemplate value, long version) {
    return new EnvelopedAspect()
        .setName(name)
        .setVersion(0L)
        .setValue(new Aspect(value.data()))
        .setSystemMetadata(new SystemMetadata().setVersion(String.valueOf(version)));
  }

  private static EnvelopedAspect domains(long version, Urn... domainUrns) {
    return aspect(
        Constants.DOMAINS_ASPECT_NAME,
        new Domains().setDomains(new UrnArray(Arrays.asList(domainUrns))),
        version);
  }

  private static EnvelopedAspect container(long version, Urn containerUrn) {
    return aspect(
        Constants.CONTAINER_ASPECT_NAME, new Container().setContainer(containerUrn), version);
  }

  private static Domains domainsIn(MetadataChangeProposal mcp) {
    return GenericRecordUtils.deserializeAspect(
        mcp.getAspect().getValue(), mcp.getAspect().getContentType(), Domains.class);
  }

  private static boolean softDeletedIncluded(OperationContext ctx) {
    return Boolean.TRUE.equals(ctx.getSearchContext().getSearchFlags().isIncludeSoftDeleted());
  }

  /**
   * DOMAIN is referenced by A (page 1) and B (page 2) of the incoming-edge scan; the edges stay in
   * the fake graph, as they would until the referrers' MCLs are processed. Reads and writes go to a
   * {@link ReferrerStore}.
   */
  private ReferrerStore twoPageDomainScenario() throws Exception {
    stubIncomingEdges(DOMAIN, null, edges(PAGE_TWO, ASSOCIATED_WITH, DOMAIN, DATASET_A));
    stubIncomingEdges(DOMAIN, PAGE_TWO, edges(null, ASSOCIATED_WITH, DOMAIN, DATASET_B));
    final ReferrerStore store = new ReferrerStore();
    store.put(DATASET_A, 3, DOMAIN, OTHER_DOMAIN);
    store.put(DATASET_B, 6, DOMAIN);
    when(entityService.getEntityV2(
            any(OperationContext.class), eq("dataset"), any(Urn.class), anySet()))
        .thenAnswer(invocation -> store.read(invocation.getArgument(2)));
    when(entityService.ingestProposal(
            any(OperationContext.class),
            any(MetadataChangeProposal.class),
            any(AuditStamp.class),
            eq(false)))
        .thenAnswer(invocation -> store.write(invocation.getArgument(1)));
    return store;
  }

  /**
   * The referrers' domains aspects as a database would hold them: a read returns the latest value
   * and version, and a write commits only when its If-Version-Match equals the current version
   * (otherwise it returns null, as ingestProposal does for a context without RequestContext).
   */
  private static final class ReferrerStore {
    private final Map<Urn, List<Urn>> domains = new HashMap<>();
    private final Map<Urn, Long> versions = new HashMap<>();
    private final Map<Urn, Integer> writes = new HashMap<>();

    void put(Urn urn, long version, Urn... domainUrns) {
      domains.put(urn, List.of(domainUrns));
      versions.put(urn, version);
    }

    @Nullable
    EntityResponse read(Urn urn) {
      return domains.containsKey(urn)
          ? entity(urn, domains(versions.get(urn), domains.get(urn).toArray(new Urn[0])))
          : null;
    }

    @Nullable
    IngestResult write(MetadataChangeProposal mcp) {
      final Urn urn = mcp.getEntityUrn();
      final String expected = mcp.getHeaders().get(HTTP_HEADER_IF_VERSION_MATCH);
      if (!String.valueOf(versions.get(urn)).equals(expected)) {
        return null;
      }
      domains.put(urn, List.copyOf(domainsIn(mcp).getDomains()));
      versions.merge(urn, 1L, Long::sum);
      writes.merge(urn, 1, Integer::sum);
      return IngestResult.builder().sqlCommitted(true).build();
    }

    // Not named domains(): that would hide the enclosing class's domains(long, Urn...) in read().
    Map<Urn, List<Urn>> currentDomains() {
      return Map.copyOf(domains);
    }

    Map<Urn, Integer> writeCounts() {
      return Map.copyOf(writes);
    }
  }
}
