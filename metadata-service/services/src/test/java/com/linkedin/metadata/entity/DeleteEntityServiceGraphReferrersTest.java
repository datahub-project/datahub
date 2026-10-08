package com.linkedin.metadata.entity;

import static com.linkedin.metadata.search.utils.QueryUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.models.graph.Edge;
import com.linkedin.metadata.aspect.models.graph.RelatedEntities;
import com.linkedin.metadata.aspect.models.graph.RelatedEntitiesScrollResult;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.run.DeleteReferencesResponse;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.mockito.invocation.Invocation;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * The graph references to an entity can be read before the entity is deleted and removed from that
 * list afterwards, when the graph no longer has the edges. Every other kind of reference is found
 * and removed as when the graph is read.
 */
public class DeleteEntityServiceGraphReferrersTest {
  private static final TagUrn DELETED_TAG = new TagUrn("deleted");
  private static final TagUrn KEPT_TAG = new TagUrn("kept");
  private static final Urn DATASET =
      UrnUtils.toDatasetUrn("snowflake", "graph_referrers", "PROD");
  private static final Urn OTHER_DATASET =
      UrnUtils.toDatasetUrn("snowflake", "graph_referrers_other", "PROD");
  private static final Urn CHART = UrnUtils.getUrn("urn:li:chart:(looker,graph_referrers)");

  private final OperationContext opContext =
      TestOperationContexts.systemContextNoSearchAuthorization();

  private EntityService<?> entityService;
  private GraphService graphService;
  private EntitySearchService searchService;
  private DeleteEntityService deleteEntityService;
  private List<MetadataChangeProposal> writes;

  @BeforeMethod
  public void setup() throws Exception {
    entityService = mock(EntityService.class);
    graphService = mock(GraphService.class);
    searchService = mock(EntitySearchService.class);
    deleteEntityService =
        new DeleteEntityService(entityService, graphService, searchService, null, null);
    writes = new ArrayList<>();

    final ScrollResult emptyScrollResult = new ScrollResult();
    emptyScrollResult.setEntities(new SearchEntityArray());
    emptyScrollResult.setNumEntities(0);
    when(searchService.structuredScroll(
            any(OperationContext.class),
            anyList(),
            anyString(),
            any(Filter.class),
            isNull(),
            nullable(String.class),
            anyString(),
            anyInt()))
        .thenReturn(emptyScrollResult);

    when(entityService.getEntityV2(
            any(OperationContext.class), eq(DATASET.getEntityType()), eq(DATASET), anySet()))
        .thenReturn(taggedDataset());
    when(entityService.ingestProposal(
            any(OperationContext.class),
            any(MetadataChangeProposal.class),
            any(AuditStamp.class),
            eq(false)))
        .thenAnswer(
            invocation -> {
              writes.add(invocation.getArgument(1));
              return IngestResult.builder().urn(DATASET).sqlCommitted(true).build();
            });
  }

  @Test
  public void getGraphReferrersReadsEveryPage() {
    graphPage(
        DELETED_TAG,
        null,
        page("p2", referrer("TaggedWith", DATASET), referrer("TaggedWith", OTHER_DATASET)));
    graphPage(DELETED_TAG, "p2", page(null, referrer("TaggedWith", CHART)));

    final List<RelatedEntities> referrers =
        deleteEntityService.getGraphReferrers(opContext, DELETED_TAG);

    assertEquals(
        referrers.stream().map(RelatedEntities::getUrn).collect(Collectors.toList()),
        List.of(DATASET.toString(), OTHER_DATASET.toString(), CHART.toString()));
    assertEquals(
        referrers.stream().map(RelatedEntities::getRelationshipType).collect(Collectors.toSet()),
        Set.of("TaggedWith"));
    verifyNoInteractions(entityService, searchService);
  }

  /** Read while the edge exists; the graph has none by the time the references are removed. */
  @Test
  public void theReferencesReadBeforeAreRemovedThoughTheGraphNoLongerHasThem() {
    graphPage(DELETED_TAG, null, page(null, referrer("TaggedWith", DATASET)), page(null));

    final List<RelatedEntities> referrers =
        deleteEntityService.getGraphReferrers(opContext, DELETED_TAG);
    final DeleteReferencesResponse response =
        deleteEntityService.deleteReferencesTo(opContext, DELETED_TAG, referrers);

    assertEquals(writes.size(), 1);
    assertEquals(tagsWritten(writes.get(0)), List.of(KEPT_TAG));
    assertEquals(response.getTotal(), Integer.valueOf(1));
    verify(graphService, times(1))
        .scrollRelatedEntities(
            any(OperationContext.class),
            nullable(Set.class),
            any(Filter.class),
            nullable(Set.class),
            any(Filter.class),
            anySet(),
            any(),
            anyList(),
            nullable(String.class),
            anyString(),
            anyInt(),
            nullable(Long.class),
            nullable(Long.class));
  }

  /** What reading the graph after the edges are gone finds: nothing, so the reference stays. */
  @Test
  public void readingTheGraphAfterItLostTheEdgesRemovesNothing() {
    graphPage(DELETED_TAG, null, page(null));

    deleteEntityService.deleteReferencesTo(opContext, DELETED_TAG, false);

    assertTrue(writes.isEmpty());
  }

  @DataProvider
  public Object[][] deletedEntities() {
    return new Object[][] {
      {DELETED_TAG},
      {UrnUtils.getUrn("urn:li:form:deleted")},
      {UrnUtils.getUrn("urn:li:structuredProperty:deleted")},
      {UrnUtils.getUrn("urn:li:container:deleted")}
    };
  }

  /** Every search-based part runs the same reads with the list as when the graph is read. */
  @Test(dataProvider = "deletedEntities")
  public void everyOtherKindOfReferenceIsFoundAsWhenTheGraphIsRead(final Urn deleted) {
    graphPage(deleted, null, page(null));

    final List<List<Object>> readingTheGraph =
        searchReads(service -> service.deleteReferencesTo(opContext, deleted, false));
    final List<List<Object>> withTheList =
        searchReads(service -> service.deleteReferencesTo(opContext, deleted, List.of()));

    assertFalse(readingTheGraph.isEmpty());
    assertEquals(withTheList, readingTheGraph);
  }

  /** The search reads one cleanup makes, each as its method name and arguments. */
  private List<List<Object>> searchReads(final Consumer<DeleteEntityService> cleanup) {
    clearInvocations(searchService);
    cleanup.accept(deleteEntityService);
    final List<List<Object>> reads = new ArrayList<>();
    for (final Invocation invocation : mockingDetails(searchService).getInvocations()) {
      final List<Object> read = new ArrayList<>();
      read.add(invocation.getMethod().getName());
      read.addAll(Arrays.asList(invocation.getArguments()));
      reads.add(read);
    }
    return reads;
  }

  /** The graph's incoming edges of {@code deleted} from {@code scrollId}, page after page. */
  private void graphPage(
      final Urn deleted,
      @Nullable final String scrollId,
      final RelatedEntitiesScrollResult first,
      final RelatedEntitiesScrollResult... then) {
    when(graphService.scrollRelatedEntities(
            any(OperationContext.class),
            nullable(Set.class),
            eq(newFilter("urn", deleted.toString())),
            nullable(Set.class),
            eq(EMPTY_FILTER),
            eq(ImmutableSet.of()),
            eq(newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING)),
            eq(Edge.EDGE_SORT_CRITERION),
            scrollId == null ? isNull() : eq(scrollId),
            eq("5m"),
            eq(1000),
            nullable(Long.class),
            nullable(Long.class)))
        .thenReturn(first, then);
  }

  private static RelatedEntitiesScrollResult page(
      @Nullable final String nextScrollId, final RelatedEntities... referrers) {
    return RelatedEntitiesScrollResult.builder()
        .numResults(referrers.length)
        .pageSize(referrers.length)
        .scrollId(nextScrollId)
        .entities(List.of(referrers))
        .build();
  }

  private static RelatedEntities referrer(final String relationshipType, final Urn referring) {
    return new RelatedEntities(
        relationshipType,
        referring.toString(),
        DELETED_TAG.toString(),
        RelationshipDirection.INCOMING,
        null);
  }

  private static EntityResponse taggedDataset() {
    final GlobalTags tags =
        new GlobalTags()
            .setTags(
                new TagAssociationArray(
                    List.of(
                        new TagAssociation().setTag(DELETED_TAG),
                        new TagAssociation().setTag(KEPT_TAG))));
    final EnvelopedAspect envelopedAspect =
        new EnvelopedAspect()
            .setName(Constants.GLOBAL_TAGS_ASPECT_NAME)
            .setValue(new Aspect(tags.data()))
            .setVersion(0L);
    final EntityResponse response = new EntityResponse();
    response.setUrn(DATASET);
    response.setEntityName(DATASET.getEntityType());
    response.setAspects(
        new EnvelopedAspectMap(Map.of(Constants.GLOBAL_TAGS_ASPECT_NAME, envelopedAspect)));
    return response;
  }

  private static List<TagUrn> tagsWritten(final MetadataChangeProposal proposal) {
    final GlobalTags written =
        GenericRecordUtils.deserializeAspect(
            proposal.getAspect().getValue(),
            proposal.getAspect().getContentType(),
            GlobalTags.class);
    return written.getTags().stream().map(TagAssociation::getTag).collect(Collectors.toList());
  }
}
