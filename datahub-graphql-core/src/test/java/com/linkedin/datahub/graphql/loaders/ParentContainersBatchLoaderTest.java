package com.linkedin.datahub.graphql.loaders;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.container.ContainerProperties;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.graph.cache.AncestorWalkResult;
import com.linkedin.metadata.graph.cache.EntityGraphBinding;
import com.linkedin.metadata.graph.cache.EntityGraphCache;
import com.linkedin.metadata.graph.cache.GraphSnapshotSource;
import com.linkedin.metadata.graph.cache.KnownEntityGraph;
import com.linkedin.metadata.graph.cache.ReadMode;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ParentContainersBatchLoaderTest {

  private static final Urn ROOT = UrnUtils.getUrn("urn:li:container:root");
  private static final Urn MID = UrnUtils.getUrn("urn:li:container:mid");
  private static final Urn LEAF_A = UrnUtils.getUrn("urn:li:container:leafA");
  private static final Urn LEAF_B = UrnUtils.getUrn("urn:li:container:leafB");

  private EntityClient _entityClient;
  private QueryContext _context;

  @BeforeMethod
  public void setup() {
    _entityClient = Mockito.mock(EntityClient.class);
    _context = getMockAllowContext();
  }

  private static EntityResponse containerResponse(Urn urn) {
    final RecordTemplate props = new ContainerProperties().setName(urn.getId());
    final Map<String, EnvelopedAspect> aspects = new HashMap<>();
    aspects.put(
        Constants.CONTAINER_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(props.data())));
    return new EntityResponse()
        .setUrn(urn)
        .setEntityName(Constants.CONTAINER_ENTITY_NAME)
        .setAspects(new EnvelopedAspectMap(aspects));
  }

  @SuppressWarnings("unchecked")
  private void stubBatchGet(Set<Urn> known) throws Exception {
    Mockito.when(
            _entityClient.batchGetV2(any(), any(String.class), any(Set.class), nullable(Set.class)))
        .thenAnswer(
            inv -> {
              final Set<Urn> requested = inv.getArgument(2);
              final Map<Urn, EntityResponse> out = new HashMap<>();
              for (Urn u : requested) {
                if (known.contains(u)) {
                  out.put(u, containerResponse(u));
                }
              }
              return out;
            });
  }

  @SuppressWarnings("unchecked")
  private ArgumentCaptor<Set<Urn>> captureBatchGet(int times) throws Exception {
    final ArgumentCaptor<Set<Urn>> captor = ArgumentCaptor.forClass(Set.class);
    Mockito.verify(_entityClient, Mockito.times(times))
        .batchGetV2(any(), any(String.class), captor.capture(), nullable(Set.class));
    return captor;
  }

  /**
   * The point of the change: ancestors of every key are fetched together, and an ancestor shared by
   * two keys is fetched once.
   */
  @Test
  public void testSharedAncestorsFetchedOnce() throws Exception {
    stubBatchGet(Set.of(ROOT, MID));
    final Map<Urn, List<Urn>> chains =
        Map.of(LEAF_A, List.of(MID, ROOT), LEAF_B, List.of(MID, ROOT));

    final List<com.linkedin.datahub.graphql.generated.ParentContainersResult> out =
        ParentContainersBatchLoader.batchLoadForTest(
            List.of(LEAF_A, LEAF_B), _context, _entityClient, chains);

    assertEquals(out.size(), 2);
    assertEquals(out.get(0).getCount(), 2);
    assertEquals(out.get(1).getCount(), 2);
    // One call for both keys, carrying the union of ancestors with no duplicates.
    assertEquals(captureBatchGet(1).getValue(), Set.of(MID, ROOT));
  }

  /** Hierarchy order must survive the shared, unordered response map. */
  @Test
  public void testHierarchyOrderPreserved() throws Exception {
    stubBatchGet(Set.of(ROOT, MID));
    final Map<Urn, List<Urn>> chains = Map.of(LEAF_A, List.of(MID, ROOT));

    final var result =
        ParentContainersBatchLoader.batchLoadForTest(
                List.of(LEAF_A), _context, _entityClient, chains)
            .get(0);

    assertEquals(result.getContainers().get(0).getUrn(), MID.toString());
    assertEquals(result.getContainers().get(1).getUrn(), ROOT.toString());
  }

  /** A top-level entity has no ancestors, so it must not trigger a fetch at all. */
  @Test
  public void testNoAncestorsIssuesNoFetch() throws Exception {
    stubBatchGet(Set.of());
    final var out =
        ParentContainersBatchLoader.batchLoadForTest(
            List.of(ROOT), _context, _entityClient, Map.of(ROOT, List.of()));

    assertEquals(out.get(0).getCount(), 0);
    Mockito.verify(_entityClient, Mockito.never())
        .batchGetV2(any(), any(String.class), any(Set.class), nullable(Set.class));
  }

  /** An ancestor the caller cannot see is skipped, not rendered as an empty container. */
  @Test
  public void testUnauthorizedAncestorSkipped() throws Exception {
    stubBatchGet(Set.of(ROOT)); // MID withheld
    final var out =
        ParentContainersBatchLoader.batchLoadForTest(
            List.of(LEAF_A), _context, _entityClient, Map.of(LEAF_A, List.of(MID, ROOT)));

    assertEquals(out.get(0).getCount(), 1);
    assertEquals(out.get(0).getContainers().get(0).getUrn(), ROOT.toString());
  }

  /**
   * End-to-end through the real batchLoad, including the concurrent ancestor walk. Two keys share a
   * hierarchy, so the whole batch must cost one hydration call.
   */
  @Test
  public void testRealBatchLoadWalksConcurrentlyAndFetchesOnce() throws Exception {
    stubBatchGet(Set.of(ROOT, MID));
    final QueryContext ctx =
        contextWithAncestors(Map.of(LEAF_A, List.of(MID, ROOT), LEAF_B, List.of(MID, ROOT)));

    final List<com.linkedin.datahub.graphql.generated.ParentContainersResult> out =
        ParentContainersBatchLoader.batchLoad(List.of(LEAF_A, LEAF_B), ctx, _entityClient);

    assertEquals(out.size(), 2);
    assertEquals(out.get(0).getCount(), 2);
    assertEquals(out.get(0).getContainers().get(0).getUrn(), MID.toString());
    assertEquals(out.get(1).getCount(), 2);
    assertEquals(captureBatchGet(1).getValue(), Set.of(MID, ROOT));
  }

  /** A QueryContext whose hierarchy cache answers the walk for each supplied seed. */
  private static QueryContext contextWithAncestors(Map<Urn, List<Urn>> chains) {
    final EntityGraphCache cache = Mockito.mock(EntityGraphCache.class);
    final EntityGraphBinding binding =
        EntityGraphBinding.builder().graphId("container").source(GraphSnapshotSource.GRAPH).build();
    Mockito.when(cache.bindingForKnownGraph(KnownEntityGraph.CONTAINER))
        .thenReturn(Optional.of(binding));
    chains.forEach(
        (seed, ancestors) ->
            Mockito.when(
                    cache.walkOrderedForwardAncestors(
                        eq("container"),
                        eq(GraphSnapshotSource.GRAPH),
                        eq(seed.toString()),
                        eq(50),
                        eq(ReadMode.CACHED)))
                .thenReturn(
                    AncestorWalkResult.fromAncestors(
                        ancestors.stream().map(Urn::toString).collect(Collectors.toList()))));

    final OperationContext base = TestOperationContexts.systemContextNoSearchAuthorization();
    final OperationContext opContext =
        base.toBuilder()
            .retrieverContext(
                RetrieverContext.builder()
                    .graphRetriever(GraphRetriever.EMPTY)
                    .searchRetriever(SearchRetriever.EMPTY)
                    .cachingAspectRetriever(CachingAspectRetriever.EMPTY)
                    .aspectRetriever(Mockito.mock(AspectRetriever.class))
                    .entityGraphCache(cache)
                    .build())
            .build(base.getSessionAuthentication(), false);

    final QueryContext ctx = Mockito.mock(QueryContext.class);
    Mockito.when(ctx.getOperationContext()).thenReturn(opContext);
    Mockito.when(ctx.getMaxParentDepth()).thenReturn(50);
    return ctx;
  }

  /** DataLoader's contract is positional, including when a key repeats in one batch. */
  @Test
  public void testDuplicateKeysMapBackPositionally() throws Exception {
    stubBatchGet(Set.of(ROOT, MID));
    final Map<Urn, List<Urn>> chains = Map.of(LEAF_A, List.of(MID, ROOT), LEAF_B, List.of(ROOT));

    final var out =
        ParentContainersBatchLoader.batchLoadForTest(
            List.of(LEAF_A, LEAF_B, LEAF_A), _context, _entityClient, chains);

    assertEquals(out.size(), 3);
    assertEquals(out.get(0).getCount(), 2);
    assertEquals(out.get(1).getCount(), 1);
    assertEquals(out.get(2).getCount(), 2);
  }
}
