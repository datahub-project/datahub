package com.linkedin.datahub.graphql.loaders;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.graph.cache.EntityGraphBinding;
import com.linkedin.metadata.graph.cache.EntityGraphCache;
import com.linkedin.metadata.graph.cache.GraphSnapshotSource;
import com.linkedin.metadata.graph.cache.KnownEntityGraph;
import com.linkedin.metadata.graph.cache.client.BoundHierarchyAccess;
import com.linkedin.metadata.graph.cache.client.HierarchyReadSpecs;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import org.dataloader.Try;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class HierarchyAncestorWalksTest {

  private static final Urn A = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,a,PROD)");
  private static final Urn B = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,b,PROD)");
  private static final Urn C = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,c,PROD)");
  private static final Urn PARENT = UrnUtils.getUrn("urn:li:container:parent");

  @Test
  public void toTriesMapsEachKeyToItsChainOrFailure() {
    final RuntimeException failure = new RuntimeException("read failed");
    final BoundHierarchyAccess.BatchParents batch =
        new BoundHierarchyAccess.BatchParents(Map.of(A, List.of(PARENT)), Map.of(B, failure));

    final Map<Urn, Try<List<Urn>>> tries =
        HierarchyAncestorWalks.toTries(List.of(A, B, C), batch, null);

    assertEquals(tries.get(A).get(), List.of(PARENT));
    assertSame(tries.get(B).getThrowable(), failure);
    // A key the batch did not report fails loudly instead of resolving to no parents.
    assertTrue(tries.get(C).getThrowable() instanceof IllegalStateException);
  }

  @Test
  public void toTriesFailsEveryKeyWithTheUnwrappedBatchError() {
    final RuntimeException cause = new RuntimeException("batch task failed");

    final Map<Urn, Try<List<Urn>>> tries =
        HierarchyAncestorWalks.toTries(List.of(A, B), null, new CompletionException(cause));

    assertSame(tries.get(A).getThrowable(), cause);
    assertSame(tries.get(B).getThrowable(), cause);
  }

  /** End to end: when the batch walk itself throws, every key's future fails, none is lost. */
  @Test
  public void resolveBatchFailsEveryKeyWhenTheWalkThrows() {
    final RuntimeException cause = new RuntimeException("graph cache unavailable");
    final EntityGraphCache cache = Mockito.mock(EntityGraphCache.class);
    Mockito.when(cache.bindingForKnownGraph(KnownEntityGraph.CONTAINER)).thenThrow(cause);

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

    final Map<Urn, Try<List<Urn>>> tries =
        HierarchyAncestorWalks.resolveBatch(
                List.of(A, B),
                opContext,
                HierarchyReadSpecs.container(
                    EntityGraphBinding.builder()
                        .graphId("container")
                        .source(GraphSnapshotSource.GRAPH)
                        .build()),
                50,
                "test")
            .join();

    assertEquals(tries.size(), 2);
    assertSame(tries.get(A).getThrowable(), cause);
    assertSame(tries.get(B).getThrowable(), cause);
  }
}
