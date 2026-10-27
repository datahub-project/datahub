package com.linkedin.metadata.graph.cache.service.rebuild;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.ScopeMode;
import com.linkedin.metadata.graph.cache.CacheStatus;
import com.linkedin.metadata.graph.cache.GraphSnapshotSource;
import com.linkedin.metadata.graph.cache.TraversalDirection;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphDefinition;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphScope;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshot;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshotBuilder;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshotBuilder.BuildResult;
import com.linkedin.metadata.graph.cache.store.EntityGraphDistributedStore;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Set;
import org.testng.annotations.Test;

public class PartialGraphRebuildStrategyTest {

  @Test
  public void executeRebuildMergesExistingSnapshotAtPublishKey() {
    EntityGraphDistributedStore distributedStore = mock(EntityGraphDistributedStore.class);
    EntityGraphSnapshotBuilder snapshotBuilder = mock(EntityGraphSnapshotBuilder.class);
    OperationContext systemContext = TestOperationContexts.systemContextNoSearchAuthorization();
    PartialGraphRebuildStrategy strategy =
        new PartialGraphRebuildStrategy(distributedStore, snapshotBuilder, systemContext);

    EntityGraphDefinition definition =
        EntityGraphDefinition.builder()
            .graphId("glossary")
            .scope(EntityGraphScope.builder().mode(ScopeMode.PARTIAL).maxDepth(15).build())
            .buildSource(GraphSnapshotSource.GRAPH)
            .enabled(true)
            .build();
    EntityGraphSnapshot existing =
        EntityGraphSnapshot.builder().graphId("glossary").cacheKey("glossary@graph:abc").build();
    BuildResult buildResult =
        BuildResult.builder().status(CacheStatus.ACTIVE).snapshot(existing).build();

    when(distributedStore.getSnapshot("glossary@graph:abc")).thenReturn(existing);
    when(snapshotBuilder.buildPartial(
            eq(systemContext),
            eq(definition),
            eq(GraphSnapshotSource.GRAPH),
            eq(Set.of("urn:li:glossaryNode:root")),
            eq(TraversalDirection.FORWARD),
            isNull(),
            isNull(),
            eq("glossary@graph:abc")))
        .thenReturn(buildResult);

    BuildResult result =
        strategy.executeRebuild(
            definition,
            GraphSnapshotSource.GRAPH,
            Set.of("urn:li:glossaryNode:root"),
            "glossary@graph:abc",
            TraversalDirection.FORWARD);

    assertEquals(result.getStatus(), CacheStatus.ACTIVE);
    verify(snapshotBuilder)
        .buildPartial(
            eq(systemContext),
            eq(definition),
            eq(GraphSnapshotSource.GRAPH),
            eq(Set.of("urn:li:glossaryNode:root")),
            eq(TraversalDirection.FORWARD),
            isNull(),
            isNull(),
            eq("glossary@graph:abc"));
  }

  @Test
  public void rebuildClaimKeyUsesFailureMarkerWhenPublishHintAbsent() {
    assertEquals(
        PartialGraphRebuildStrategy.rebuildClaimKey(
            "glossary", GraphSnapshotSource.GRAPH, null, List.of("urn:li:glossaryNode:root")),
        PartialGraphRebuildStrategy.failureMarkerKey(
            "glossary", GraphSnapshotSource.GRAPH, "urn:li:glossaryNode:root"));
  }
}
