package com.linkedin.metadata.graph.cache.service.read;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.ScopeMode;
import com.linkedin.metadata.graph.cache.GraphReadResult;
import com.linkedin.metadata.graph.cache.GraphSnapshotSource;
import com.linkedin.metadata.graph.cache.ReadMissReason;
import com.linkedin.metadata.graph.cache.ReadMode;
import com.linkedin.metadata.graph.cache.TraversalDirection;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphDefinition;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphScope;
import com.linkedin.metadata.graph.cache.service.freshness.SnapshotFreshnessEvaluator;
import com.linkedin.metadata.graph.cache.service.rebuild.GraphCacheRebuilder;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshotBuilder;
import com.linkedin.metadata.graph.cache.store.EntityGraphDistributedStore;
import com.linkedin.metadata.graph.cache.store.EntityGraphLocalViewCache;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collections;
import java.util.Set;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class GraphCacheReaderTest {

  private GraphCacheReader reader;
  private EntityGraphDefinition fullDefinition;
  private EntityGraphDefinition partialDefinition;

  @BeforeMethod
  public void setUp() {
    reader =
        new GraphCacheReader(
            mock(EntityGraphDistributedStore.class),
            mock(EntityGraphLocalViewCache.class),
            mock(EntityGraphSnapshotBuilder.class),
            TestOperationContexts.Builder.builder().buildSystemContext(),
            mock(SnapshotFreshnessEvaluator.class),
            mock(GraphCacheRebuilder.class));
    fullDefinition =
        EntityGraphDefinition.builder()
            .graphId("domain")
            .enabled(true)
            .buildSource(GraphSnapshotSource.SEARCH)
            .scope(EntityGraphScope.builder().mode(ScopeMode.FULL).build())
            .build();
    partialDefinition =
        EntityGraphDefinition.builder()
            .graphId("glossary")
            .enabled(true)
            .buildSource(GraphSnapshotSource.GRAPH)
            .scope(EntityGraphScope.builder().mode(ScopeMode.PARTIAL).maxDepth(15).build())
            .build();
  }

  @Test
  public void expandRejectsDisabledDefinition() {
    EntityGraphDefinition disabled =
        EntityGraphDefinition.builder()
            .graphId("domain")
            .enabled(false)
            .buildSource(GraphSnapshotSource.SEARCH)
            .scope(EntityGraphScope.builder().mode(ScopeMode.FULL).build())
            .build();

    GraphReadResult result =
        reader.expand(
            disabled,
            GraphSnapshotSource.SEARCH,
            TraversalDirection.FORWARD,
            Set.of("urn:li:domain:root"),
            100,
            15,
            ReadMode.CACHED);

    assertTrue(result.isMiss());
    assertEquals(((GraphReadResult.Miss) result).reason(), ReadMissReason.INVALID_REQUEST);
  }

  @Test
  public void expandRejectsUnsupportedSource() {
    GraphReadResult result =
        reader.expand(
            fullDefinition,
            GraphSnapshotSource.GRAPH,
            TraversalDirection.FORWARD,
            Set.of("urn:li:domain:root"),
            100,
            15,
            ReadMode.CACHED);

    assertEquals(((GraphReadResult.Miss) result).reason(), ReadMissReason.INVALID_REQUEST);
  }

  @Test
  public void expandRejectsPartialGraphWithEmptyRoots() {
    GraphReadResult result =
        reader.expand(
            partialDefinition,
            GraphSnapshotSource.GRAPH,
            TraversalDirection.REVERSE,
            Collections.emptyList(),
            100,
            15,
            ReadMode.CACHED);

    assertEquals(((GraphReadResult.Miss) result).reason(), ReadMissReason.INVALID_REQUEST);
  }

  @Test
  public void walkRejectsBlankSeed() {
    var result =
        reader.walkOrderedForwardAncestors(
            fullDefinition, GraphSnapshotSource.SEARCH, "   ", 10, ReadMode.CACHED);

    assertEquals(
        ((com.linkedin.metadata.graph.cache.AncestorWalkResult.Miss) result).reason(),
        ReadMissReason.INVALID_REQUEST);
  }

  @Test
  public void walkRejectsNonPositiveDepth() {
    var result =
        reader.walkOrderedForwardAncestors(
            fullDefinition,
            GraphSnapshotSource.SEARCH,
            "urn:li:domain:child",
            0,
            ReadMode.EPHEMERAL);

    assertEquals(
        ((com.linkedin.metadata.graph.cache.AncestorWalkResult.Miss) result).reason(),
        ReadMissReason.INVALID_REQUEST);
  }
}
