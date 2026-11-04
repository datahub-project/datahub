package com.linkedin.gms;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;

import com.linkedin.metadata.graph.cache.EntityGraphCache;
import com.linkedin.metadata.graph.cache.GraphReadResult;
import com.linkedin.metadata.graph.cache.KnownEntityGraph;
import com.linkedin.metadata.graph.cache.ReadMissReason;
import com.linkedin.metadata.graph.cache.TraversalDirection;
import com.linkedin.metadata.graph.cache.service.EntityGraphCacheScheduler;
import com.linkedin.metadata.graph.cache.service.EntityGraphCacheService;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.context.TestPropertySource;
import org.testng.annotations.Test;

/** Production {@code application.yaml} with the entity graph cache switched off. */
@TestPropertySource(properties = "datahub.gms.entityGraphCache.enabled=false")
public class SpringEntityGraphCacheDisabledTest extends AbstractGmsSpringContextTest {

  @Autowired
  @Qualifier("entityGraphCache")
  private EntityGraphCache entityGraphCache;

  @Test
  public void testDisabledEntityGraphCacheIsNoOp() {
    assertSame(entityGraphCache, EntityGraphCache.NO_OP);
    assertEquals(applicationContext.getBeanNamesForType(EntityGraphCacheService.class).length, 0);
    assertEquals(applicationContext.getBeanNamesForType(EntityGraphCacheScheduler.class).length, 0);

    // Readers fall back to the graph walk: no binding resolves and every read misses.
    assertFalse(entityGraphCache.bindingForKnownGraph(KnownEntityGraph.CONTAINER).isPresent());
    GraphReadResult read =
        entityGraphCache.expand(
            KnownEntityGraph.CONTAINER.getConfigKey(),
            KnownEntityGraph.CONTAINER.getExpectedBuildSource(),
            TraversalDirection.REVERSE,
            List.of("urn:li:container:parent"),
            10);
    assertEquals(read, GraphReadResult.miss(ReadMissReason.DISABLED));
  }
}
