package com.linkedin.metadata.graph.cache.service;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.PopulationStrategy;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.ScopeMode;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphDefinition;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphScope;
import com.linkedin.metadata.graph.cache.config.EntityGraphRegistry;
import java.util.List;
import org.testng.annotations.Test;

public class EntityGraphCacheSchedulerTest {

  @Test
  public void schedulesRebuildForScheduledFullGraphs() {
    EntityGraphRegistry registry = mock(EntityGraphRegistry.class);
    EntityGraphCacheService cacheService = mock(EntityGraphCacheService.class);
    EntityGraphDefinition scheduled =
        EntityGraphDefinition.builder()
            .graphId("domain")
            .scope(EntityGraphScope.builder().mode(ScopeMode.FULL).build())
            .populationStrategy(PopulationStrategy.SCHEDULED)
            .populationIntervalSeconds(5)
            .build();
    when(registry.getScheduledFullGraphs()).thenReturn(List.of(scheduled));

    try (EntityGraphCacheScheduler scheduler =
        new EntityGraphCacheScheduler(registry, cacheService)) {
      verify(cacheService, timeout(8000).atLeastOnce()).scheduledRebuild(scheduled);
    }
  }
}
