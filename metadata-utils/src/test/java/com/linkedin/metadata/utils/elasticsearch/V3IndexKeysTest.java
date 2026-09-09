package com.linkedin.metadata.utils.elasticsearch;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;

import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

public class V3IndexKeysTest {

  @Test
  public void unsetSearchGroupUsesEntityName() {
    EntitySpec spec = mock(EntitySpec.class);
    when(spec.getName()).thenReturn("dataset");
    when(spec.getSearchGroup()).thenReturn(null);
    assertEquals(V3IndexKeys.resolve(spec), "dataset");
  }

  @Test
  public void explicitSearchGroupWinsOverEntityName() {
    EntitySpec spec = mock(EntitySpec.class);
    when(spec.getName()).thenReturn("dataset");
    when(spec.getSearchGroup()).thenReturn("primary");
    assertEquals(V3IndexKeys.resolve(spec), "primary");
  }

  @Test
  public void groupEntitySpecsCollapsesExplicitGroup() {
    EntitySpec dataset = mock(EntitySpec.class);
    when(dataset.getName()).thenReturn("dataset");
    when(dataset.getSearchGroup()).thenReturn("primary");
    EntitySpec chart = mock(EntitySpec.class);
    when(chart.getName()).thenReturn("chart");
    when(chart.getSearchGroup()).thenReturn("primary");
    EntitySpec query = mock(EntitySpec.class);
    when(query.getName()).thenReturn("query");
    when(query.getSearchGroup()).thenReturn(null);

    EntityRegistry registry = mock(EntityRegistry.class);
    when(registry.getEntitySpecs())
        .thenReturn(Map.of("dataset", dataset, "chart", chart, "query", query));

    Map<String, List<EntitySpec>> grouped = V3IndexKeys.groupEntitySpecs(registry);
    assertEquals(grouped.get("primary").size(), 2);
    assertEquals(grouped.get("query").size(), 1);
    assertSame(V3IndexKeys.groupEntitySpecs(registry), grouped);
    assertEquals(V3IndexKeys.entitySpecsForKey(registry, "primary").size(), 2);
  }
}
