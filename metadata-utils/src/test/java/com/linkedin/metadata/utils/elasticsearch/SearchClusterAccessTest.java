package com.linkedin.metadata.utils.elasticsearch;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;

import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.SearchComponent;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class SearchClusterAccessTest {

  private static final IndexConvention CONVENTION =
      IndexConventionImpl.noPrefix("MD5", new EntityIndexConfiguration());

  @Test
  public void testEntityFamiliesMapToTheirComponent() {
    assertEquals(
        SearchClusterAccess.componentForEntityIndex(CONVENTION, "datasetindex_v2"),
        SearchComponent.SEARCH_V2);
    assertEquals(
        SearchClusterAccess.componentForEntityIndex(CONVENTION, "datasetindex_v3"),
        SearchComponent.SEARCH_V3);
    assertEquals(
        SearchClusterAccess.componentForEntityIndex(CONVENTION, "datasetindex_v2_semantic"),
        SearchComponent.SEMANTIC);
  }

  @Test
  public void testGraphIndexIsNotClassifiedAsSearchV2() {
    assertThrows(
        IllegalArgumentException.class,
        () -> SearchClusterAccess.componentForEntityIndex(CONVENTION, "graph_service_v1"));
  }

  @Test
  public void testFixedReturnsTheSameClientForEveryComponent() {
    SearchClientShim<?> client = Mockito.mock(SearchClientShim.class);
    SearchClusterAccess access = SearchClusterAccess.fixed(client);
    assertSame(access.clientFor(SearchComponent.SEARCH_V2), client);
    assertSame(access.clientFor(SearchComponent.GRAPH), client);
    assertSame(access.clientForIndex(CONVENTION, "datasetindex_v3"), client);
  }

  @Test
  public void testMixedEntityFamiliesThrow() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            SearchClusterAccess.componentForEntityIndicesOrNull(
                CONVENTION, new String[] {"datasetindex_v2", "datasetindex_v3"}));
  }

  @Test
  public void testUnrecognizedIndicesAreNullNotV2() {
    assertEquals(
        SearchClusterAccess.componentForEntityIndicesOrNull(
            CONVENTION, new String[] {"graph_service_v1", "datahub_usage_event"}),
        null);
  }

  @Test
  public void testWildcardPatternsResolveToTheirFamily() {
    assertEquals(
        SearchClusterAccess.tryComponentForEntityIndex(CONVENTION, "*index_v3*"),
        SearchComponent.SEARCH_V3);
    assertEquals(
        SearchClusterAccess.tryComponentForEntityIndex(CONVENTION, "*index_v2*"),
        SearchComponent.SEARCH_V2);
    assertEquals(
        SearchClusterAccess.tryComponentForEntityIndex(CONVENTION, "*index_v2_semantic*"),
        SearchComponent.SEMANTIC);
  }
}
