package com.linkedin.metadata.search;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.config.search.SearchServiceConfiguration;
import com.linkedin.metadata.search.cache.EntityDocCountCache;
import com.linkedin.metadata.search.client.CachingEntitySearchService;
import com.linkedin.metadata.search.ranker.SearchRanker;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import org.testng.annotations.Test;

/**
 * Entity types only a newer version knows (e.g. requested by a client built for it, after a
 * rollback) are dropped from a search instead of failing it; none left means an empty result, never
 * a search across every type.
 */
public class SearchServiceUnknownEntityTypeTest {

  private final OperationContext opContext =
      TestOperationContexts.systemContextNoSearchAuthorization();
  private final SearchService searchService =
      new SearchService(
          mock(EntityDocCountCache.class),
          mock(CachingEntitySearchService.class),
          mock(SearchRanker.class),
          mock(SearchServiceConfiguration.class));

  @Test
  public void testUnknownEntityTypesAreDropped() {
    assertEquals(
        searchService.getEntitiesToSearch(
            opContext, List.of("dataset", "entityFromNewerBuild"), 10),
        List.of("dataset"));
  }

  @Test
  public void testNamesMatchCaseInsensitivelyButUnderscoredAliasesAreUnknown() {
    // ESSearchDAO resolves names with getEntitySpec, which ignores case but has no "data_product"
    // alias; keeping the alias would fail the search downstream.
    assertEquals(
        searchService.getEntitiesToSearch(
            opContext, List.of("dataProduct", "DATAPRODUCT", "data_product"), 10),
        List.of("dataproduct", "dataproduct"));
  }

  @Test
  public void testOnlyUnknownEntityTypesGiveNothingToSearch() {
    assertTrue(
        searchService
            .getEntitiesToSearch(opContext, List.of("entityFromNewerBuild"), 10)
            .isEmpty());
  }
}
