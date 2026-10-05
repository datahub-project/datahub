package com.linkedin.metadata.search.elasticsearch.query;

import static io.datahubproject.test.search.SearchTestUtils.TEST_OS_SEARCH_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.TEST_SEARCH_SERVICE_CONFIG;
import static org.testng.Assert.assertEquals;

import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.utils.elasticsearch.ConfiguredIndexPrefixResolver;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SearchContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import org.testng.annotations.Test;

/** Which entity indices an aggregation without an entity list reads. */
public class ESSearchDAOAggregateByValueTest {

  private static final EntityIndexConfiguration DUAL_WRITE =
      EntityIndexConfiguration.builder()
          .v2(EntityIndexVersionConfiguration.builder().enabled(true).build())
          .v3(EntityIndexVersionConfiguration.builder().enabled(true).build())
          .build();

  // Both families are enabled, so the convention's all-entity patterns hold V2 and V3
  private final OperationContext opContext =
      TestOperationContexts.systemContextNoSearchAuthorization(
          SearchContext.EMPTY.toBuilder()
              .indexConvention(
                  new IndexConventionImpl(
                      IndexConventionImpl.IndexConventionConfig.builder().hashIdAlgo("MD5").build(),
                      new ConfiguredIndexPrefixResolver("agg"),
                      DUAL_WRITE))
              .build());
  private final IndexConvention convention = opContext.getSearchContext().getIndexConvention();

  @Test
  public void testReadsOnlyV2WhileDualWriting() {
    // Adding V3 would count every dual-written entity twice, and the search client rejects a
    // request that mixes the two families
    List<String> v3Patterns = convention.getV3EntityIndexPatterns(opContext);
    List<String> v2Patterns =
        convention.getAllEntityIndicesPatterns(opContext).stream()
            .filter(pattern -> !v3Patterns.contains(pattern))
            .toList();

    assertEquals(List.of(aggregationIndices(DUAL_WRITE)), v2Patterns);
    // An empty entity list must not become an empty index list, which searches every index
    assertEquals(List.of(aggregationIndices(DUAL_WRITE, List.of())), v2Patterns);
  }

  @Test
  public void testReadsOnlyV3WithKeywordReads() {
    EntityIndexConfiguration keywordRead =
        DUAL_WRITE.toBuilder()
            .v3(
                EntityIndexVersionConfiguration.builder()
                    .enabled(true)
                    .keywordReadEnabled(true)
                    .build())
            .build();

    assertEquals(
        List.of(aggregationIndices(keywordRead)), convention.getV3EntityIndexPatterns(opContext));
  }

  private String[] aggregationIndices(EntityIndexConfiguration entityIndex) {
    return aggregationIndices(entityIndex, null);
  }

  private String[] aggregationIndices(
      EntityIndexConfiguration entityIndex, List<String> entityNames) {
    return new ESSearchDAO(
            false,
            TEST_OS_SEARCH_CONFIG.toBuilder().entityIndex(entityIndex).build(),
            null,
            QueryFilterRewriteChain.EMPTY,
            TEST_SEARCH_SERVICE_CONFIG)
        .buildAggregateByValue(opContext, entityNames, "platform", null, 10)
        .indices();
  }
}
