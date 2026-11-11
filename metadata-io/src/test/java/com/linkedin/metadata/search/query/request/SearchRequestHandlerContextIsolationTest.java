package com.linkedin.metadata.search.query.request;

import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static io.datahubproject.test.search.SearchTestUtils.TEST_ES_SEARCH_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.TEST_SEARCH_SERVICE_CONFIG;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchRequestHandler;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SearchContext;
import io.datahubproject.test.search.config.SearchCommonTestConfiguration;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

/**
 * Handlers are cached process-wide, and each one copies the searchable field metadata of the
 * context that built it. Contexts that do not share that metadata must not share a handler.
 */
@Import(SearchCommonTestConfiguration.class)
public class SearchRequestHandlerContextIsolationTest extends AbstractTestNGSpringContextTests {

  @Autowired
  @Qualifier("queryOperationContext")
  private OperationContext operationContext;

  /**
   * Distinct from every other suite's configuration so this test owns its cache entry, and reused
   * across both lookups so the two differ only by context.
   */
  private static final ElasticSearchConfiguration CONFIG =
      TEST_ES_SEARCH_CONFIG.toBuilder()
          .search(TEST_ES_SEARCH_CONFIG.getSearch().toBuilder().maxTermBucketSize(997).build())
          .build();

  @Test
  public void testFieldTypesSurviveAContextThatHasNone() {
    EntitySpec dataset = operationContext.getEntityRegistry().getEntitySpec(DATASET_ENTITY_NAME);
    OperationContext noFieldTypes = withSearchableFieldTypes(Map.of());

    SearchRequestHandler unaware = handler(noFieldTypes, dataset);
    SearchRequestHandler aware = handler(operationContext, dataset);
    assertNotSame(aware, unaware);

    // isLatest is a boolean searchable field, so an EQUAL criterion on it is a term on the field
    // itself. A handler holding the empty map cannot know the type and falls back to .keyword.
    assertTrue(latestVersionFilter(unaware).contains("isLatest.keyword"));
    assertFalse(latestVersionFilter(aware).contains("isLatest.keyword"));
  }

  private String latestVersionFilter(SearchRequestHandler handler) {
    return handler
        .getFilterQuery(filterNonLatestVersions(operationContext), (Filter) null)
        .toString();
  }

  private static SearchRequestHandler handler(
      OperationContext opContext, @Nullable EntitySpec entitySpec) {
    return SearchRequestHandler.getBuilder(
        opContext,
        entitySpec,
        CONFIG,
        null,
        QueryFilterRewriteChain.EMPTY,
        TEST_SEARCH_SERVICE_CONFIG);
  }

  private static OperationContext filterNonLatestVersions(OperationContext opContext) {
    return opContext.withSearchFlags(flags -> flags.setFilterNonLatestVersions(true));
  }

  private OperationContext withSearchableFieldTypes(
      Map<String, Set<SearchableAnnotation.FieldType>> fieldTypes) {
    SearchContext searchContext =
        operationContext.getSearchContext().toBuilder().searchableFieldTypes(fieldTypes).build();
    return operationContext.toBuilder()
        .searchContext(searchContext)
        .build(operationContext.getSessionAuthentication(), false);
  }
}
