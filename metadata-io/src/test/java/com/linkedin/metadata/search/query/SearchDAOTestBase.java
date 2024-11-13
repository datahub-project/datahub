package com.linkedin.metadata.search.query;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.search.fixtures.SampleDataFixtureTestBase.DEFAULT_CONFIG;
import static com.linkedin.metadata.search.fixtures.SampleDataFixtureTestBase.MAPPER;
import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;
import static com.linkedin.metadata.utils.SearchUtil.AGGREGATION_SEPARATOR_CHAR;
import static com.linkedin.metadata.utils.SearchUtil.ES_INDEX_FIELD;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.data.template.LongMap;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.AggregationMetadata;
import com.linkedin.metadata.search.AggregationMetadataArray;
import com.linkedin.metadata.search.FilterValueArray;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.query.ESSearchDAO;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.utils.SearchUtil;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.opensearch.action.explain.ExplainResponse;
import org.opensearch.client.RestHighLevelClient;
import org.springframework.core.io.ClassPathResource;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

public abstract class SearchDAOTestBase extends AbstractTestNGSpringContextTests {

  protected abstract RestHighLevelClient getSearchClient();

  protected abstract SearchConfiguration getSearchConfiguration();

  protected abstract OperationContext getOperationContext();

  protected abstract ESSearchDAO getESSearchDao();

  protected abstract CustomSearchConfiguration getCustomSearchConfiguration();

  @Test
  public void testTransformFilterForEntitiesNoChange() {
    Criterion c =
        buildCriterion("tags.keyword", Condition.EQUAL, "urn:ki:tag:abc", "urn:li:tag:def");

    Filter f =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion().setAnd(new CriterionArray(c))));

    Filter transformedFilter =
        SearchUtil.transformFilterForEntities(
            f, getOperationContext().getSearchContext().getIndexConvention());
    assertEquals(f, transformedFilter);
  }

  @Test
  public void testTransformFilterForEntitiesNullFilter() {
    Filter transformedFilter =
        SearchUtil.transformFilterForEntities(
            null, getOperationContext().getSearchContext().getIndexConvention());
    assertNotNull(getOperationContext().getSearchContext().getIndexConvention());
    assertEquals(null, transformedFilter);
  }

  @Test
  public void testTransformFilterForEntitiesWithChanges() {

    Criterion c = buildCriterion("_entityType", Condition.EQUAL, "dataset");

    Filter f =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion().setAnd(new CriterionArray(c))));
    Filter originalF = null;
    try {
      originalF = f.copy();
    } catch (CloneNotSupportedException e) {
      fail(e.getMessage());
    }
    assertEquals(f, originalF);

    Filter transformedFilter =
        SearchUtil.transformFilterForEntities(
            f, getOperationContext().getSearchContext().getIndexConvention());
    assertNotEquals(originalF, transformedFilter);

    Criterion expectedNewCriterion =
        buildCriterion(ES_INDEX_FIELD, Condition.EQUAL, "smpldat_datasetindex_v2");

    Filter expectedNewFilter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion().setAnd(new CriterionArray(expectedNewCriterion))));

    assertEquals(expectedNewFilter, transformedFilter);
  }

  @Test
  public void testTransformFilterForEntitiesWithUnderscore() {

    Criterion c = buildCriterion("_entityType", Condition.EQUAL, "data_job");

    Filter f =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion().setAnd(new CriterionArray(c))));
    Filter originalF = null;
    try {
      originalF = f.copy();
    } catch (CloneNotSupportedException e) {
      fail(e.getMessage());
    }
    assertEquals(f, originalF);

    Filter transformedFilter =
        SearchUtil.transformFilterForEntities(
            f, getOperationContext().getSearchContext().getIndexConvention());
    assertNotEquals(originalF, transformedFilter);

    Criterion expectedNewCriterion =
        buildCriterion(ES_INDEX_FIELD, Condition.EQUAL, "smpldat_datajobindex_v2");

    Filter expectedNewFilter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion().setAnd(new CriterionArray(expectedNewCriterion))));

    assertEquals(transformedFilter, expectedNewFilter);
  }

  @Test
  public void testTransformFilterForEntitiesWithSomeChanges() {

    Criterion criterionChanged = buildCriterion("_entityType", Condition.EQUAL, "dataset");

    Criterion criterionUnchanged =
        buildCriterion("tags.keyword", Condition.EQUAL, "urn:li:tag:abc", "urn:li:tag:def");

    Filter f =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(new CriterionArray(criterionChanged, criterionUnchanged))));
    Filter originalF = null;
    try {
      originalF = f.copy();
    } catch (CloneNotSupportedException e) {
      fail(e.getMessage());
    }
    assertEquals(f, originalF);

    Filter transformedFilter =
        SearchUtil.transformFilterForEntities(
            f, getOperationContext().getSearchContext().getIndexConvention());
    assertNotEquals(originalF, transformedFilter);

    Criterion expectedNewCriterion =
        buildCriterion(ES_INDEX_FIELD, Condition.EQUAL, "smpldat_datasetindex_v2");

    Filter expectedNewFilter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(new CriterionArray(expectedNewCriterion, criterionUnchanged))));

    assertEquals(expectedNewFilter, transformedFilter);
  }

  @Test
  public void testTransformIndexIntoEntityNameSingle() {
    ESSearchDAO searchDAO =
        new ESSearchDAO(
            getSearchClient(),
            false,
            ELASTICSEARCH_IMPLEMENTATION_ELASTICSEARCH,
            getSearchConfiguration(),
            null,
            QueryFilterRewriteChain.EMPTY);
    // Empty aggregations
    final SearchResultMetadata searchResultMetadata =
        new SearchResultMetadata().setAggregations(new AggregationMetadataArray());
    SearchResult result =
        new SearchResult()
            .setEntities(new SearchEntityArray(new ArrayList<>()))
            .setMetadata(searchResultMetadata)
            .setFrom(0)
            .setPageSize(100)
            .setNumEntities(30);
    SearchResult expectedResult = null;
    try {
      expectedResult = result.copy();
    } catch (CloneNotSupportedException e) {
      fail(e.getMessage());
    }
    assertEquals(
        expectedResult,
        searchDAO.transformIndexIntoEntityName(
            getOperationContext().getSearchContext().getIndexConvention(), result));

    // one facet, do not transform
    Map<String, Long> aggMap = Map.of("urn:li:corpuser:datahub", Long.valueOf(3));

    List<AggregationMetadata> aggregationMetadataList = new ArrayList<>();
    aggregationMetadataList.add(
        new AggregationMetadata()
            .setName("owners")
            .setDisplayName("Owned by")
            .setAggregations(new LongMap(aggMap))
            .setFilterValues(
                new FilterValueArray(SearchUtil.convertToFilters(aggMap, Collections.emptySet()))));
    searchResultMetadata.setAggregations(new AggregationMetadataArray(aggregationMetadataList));
    result.setMetadata(searchResultMetadata);

    try {
      expectedResult = result.copy();
    } catch (CloneNotSupportedException e) {
      fail(e.getMessage());
    }
    assertEquals(
        searchDAO.transformIndexIntoEntityName(
            getOperationContext().getSearchContext().getIndexConvention(), result),
        expectedResult);

    // one facet, transform
    Map<String, Long> entityTypeMap = Map.of("smpldat_datasetindex_v2", Long.valueOf(3));

    aggregationMetadataList =
        List.of(
            new AggregationMetadata()
                .setName("_entityType")
                .setDisplayName("Type")
                .setAggregations(new LongMap(entityTypeMap))
                .setFilterValues(
                    new FilterValueArray(
                        SearchUtil.convertToFilters(entityTypeMap, Collections.emptySet()))));
    searchResultMetadata.setAggregations(new AggregationMetadataArray(aggregationMetadataList));
    result.setMetadata(searchResultMetadata);

    Map<String, Long> expectedEntityTypeMap = Map.of("dataset", Long.valueOf(3));

    List<AggregationMetadata> expectedAggregationMetadataList =
        List.of(
            new AggregationMetadata()
                .setName("_entityType")
                .setDisplayName("Type")
                .setAggregations(new LongMap(expectedEntityTypeMap))
                .setFilterValues(
                    new FilterValueArray(
                        SearchUtil.convertToFilters(
                            expectedEntityTypeMap, Collections.emptySet()))));
    expectedResult.setMetadata(
        new SearchResultMetadata()
            .setAggregations(new AggregationMetadataArray(expectedAggregationMetadataList)));
    assertEquals(
        searchDAO.transformIndexIntoEntityName(
            getOperationContext().getSearchContext().getIndexConvention(), result),
        expectedResult);
  }

  @Test
  public void testTransformIndexIntoEntityNameNested() {
    ESSearchDAO searchDAO =
        new ESSearchDAO(
            getSearchClient(),
            false,
            ELASTICSEARCH_IMPLEMENTATION_ELASTICSEARCH,
            getSearchConfiguration(),
            null,
            QueryFilterRewriteChain.EMPTY);
    // One nested facet
    Map<String, Long> entityTypeMap =
        Map.of(
            String.format(
                "smpldat_datasetindex_v2%surn:li:corpuser:datahub", AGGREGATION_SEPARATOR_CHAR),
            Long.valueOf(3),
            String.format(
                "smpldat_datasetindex_v2%surn:li:corpuser:bfoo", AGGREGATION_SEPARATOR_CHAR),
            Long.valueOf(7),
            "smpldat_datasetindex_v2",
            Long.valueOf(20));
    List<AggregationMetadata> aggregationMetadataList =
        List.of(
            new AggregationMetadata()
                .setName("_entityType␞owners")
                .setDisplayName("Type␞Owned By")
                .setAggregations(new LongMap(entityTypeMap))
                .setFilterValues(
                    new FilterValueArray(
                        SearchUtil.convertToFilters(entityTypeMap, Collections.emptySet()))));
    SearchResult result =
        new SearchResult()
            .setEntities(new SearchEntityArray(new ArrayList<>()))
            .setMetadata(
                new SearchResultMetadata()
                    .setAggregations(new AggregationMetadataArray(aggregationMetadataList)))
            .setFrom(0)
            .setPageSize(100)
            .setNumEntities(50);

    Map<String, Long> expectedEntityTypeMap =
        Map.of(
            String.format("dataset%surn:li:corpuser:datahub", AGGREGATION_SEPARATOR_CHAR),
            Long.valueOf(3),
            String.format("dataset%surn:li:corpuser:bfoo", AGGREGATION_SEPARATOR_CHAR),
            Long.valueOf(7),
            "dataset",
            Long.valueOf(20));

    List<AggregationMetadata> expectedAggregationMetadataList =
        List.of(
            new AggregationMetadata()
                .setName("_entityType␞owners")
                .setDisplayName("Type␞Owned By")
                .setAggregations(new LongMap(expectedEntityTypeMap))
                .setFilterValues(
                    new FilterValueArray(
                        SearchUtil.convertToFilters(
                            expectedEntityTypeMap, Collections.emptySet()))));
    SearchResult expectedResult =
        new SearchResult()
            .setEntities(new SearchEntityArray(new ArrayList<>()))
            .setMetadata(
                new SearchResultMetadata()
                    .setAggregations(new AggregationMetadataArray(expectedAggregationMetadataList)))
            .setFrom(0)
            .setPageSize(100)
            .setNumEntities(50);
    assertEquals(
        searchDAO.transformIndexIntoEntityName(
            getOperationContext().getSearchContext().getIndexConvention(), result),
        expectedResult);

    // One nested facet, opposite order
    entityTypeMap =
        Map.of(
            String.format(
                "urn:li:corpuser:datahub%ssmpldat_datasetindex_v2", AGGREGATION_SEPARATOR_CHAR),
            Long.valueOf(3),
            String.format(
                "urn:li:corpuser:datahub%ssmpldat_chartindex_v2", AGGREGATION_SEPARATOR_CHAR),
            Long.valueOf(7),
            "urn:li:corpuser:datahub",
            Long.valueOf(20));
    aggregationMetadataList =
        List.of(
            new AggregationMetadata()
                .setName("owners␞_entityType")
                .setDisplayName("Owned By␞Type")
                .setAggregations(new LongMap(entityTypeMap))
                .setFilterValues(
                    new FilterValueArray(
                        SearchUtil.convertToFilters(entityTypeMap, Collections.emptySet()))));
    result =
        new SearchResult()
            .setEntities(new SearchEntityArray(new ArrayList<>()))
            .setMetadata(
                new SearchResultMetadata()
                    .setAggregations(new AggregationMetadataArray(aggregationMetadataList)))
            .setFrom(0)
            .setPageSize(100)
            .setNumEntities(50);

    expectedEntityTypeMap =
        Map.of(
            String.format("urn:li:corpuser:datahub%sdataset", AGGREGATION_SEPARATOR_CHAR),
            Long.valueOf(3),
            String.format("urn:li:corpuser:datahub%schart", AGGREGATION_SEPARATOR_CHAR),
            Long.valueOf(7),
            "urn:li:corpuser:datahub",
            Long.valueOf(20));

    expectedAggregationMetadataList =
        List.of(
            new AggregationMetadata()
                .setName("owners␞_entityType")
                .setDisplayName("Owned By␞Type")
                .setAggregations(new LongMap(expectedEntityTypeMap))
                .setFilterValues(
                    new FilterValueArray(
                        SearchUtil.convertToFilters(
                            expectedEntityTypeMap, Collections.emptySet()))));
    expectedResult =
        new SearchResult()
            .setEntities(new SearchEntityArray(new ArrayList<>()))
            .setMetadata(
                new SearchResultMetadata()
                    .setAggregations(new AggregationMetadataArray(expectedAggregationMetadataList)))
            .setFrom(0)
            .setPageSize(100)
            .setNumEntities(50);
    assertEquals(
        searchDAO.transformIndexIntoEntityName(
            getOperationContext().getSearchContext().getIndexConvention(), result),
        expectedResult);
  }

  @Test
  public void testExplain() {
    ExplainResponse explainResponse =
        getESSearchDao()
            .explain(
                getOperationContext()
                    .withSearchFlags(flags -> ElasticSearchService.DEFAULT_SERVICE_SEARCH_FLAGS),
                "*",
                "urn:li:dataset:(urn:li:dataPlatform:bigquery,bigquery-public-data.covid19_geotab_mobility_impact."
                    + "ca_border_wait_times,PROD)",
                DATASET_ENTITY_NAME,
                null,
                null,
                null,
                null,
                10,
                null);

    assertNotNull(explainResponse);
    assertEquals(explainResponse.getIndex(), "smpldat_datasetindex_v2");
    assertEquals(
        explainResponse.getId(),
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,bigquery-public-data.covid19_geotab_mobility_impact.ca_border_wait_times,PROD)");
    assertTrue(explainResponse.isExists());
    assertEquals(explainResponse.getExplanation().getValue(), 1.25f);
  }

  /**
   * Ensure default search configuration matches the test fixture configuration (allowing for some
   * differences)
   */
  @Test
  public void testConfig() throws IOException {
    final CustomSearchConfiguration defaultConfig;
    try (InputStream stream = new ClassPathResource(DEFAULT_CONFIG).getInputStream()) {
      defaultConfig = MAPPER.readValue(stream, CustomSearchConfiguration.class);
    }

    final CustomSearchConfiguration fixtureConfig =
        MAPPER.readValue(
            MAPPER.writeValueAsBytes(getCustomSearchConfiguration()),
            CustomSearchConfiguration.class);

    // test specifics
    ((List<Map<String, Object>>)
            fixtureConfig.getQueryConfigurations().get(1).getFunctionScore().get("functions"))
        .remove(1);

    ((List<Map<String, Object>>)
            fixtureConfig.getQueryConfigurations().get(2).getFunctionScore().get("functions"))
        .remove(1);

    assertEquals(fixtureConfig, defaultConfig);
  }
}
