package com.linkedin.metadata.search.query;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.search.fixtures.SampleDataFixtureTestBase.DEFAULT_CONFIG;
import static com.linkedin.metadata.search.fixtures.SampleDataFixtureTestBase.MAPPER;
import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;
import static com.linkedin.metadata.utils.SearchUtil.AGGREGATION_SEPARATOR_CHAR;
import static com.linkedin.metadata.utils.SearchUtil.ES_INDEX_FIELD;
import static com.linkedin.metadata.utils.SearchUtil.INDEX_VIRTUAL_FIELD;
import static io.datahubproject.test.search.SearchTestUtils.TEST_ES_SEARCH_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.TEST_SEARCH_SERVICE_CONFIG;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.data.template.LongMap;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Triple;
import org.opensearch.action.explain.ExplainResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.index.query.BoolQueryBuilder;
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
        buildCriterion("tags.keyword", Condition.EQUAL, "urn:li:tag:abc", "urn:li:tag:def");

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
            TEST_ES_SEARCH_CONFIG,
            null,
            QueryFilterRewriteChain.EMPTY,
            TEST_SEARCH_SERVICE_CONFIG);
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
            TEST_ES_SEARCH_CONFIG,
            null,
            QueryFilterRewriteChain.EMPTY,
            TEST_SEARCH_SERVICE_CONFIG);
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
                List.of());

    assertNotNull(explainResponse);
    assertEquals(explainResponse.getIndex(), "smpldat_datasetindex_v2");
    assertEquals(
        explainResponse.getId(),
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,bigquery-public-data.covid19_geotab_mobility_impact.ca_border_wait_times,PROD)");
    assertTrue(explainResponse.isExists());
    assertEquals(explainResponse.getExplanation().getValue(), 1.25f);
  }

  @Test
  public void testFilterTransform() {
    ESSearchDAO esSearchDAO = getESSearchDao();
    Filter testFilter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(
                            new CriterionArray(
                                new Criterion()
                                    .setField(INDEX_VIRTUAL_FIELD)
                                    .setCondition(Condition.EQUAL)
                                    .setValues(new StringArray(List.of("DATASET")))))));

    Triple<SearchRequest, Filter, List<EntitySpec>> searchReq =
        esSearchDAO.buildSearchRequest(
            getOperationContext(),
            List.of("dataset"),
            "*",
            testFilter,
            List.of(new SortCriterion().setField("urn").setOrder(SortOrder.ASCENDING)),
            0,
            10,
            List.of());
    assertNotEquals(searchReq.getMiddle(), testFilter);
    assertFalse(
        ((BoolQueryBuilder) searchReq.getLeft().source().query())
            .filter()
            .toString()
            .contains(INDEX_VIRTUAL_FIELD));

    Triple<SearchRequest, Filter, List<EntitySpec>> scrollReq =
        esSearchDAO.buildScrollRequest(
            getOperationContext(),
            getOperationContext().getSearchContext().getIndexConvention(),
            null,
            null,
            List.of("dataset"),
            10,
            testFilter,
            "*",
            List.of(new SortCriterion().setField("urn").setOrder(SortOrder.ASCENDING)),
            List.of());
    assertNotEquals(scrollReq.getMiddle(), testFilter);
    assertFalse(
        ((BoolQueryBuilder) scrollReq.getLeft().source().query())
            .filter()
            .toString()
            .contains(INDEX_VIRTUAL_FIELD));
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

  @Test
  public void testBuildSearchRequestRemovesEntityTypeFilter() {
    // Create a filter with _entityType criterion
    Criterion entityTypeCriterion = buildCriterion("_entityType", Condition.EQUAL, "dataset");
    Criterion tagCriterion = buildCriterion("tags.keyword", Condition.EQUAL, "urn:li:tag:test");

    Filter originalFilter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(new CriterionArray(entityTypeCriterion, tagCriterion))));

    // Build search request
    Triple<SearchRequest, Filter, List<EntitySpec>> result =
        getESSearchDao()
            .buildSearchRequest(
                getOperationContext(),
                Arrays.asList("dataset"),
                "test query",
                originalFilter,
                Collections.emptyList(),
                0,
                10,
                Arrays.asList("tags"));

    // Verify the transformed filter (middle) has _entityType replaced with _index
    Filter transformedFilter = result.getMiddle();
    assertNotNull(transformedFilter);
    assertNotEquals(originalFilter, transformedFilter);

    // Check that _entityType is not present in transformed filter
    boolean hasEntityTypeField =
        transformedFilter.getOr().stream()
            .flatMap(cc -> cc.getAnd().stream())
            .anyMatch(criterion -> "_entityType".equals(criterion.getField()));
    assertFalse(hasEntityTypeField, "Transformed filter should not contain _entityType field");

    // Check that _index field is present instead
    boolean hasIndexField =
        transformedFilter.getOr().stream()
            .flatMap(cc -> cc.getAnd().stream())
            .anyMatch(criterion -> ES_INDEX_FIELD.equals(criterion.getField()));
    assertTrue(hasIndexField, "Transformed filter should contain _index field");

    // Verify the SearchRequest and EntitySpecs are not null
    assertNotNull(result.getLeft());
    assertNotNull(result.getRight());
    assertEquals(result.getRight().size(), 1);
  }

  @Test
  public void testBuildSearchRequestMultipleEntityTypes() {
    // Create a filter with multiple entity types
    Criterion entityTypeCriterion1 = buildCriterion("_entityType", Condition.EQUAL, "dataset");
    Criterion entityTypeCriterion2 = buildCriterion("_entityType", Condition.EQUAL, "chart");

    Filter originalFilter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion().setAnd(new CriterionArray(entityTypeCriterion1)),
                    new ConjunctiveCriterion().setAnd(new CriterionArray(entityTypeCriterion2))));

    Triple<SearchRequest, Filter, List<EntitySpec>> result =
        getESSearchDao()
            .buildSearchRequest(
                getOperationContext(),
                Arrays.asList("dataset", "chart"),
                "*",
                originalFilter,
                Collections.emptyList(),
                0,
                20,
                Collections.emptyList());

    // Verify all _entityType criteria are transformed
    Filter transformedFilter = result.getMiddle();
    boolean hasEntityTypeField =
        transformedFilter.getOr().stream()
            .flatMap(cc -> cc.getAnd().stream())
            .anyMatch(criterion -> "_entityType".equals(criterion.getField()));
    assertFalse(hasEntityTypeField, "No _entityType fields should remain after transformation");

    // Verify entity specs match requested entities
    assertEquals(result.getRight().size(), 2);
  }

  @Test
  public void testBuildSearchRequestWithSortCriteria() {
    SortCriterion sortByName =
        new SortCriterion().setField("name.keyword").setOrder(SortOrder.ASCENDING);

    SortCriterion sortByLastModified =
        new SortCriterion().setField("lastModified").setOrder(SortOrder.DESCENDING);

    List<SortCriterion> sortCriteria = Arrays.asList(sortByName, sortByLastModified);

    Triple<SearchRequest, Filter, List<EntitySpec>> result =
        getESSearchDao()
            .buildSearchRequest(
                getOperationContext(),
                Arrays.asList("dataset"),
                "search term",
                null,
                sortCriteria,
                10,
                50,
                Arrays.asList("owners", "tags"));

    assertNotNull(result.getLeft());
    assertNotNull(result.getLeft().source());
    // Verify sort criteria are applied to the search request
    assertNotNull(result.getLeft().source().sorts());
    assertEquals(
        result.getLeft().source().sorts().size(),
        sortCriteria.size() + 1); // +1 for default _score sort
  }

  @Test
  public void testBuildScrollRequestRemovesEntityTypeFilter() {
    Criterion entityTypeCriterion = buildCriterion("_entityType", Condition.EQUAL, "dataset");
    Criterion ownerCriterion = buildCriterion("owners", Condition.EQUAL, "urn:li:corpuser:test");

    Filter originalFilter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(new CriterionArray(entityTypeCriterion, ownerCriterion))));

    Triple<SearchRequest, Filter, List<EntitySpec>> result =
        getESSearchDao()
            .buildScrollRequest(
                getOperationContext(),
                getOperationContext().getSearchContext().getIndexConvention(),
                null, // no scrollId
                "5m",
                Arrays.asList("dataset"),
                25,
                originalFilter,
                "test",
                Collections.emptyList(),
                Arrays.asList("owners"));

    // Verify filter transformation
    Filter transformedFilter = result.getMiddle();
    assertNotNull(transformedFilter);

    boolean hasEntityTypeField =
        transformedFilter.getOr().stream()
            .flatMap(cc -> cc.getAnd().stream())
            .anyMatch(criterion -> "_entityType".equals(criterion.getField()));
    assertFalse(hasEntityTypeField, "Transformed filter should not contain _entityType");

    boolean hasOwnerField =
        transformedFilter.getOr().stream()
            .flatMap(cc -> cc.getAnd().stream())
            .anyMatch(criterion -> "owners".equals(criterion.getField()));
    assertTrue(hasOwnerField, "Other criteria should be preserved");
  }

  @Test
  public void testBuildScrollRequestComplexFilter() {
    // Complex filter with nested conditions and entity type
    Criterion entityTypeCriterion =
        buildCriterion("_entityType", Condition.EQUAL, "dataset", "chart");
    Criterion platformCriterion =
        buildCriterion("platform", Condition.EQUAL, "urn:li:dataPlatform:hive");
    Criterion tagCriterion = buildCriterion("tags", Condition.CONTAIN, "urn:li:tag:pii");

    ConjunctiveCriterion cc1 =
        new ConjunctiveCriterion()
            .setAnd(new CriterionArray(entityTypeCriterion, platformCriterion));

    ConjunctiveCriterion cc2 = new ConjunctiveCriterion().setAnd(new CriterionArray(tagCriterion));

    Filter complexFilter = new Filter().setOr(new ConjunctiveCriterionArray(cc1, cc2));

    Triple<SearchRequest, Filter, List<EntitySpec>> result =
        getESSearchDao()
            .buildScrollRequest(
                getOperationContext(),
                getOperationContext().getSearchContext().getIndexConvention(),
                null,
                "10m",
                Arrays.asList("dataset", "chart"),
                50,
                complexFilter,
                "*",
                Arrays.asList(new SortCriterion().setField("urn").setOrder(SortOrder.ASCENDING)),
                Arrays.asList("platform", "tags"));

    // Verify complex filter transformation
    Filter transformedFilter = result.getMiddle();
    assertNotNull(transformedFilter);
    assertEquals(transformedFilter.getOr().size(), 2);

    // First conjunctive criterion should have _entityType transformed to _index
    ConjunctiveCriterion transformedCc1 = transformedFilter.getOr().get(0);
    boolean hasIndexInFirstCC =
        transformedCc1.getAnd().stream()
            .anyMatch(criterion -> ES_INDEX_FIELD.equals(criterion.getField()));
    assertTrue(hasIndexInFirstCC, "First CC should have _index field");

    boolean hasPlatformInFirstCC =
        transformedCc1.getAnd().stream()
            .anyMatch(criterion -> "platform".equals(criterion.getField()));
    assertTrue(hasPlatformInFirstCC, "Platform criterion should be preserved");

    // Second conjunctive criterion should remain unchanged
    ConjunctiveCriterion transformedCc2 = transformedFilter.getOr().get(1);
    assertEquals(transformedCc2.getAnd().size(), 1);
    assertEquals(transformedCc2.getAnd().get(0).getField(), "tags");
  }

  @Test
  public void testBuildSearchRequestNullFilter() {
    Triple<SearchRequest, Filter, List<EntitySpec>> result =
        getESSearchDao()
            .buildSearchRequest(
                getOperationContext(),
                Arrays.asList("dataset"),
                "test",
                null,
                Collections.emptyList(),
                0,
                10,
                Collections.emptyList());

    // When filter is null, transformed filter should also be null
    assertEquals(result.getMiddle(), null);
    assertNotNull(result.getLeft());
    assertNotNull(result.getRight());
  }

  @Test
  public void testBuildSearchRequestEmptyInput() {
    // Test with empty input string - should be converted to "*"
    Triple<SearchRequest, Filter, List<EntitySpec>> result =
        getESSearchDao()
            .buildSearchRequest(
                getOperationContext(),
                Arrays.asList("glossaryterm"),
                "", // empty input
                null,
                Collections.emptyList(),
                0,
                10,
                Arrays.asList("glossaryTerms"));

    assertNotNull(result.getLeft());
    // Verify that the query was built (empty string should be converted to "*")
    assertNotNull(result.getLeft().source());
    assertNotNull(result.getLeft().source().query());
  }
}
