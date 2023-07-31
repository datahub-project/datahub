package com.linkedin.metadata.search.elasticsearch.query;

import com.linkedin.metadata.config.search.SearchConfiguration;
import com.datahub.test.Snapshot;
import com.google.common.collect.ImmutableList;
import com.linkedin.data.template.LongMap;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.ESSampleDataFixture;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.AggregationMetadata;
import com.linkedin.metadata.search.AggregationMetadataArray;
import com.linkedin.metadata.search.FilterValueArray;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.metadata.utils.SearchUtil;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import com.linkedin.metadata.query.filter.Criterion;
import org.springframework.beans.factory.annotation.Qualifier;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.utils.SearchUtil.*;
import static org.testng.Assert.*;


@Import(ESSampleDataFixture.class)
public class ESSearchDAOTest extends AbstractTestNGSpringContextTests {
  @Autowired
  private RestHighLevelClient _searchClient;
  @Autowired
  private SearchConfiguration _searchConfiguration;
  @Autowired
  @Qualifier("sampleDataIndexConvention")
  IndexConvention _indexConvention;
  EntityRegistry _entityRegistry = new SnapshotEntityRegistry(new Snapshot());



  @Test
  public void testTransformFilterForEntitiesNoChange() {
    Criterion c = new Criterion().setValue("urn:li:tag:abc").setValues(
        new StringArray(ImmutableList.of("urn:li:tag:abc", "urn:li:tag:def"))
    ).setNegated(false).setCondition(Condition.EQUAL).setField("tags.keyword");

    Filter f = new Filter().setOr(
        new ConjunctiveCriterionArray(new ConjunctiveCriterion().setAnd(new CriterionArray(c))));

    Filter transformedFilter = SearchUtil.transformFilterForEntities(f, _indexConvention);
    assertEquals(f, transformedFilter);
  }

  @Test
  public void testTransformFilterForEntitiesNullFilter() {
    Filter transformedFilter = SearchUtil.transformFilterForEntities(null, _indexConvention);
    assertNotNull(_indexConvention);
    assertEquals(null, transformedFilter);
  }

  @Test
  public void testTransformFilterForEntitiesWithChanges() {

    Criterion c = new Criterion().setValue("dataset").setValues(
        new StringArray(ImmutableList.of("dataset"))
    ).setNegated(false).setCondition(Condition.EQUAL).setField("_entityType");

    Filter f = new Filter().setOr(
        new ConjunctiveCriterionArray(new ConjunctiveCriterion().setAnd(new CriterionArray(c))));
    Filter originalF = null;
    try {
      originalF = f.copy();
    } catch (CloneNotSupportedException e) {
      fail(e.getMessage());
    }
    assertEquals(f, originalF);

    Filter transformedFilter = SearchUtil.transformFilterForEntities(f, _indexConvention);
    assertNotEquals(originalF, transformedFilter);

    Criterion expectedNewCriterion = new Criterion().setValue("smpldat_datasetindex_v2").setValues(
        new StringArray(ImmutableList.of("smpldat_datasetindex_v2"))
    ).setNegated(false).setCondition(Condition.EQUAL).setField("_index");

    Filter expectedNewFilter = new Filter().setOr(
        new ConjunctiveCriterionArray(new ConjunctiveCriterion().setAnd(new CriterionArray(expectedNewCriterion))));

    assertEquals(expectedNewFilter, transformedFilter);
  }

  @Test
  public void testTransformFilterForEntitiesWithUnderscore() {

    Criterion c = new Criterion().setValue("data_job").setValues(
        new StringArray(ImmutableList.of("data_job"))
    ).setNegated(false).setCondition(Condition.EQUAL).setField("_entityType");

    Filter f = new Filter().setOr(
        new ConjunctiveCriterionArray(new ConjunctiveCriterion().setAnd(new CriterionArray(c))));
    Filter originalF = null;
    try {
      originalF = f.copy();
    } catch (CloneNotSupportedException e) {
      fail(e.getMessage());
    }
    assertEquals(f, originalF);

    Filter transformedFilter = SearchUtil.transformFilterForEntities(f, _indexConvention);
    assertNotEquals(originalF, transformedFilter);

    Criterion expectedNewCriterion = new Criterion().setValue("smpldat_datajobindex_v2").setValues(
        new StringArray(ImmutableList.of("smpldat_datajobindex_v2"))
    ).setNegated(false).setCondition(Condition.EQUAL).setField("_index");

    Filter expectedNewFilter = new Filter().setOr(
        new ConjunctiveCriterionArray(new ConjunctiveCriterion().setAnd(new CriterionArray(expectedNewCriterion))));

    assertEquals(transformedFilter, expectedNewFilter);
  }

  @Test
  public void testTransformFilterForEntitiesWithSomeChanges() {

    Criterion criterionChanged = new Criterion().setValue("dataset").setValues(
        new StringArray(ImmutableList.of("dataset"))
    ).setNegated(false).setCondition(Condition.EQUAL).setField("_entityType");
    Criterion criterionUnchanged = new Criterion().setValue("urn:li:tag:abc").setValues(
        new StringArray(ImmutableList.of("urn:li:tag:abc", "urn:li:tag:def"))
    ).setNegated(false).setCondition(Condition.EQUAL).setField("tags.keyword");

    Filter f = new Filter().setOr(
        new ConjunctiveCriterionArray(new ConjunctiveCriterion().setAnd(new CriterionArray(criterionChanged, criterionUnchanged))));
    Filter originalF = null;
    try {
      originalF = f.copy();
    } catch (CloneNotSupportedException e) {
      fail(e.getMessage());
    }
    assertEquals(f, originalF);

    Filter transformedFilter = SearchUtil.transformFilterForEntities(f, _indexConvention);
    assertNotEquals(originalF, transformedFilter);

    Criterion expectedNewCriterion = new Criterion().setValue("smpldat_datasetindex_v2").setValues(
        new StringArray(ImmutableList.of("smpldat_datasetindex_v2"))
    ).setNegated(false).setCondition(Condition.EQUAL).setField("_index");

    Filter expectedNewFilter = new Filter().setOr(
        new ConjunctiveCriterionArray(new ConjunctiveCriterion().setAnd(new CriterionArray(expectedNewCriterion, criterionUnchanged))));

    assertEquals(expectedNewFilter, transformedFilter);
  }

  @Test
  public void testTransformIndexIntoEntityNameSingle() {
    ESSearchDAO searchDAO = new ESSearchDAO(_entityRegistry, _searchClient, _indexConvention, false,
        ELASTICSEARCH_IMPLEMENTATION_ELASTICSEARCH, _searchConfiguration, null);
    // Empty aggregations
    final SearchResultMetadata searchResultMetadata =
        new SearchResultMetadata().setAggregations(new AggregationMetadataArray());
    SearchResult result = new SearchResult().setEntities(new SearchEntityArray(new ArrayList<>()))
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
    assertEquals(expectedResult, searchDAO.transformIndexIntoEntityName(result));

    // one facet, do not transform
    Map<String, Long> aggMap = Map.of("urn:li:corpuser:datahub", Long.valueOf(3));

    List<AggregationMetadata> aggregationMetadataList = new ArrayList<>();
    aggregationMetadataList.add(new AggregationMetadata().setName("owners")
        .setDisplayName("Owned by")
        .setAggregations(new LongMap(aggMap))
        .setFilterValues(new FilterValueArray(SearchUtil.convertToFilters(aggMap, Collections.emptySet())))
    );
    searchResultMetadata.setAggregations(new AggregationMetadataArray(aggregationMetadataList));
    result.setMetadata(searchResultMetadata);

    try {
      expectedResult = result.copy();
    } catch (CloneNotSupportedException e) {
      fail(e.getMessage());
    }
    assertEquals(searchDAO.transformIndexIntoEntityName(result), expectedResult);

    // one facet, transform
    Map<String, Long> entityTypeMap = Map.of("smpldat_datasetindex_v2", Long.valueOf(3));

    aggregationMetadataList = List.of(new AggregationMetadata().setName("_entityType")
        .setDisplayName("Type")
        .setAggregations(new LongMap(entityTypeMap))
        .setFilterValues(new FilterValueArray(SearchUtil.convertToFilters(entityTypeMap, Collections.emptySet())))
    );
    searchResultMetadata.setAggregations(new AggregationMetadataArray(aggregationMetadataList));
    result.setMetadata(searchResultMetadata);

    Map<String, Long> expectedEntityTypeMap = Map.of("dataset", Long.valueOf(3));

    List<AggregationMetadata> expectedAggregationMetadataList = List.of(
        new AggregationMetadata().setName("_entityType")
        .setDisplayName("Type")
        .setAggregations(new LongMap(expectedEntityTypeMap))
        .setFilterValues(new FilterValueArray(SearchUtil.convertToFilters(expectedEntityTypeMap, Collections.emptySet())))
    );
    expectedResult.setMetadata(new SearchResultMetadata().setAggregations(new AggregationMetadataArray(expectedAggregationMetadataList)));
    assertEquals(searchDAO.transformIndexIntoEntityName(result), expectedResult);
  }

  @Test
  public void testTransformIndexIntoEntityNameNested() {
    ESSearchDAO searchDAO = new ESSearchDAO(_entityRegistry, _searchClient, _indexConvention, false,
        ELASTICSEARCH_IMPLEMENTATION_ELASTICSEARCH, _searchConfiguration, null);
    // One nested facet
    Map<String, Long> entityTypeMap = Map.of(
        String.format("smpldat_datasetindex_v2%surn:li:corpuser:datahub", AGGREGATION_SEPARATOR_CHAR), Long.valueOf(3),
        String.format("smpldat_datasetindex_v2%surn:li:corpuser:bfoo", AGGREGATION_SEPARATOR_CHAR), Long.valueOf(7),
        "smpldat_datasetindex_v2", Long.valueOf(20)
    );
    List<AggregationMetadata> aggregationMetadataList = List.of(new AggregationMetadata().setName("_entityType␞owners")
        .setDisplayName("Type␞Owned By")
        .setAggregations(new LongMap(entityTypeMap))
        .setFilterValues(new FilterValueArray(SearchUtil.convertToFilters(entityTypeMap, Collections.emptySet())))
    );
    SearchResult result = new SearchResult().setEntities(new SearchEntityArray(new ArrayList<>()))
        .setMetadata(new SearchResultMetadata().setAggregations(
            new AggregationMetadataArray(aggregationMetadataList)
        ))
        .setFrom(0)
        .setPageSize(100)
        .setNumEntities(50);

    Map<String, Long> expectedEntityTypeMap = Map.of(
        String.format("dataset%surn:li:corpuser:datahub", AGGREGATION_SEPARATOR_CHAR), Long.valueOf(3),
        String.format("dataset%surn:li:corpuser:bfoo", AGGREGATION_SEPARATOR_CHAR), Long.valueOf(7),
        "dataset", Long.valueOf(20)
    );

    List<AggregationMetadata> expectedAggregationMetadataList = List.of(new AggregationMetadata().setName("_entityType␞owners")
        .setDisplayName("Type␞Owned By")
        .setAggregations(new LongMap(expectedEntityTypeMap))
        .setFilterValues(new FilterValueArray(SearchUtil.convertToFilters(expectedEntityTypeMap, Collections.emptySet())))
    );
    SearchResult expectedResult = new SearchResult().setEntities(new SearchEntityArray(new ArrayList<>()))
        .setMetadata(new SearchResultMetadata().setAggregations(
            new AggregationMetadataArray(expectedAggregationMetadataList)))
        .setFrom(0)
        .setPageSize(100)
        .setNumEntities(50);
    assertEquals(searchDAO.transformIndexIntoEntityName(result), expectedResult);

    // One nested facet, opposite order
    entityTypeMap = Map.of(
        String.format("urn:li:corpuser:datahub%ssmpldat_datasetindex_v2", AGGREGATION_SEPARATOR_CHAR), Long.valueOf(3),
        String.format("urn:li:corpuser:datahub%ssmpldat_chartindex_v2", AGGREGATION_SEPARATOR_CHAR), Long.valueOf(7),
        "urn:li:corpuser:datahub", Long.valueOf(20)
    );
    aggregationMetadataList = List.of(new AggregationMetadata().setName("owners␞_entityType")
        .setDisplayName("Owned By␞Type")
        .setAggregations(new LongMap(entityTypeMap))
        .setFilterValues(new FilterValueArray(SearchUtil.convertToFilters(entityTypeMap, Collections.emptySet())))
    );
    result = new SearchResult().setEntities(new SearchEntityArray(new ArrayList<>()))
        .setMetadata(new SearchResultMetadata().setAggregations(
            new AggregationMetadataArray(aggregationMetadataList)
        ))
        .setFrom(0)
        .setPageSize(100)
        .setNumEntities(50);

    expectedEntityTypeMap = Map.of(
        String.format("urn:li:corpuser:datahub%sdataset", AGGREGATION_SEPARATOR_CHAR), Long.valueOf(3),
        String.format("urn:li:corpuser:datahub%schart", AGGREGATION_SEPARATOR_CHAR), Long.valueOf(7),
        "urn:li:corpuser:datahub", Long.valueOf(20)
    );

    expectedAggregationMetadataList = List.of(new AggregationMetadata().setName("owners␞_entityType")
        .setDisplayName("Owned By␞Type")
        .setAggregations(new LongMap(expectedEntityTypeMap))
        .setFilterValues(new FilterValueArray(SearchUtil.convertToFilters(expectedEntityTypeMap, Collections.emptySet())))
    );
    expectedResult = new SearchResult().setEntities(new SearchEntityArray(new ArrayList<>()))
        .setMetadata(new SearchResultMetadata().setAggregations(
            new AggregationMetadataArray(expectedAggregationMetadataList)))
        .setFrom(0)
        .setPageSize(100)
        .setNumEntities(50);
    assertEquals(searchDAO.transformIndexIntoEntityName(result), expectedResult);
  }


}
