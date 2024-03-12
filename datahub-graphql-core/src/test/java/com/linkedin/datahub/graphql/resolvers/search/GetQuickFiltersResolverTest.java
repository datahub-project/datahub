package com.linkedin.datahub.graphql.resolvers.search;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.datahub.graphql.resolvers.search.SearchUtils.SEARCHABLE_ENTITY_TYPES;

import com.datahub.authentication.Authentication;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.GetQuickFiltersInput;
import com.linkedin.datahub.graphql.generated.GetQuickFiltersResult;
import com.linkedin.datahub.graphql.generated.QuickFilter;
import com.linkedin.datahub.graphql.resolvers.EntityTypeMapper;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.AggregationMetadata;
import com.linkedin.metadata.search.AggregationMetadataArray;
import com.linkedin.metadata.search.FilterValue;
import com.linkedin.metadata.search.FilterValueArray;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.metadata.service.ViewService;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class GetQuickFiltersResolverTest {

  @Test
  public static void testGetQuickFiltersHappyPathSuccess() throws Exception {
    SearchResultMetadata mockData = getHappyPathTestData();
    ViewService mockService = Mockito.mock(ViewService.class);
    EntityClient mockClient =
        initMockEntityClient(
            SEARCHABLE_ENTITY_TYPES.stream()
                .map(EntityTypeMapper::getName)
                .collect(Collectors.toList()),
            "*",
            null,
            0,
            0,
            new SearchResult()
                .setEntities(new SearchEntityArray())
                .setNumEntities(0)
                .setFrom(0)
                .setPageSize(0)
                .setMetadata(mockData));

    final GetQuickFiltersResolver resolver = new GetQuickFiltersResolver(mockClient, mockService);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(new GetQuickFiltersInput());
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    GetQuickFiltersResult result = resolver.get(mockEnv).get();

    Assert.assertEquals(result.getQuickFilters().size(), 10);
    compareResultToExpectedData(result, getHappyPathResultData());
  }

  @Test
  public static void testGetQuickFiltersUnhappyPathSuccess() throws Exception {
    SearchResultMetadata mockData = getUnHappyPathTestData();
    ViewService mockService = Mockito.mock(ViewService.class);
    EntityClient mockClient =
        initMockEntityClient(
            SEARCHABLE_ENTITY_TYPES.stream()
                .map(EntityTypeMapper::getName)
                .collect(Collectors.toList()),
            "*",
            null,
            0,
            0,
            new SearchResult()
                .setEntities(new SearchEntityArray())
                .setNumEntities(0)
                .setFrom(0)
                .setPageSize(0)
                .setMetadata(mockData));

    final GetQuickFiltersResolver resolver = new GetQuickFiltersResolver(mockClient, mockService);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(new GetQuickFiltersInput());
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    GetQuickFiltersResult result = resolver.get(mockEnv).get();

    Assert.assertEquals(result.getQuickFilters().size(), 8);
    compareResultToExpectedData(result, getUnHappyPathResultData());
  }

  @Test
  public static void testGetQuickFiltersFailure() throws Exception {
    ViewService mockService = Mockito.mock(ViewService.class);
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.when(
            mockClient.searchAcrossEntities(
                Mockito.anyList(),
                Mockito.anyString(),
                Mockito.any(),
                Mockito.anyInt(),
                Mockito.anyInt(),
                Mockito.eq(null),
                Mockito.eq(null),
                Mockito.any(Authentication.class)))
        .thenThrow(new RemoteInvocationException());

    final GetQuickFiltersResolver resolver = new GetQuickFiltersResolver(mockClient, mockService);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(new GetQuickFiltersInput());
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Assert.assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  private static void compareResultToExpectedData(
      GetQuickFiltersResult result, GetQuickFiltersResult expected) {
    IntStream.range(0, result.getQuickFilters().size())
        .forEach(
            index -> {
              QuickFilter resultFilter = result.getQuickFilters().get(index);
              QuickFilter expectedFilter = expected.getQuickFilters().get(index);
              Assert.assertEquals(resultFilter.getField(), expectedFilter.getField());
              Assert.assertEquals(resultFilter.getValue(), expectedFilter.getValue());
              if (resultFilter.getEntity() != null) {
                Assert.assertEquals(
                    resultFilter.getEntity().getUrn(), expectedFilter.getEntity().getUrn());
              }
            });
  }

  private static SearchResultMetadata getHappyPathTestData() {
    FilterValueArray platformFilterValues = new FilterValueArray();
    platformFilterValues.add(
        createFilterValue("urn:li:dataPlatform:snowflake", 100, "urn:li:dataPlatform:snowflake"));
    platformFilterValues.add(
        createFilterValue("urn:li:dataPlatform:looker", 99, "urn:li:dataPlatform:looker"));
    platformFilterValues.add(
        createFilterValue("urn:li:dataPlatform:dbt", 98, "urn:li:dataPlatform:dbt"));
    platformFilterValues.add(
        createFilterValue("urn:li:dataPlatform:bigquery", 97, "urn:li:dataPlatform:bigquery"));
    platformFilterValues.add(
        createFilterValue("urn:li:dataPlatform:test", 1, "urn:li:dataPlatform:test"));
    platformFilterValues.add(
        createFilterValue("urn:li:dataPlatform:custom", 96, "urn:li:dataPlatform:custom"));

    FilterValueArray entityTypeFilters = new FilterValueArray();
    entityTypeFilters.add(createFilterValue("dataset", 100, null));
    entityTypeFilters.add(createFilterValue("dashboard", 100, null));
    entityTypeFilters.add(createFilterValue("dataflow", 100, null));
    entityTypeFilters.add(createFilterValue("datajob", 200, null));
    entityTypeFilters.add(createFilterValue("chart", 200, null));
    entityTypeFilters.add(createFilterValue("domain", 200, null));
    entityTypeFilters.add(createFilterValue("corpuser", 200, null));
    entityTypeFilters.add(createFilterValue("glossaryterm", 200, null));

    AggregationMetadataArray aggregationMetadataArray = new AggregationMetadataArray();
    aggregationMetadataArray.add(createAggregationMetadata("platform", platformFilterValues));
    aggregationMetadataArray.add(createAggregationMetadata("_entityType", entityTypeFilters));

    SearchResultMetadata resultMetadata = new SearchResultMetadata();
    resultMetadata.setAggregations(aggregationMetadataArray);
    return resultMetadata;
  }

  private static GetQuickFiltersResult getHappyPathResultData() {
    GetQuickFiltersResult result = new GetQuickFiltersResult();
    List<QuickFilter> quickFilters = new ArrayList<>();
    // platforms should be in alphabetical order
    quickFilters.add(
        createQuickFilter(
            "platform", "urn:li:dataPlatform:bigquery", "urn:li:dataPlatform:bigquery"));
    quickFilters.add(
        createQuickFilter("platform", "urn:li:dataPlatform:custom", "urn:li:dataPlatform:custom"));
    quickFilters.add(
        createQuickFilter("platform", "urn:li:dataPlatform:dbt", "urn:li:dataPlatform:dbt"));
    quickFilters.add(
        createQuickFilter("platform", "urn:li:dataPlatform:looker", "urn:li:dataPlatform:looker"));
    quickFilters.add(
        createQuickFilter(
            "platform", "urn:li:dataPlatform:snowflake", "urn:li:dataPlatform:snowflake"));
    quickFilters.add(createQuickFilter("_entityType", "DATASET", null));
    quickFilters.add(createQuickFilter("_entityType", "DASHBOARD", null));
    quickFilters.add(createQuickFilter("_entityType", "DATA_FLOW", null));
    quickFilters.add(createQuickFilter("_entityType", "DOMAIN", null));
    quickFilters.add(createQuickFilter("_entityType", "GLOSSARY_TERM", null));
    result.setQuickFilters(quickFilters);

    return result;
  }

  private static SearchResultMetadata getUnHappyPathTestData() {
    FilterValueArray platformFilterValues = new FilterValueArray();
    // only 3 platforms available
    platformFilterValues.add(
        createFilterValue("urn:li:dataPlatform:snowflake", 98, "urn:li:dataPlatform:snowflake"));
    platformFilterValues.add(
        createFilterValue("urn:li:dataPlatform:looker", 100, "urn:li:dataPlatform:looker"));
    platformFilterValues.add(
        createFilterValue("urn:li:dataPlatform:dbt", 99, "urn:li:dataPlatform:dbt"));

    FilterValueArray entityTypeFilters = new FilterValueArray();
    // no dashboard, data flows, or glossary terms
    entityTypeFilters.add(createFilterValue("datajob", 200, null));
    entityTypeFilters.add(createFilterValue("chart", 200, null));
    entityTypeFilters.add(createFilterValue("domain", 200, null));
    entityTypeFilters.add(createFilterValue("corpuser", 200, null));
    entityTypeFilters.add(createFilterValue("dataset", 100, null));

    AggregationMetadataArray aggregationMetadataArray = new AggregationMetadataArray();
    aggregationMetadataArray.add(createAggregationMetadata("platform", platformFilterValues));
    aggregationMetadataArray.add(createAggregationMetadata("_entityType", entityTypeFilters));

    SearchResultMetadata resultMetadata = new SearchResultMetadata();
    resultMetadata.setAggregations(aggregationMetadataArray);
    return resultMetadata;
  }

  private static GetQuickFiltersResult getUnHappyPathResultData() {
    GetQuickFiltersResult result = new GetQuickFiltersResult();
    List<QuickFilter> quickFilters = new ArrayList<>();
    // in correct order by count for platforms (alphabetical). In correct order by priority for
    // entity types
    quickFilters.add(
        createQuickFilter("platform", "urn:li:dataPlatform:dbt", "urn:li:dataPlatform:dbt"));
    quickFilters.add(
        createQuickFilter("platform", "urn:li:dataPlatform:looker", "urn:li:dataPlatform:looker"));
    quickFilters.add(
        createQuickFilter(
            "platform", "urn:li:dataPlatform:snowflake", "urn:li:dataPlatform:snowflake"));
    quickFilters.add(createQuickFilter("_entityType", "DATASET", null));
    quickFilters.add(createQuickFilter("_entityType", "DATA_JOB", null));
    quickFilters.add(createQuickFilter("_entityType", "CHART", null));
    quickFilters.add(createQuickFilter("_entityType", "DOMAIN", null));
    quickFilters.add(createQuickFilter("_entityType", "CORP_USER", null));
    result.setQuickFilters(quickFilters);

    return result;
  }

  private static QuickFilter createQuickFilter(
      @Nonnull final String field, @Nonnull final String value, @Nullable final String entityUrn) {
    QuickFilter quickFilter = new QuickFilter();
    quickFilter.setField(field);
    quickFilter.setValue(value);
    if (entityUrn != null) {
      quickFilter.setEntity(UrnToEntityMapper.map(UrnUtils.getUrn(entityUrn)));
    }
    return quickFilter;
  }

  private static FilterValue createFilterValue(
      @Nonnull final String value, final int count, @Nullable final String entity) {
    FilterValue filterValue = new FilterValue();
    filterValue.setValue(value);
    filterValue.setFacetCount(count);
    if (entity != null) {
      filterValue.setEntity(UrnUtils.getUrn(entity));
    }
    return filterValue;
  }

  private static AggregationMetadata createAggregationMetadata(
      @Nonnull final String name, @Nonnull final FilterValueArray filterValues) {
    AggregationMetadata aggregationMetadata = new AggregationMetadata();
    aggregationMetadata.setName(name);
    aggregationMetadata.setFilterValues(filterValues);
    return aggregationMetadata;
  }

  private static EntityClient initMockEntityClient(
      List<String> entityTypes,
      String query,
      Filter filter,
      int start,
      int limit,
      SearchResult result)
      throws Exception {
    EntityClient client = Mockito.mock(EntityClient.class);
    Mockito.when(
            client.searchAcrossEntities(
                Mockito.eq(entityTypes),
                Mockito.eq(query),
                Mockito.eq(filter),
                Mockito.eq(start),
                Mockito.eq(limit),
                Mockito.eq(null),
                Mockito.eq(null),
                Mockito.any(Authentication.class)))
        .thenReturn(result);
    return client;
  }

  private GetQuickFiltersResolverTest() {}
}
