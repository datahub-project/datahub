package com.linkedin.datahub.graphql.resolvers.assertion;

import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionStdAggregation;
import com.linkedin.assertion.AssertionStdOperator;
import com.linkedin.assertion.AssertionStdParameter;
import com.linkedin.assertion.AssertionStdParameterType;
import com.linkedin.assertion.AssertionStdParameters;
import com.linkedin.assertion.AssertionType;
import com.linkedin.assertion.DatasetAssertionInfo;
import com.linkedin.assertion.DatasetAssertionScope;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AssertionsQueryFilterInput;
import com.linkedin.datahub.graphql.generated.AssertionsTimeRangeFilter;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.FilterOperator;
import com.linkedin.datahub.graphql.generated.ListAssertionsResult;
import com.linkedin.datahub.graphql.generated.LogicalOperator;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.key.AssertionKey;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class ListAssertionResolverTest {

  @Test
  public void testGetSuccess() throws Exception {
    // Mock clients
    EntityClient mockEntityClient = Mockito.mock(EntityClient.class);
    GraphClient mockGraphClient = Mockito.mock(GraphClient.class);

    // Create test URNs
    Urn datasetUrn = Urn.createFromString("urn:li:dataset:(test,test,test)");
    Urn assertionUrn1 = Urn.createFromString("urn:li:assertion:test-guid-1");
    Urn assertionUrn2 = Urn.createFromString("urn:li:assertion:test-guid-2");
    List<Urn> assertionUrns = ImmutableList.of(assertionUrn1, assertionUrn2);

    // Setup search result
    SearchResult mockSearchResult = new SearchResult();
    mockSearchResult.setFrom(0);
    mockSearchResult.setPageSize(10);
    mockSearchResult.setNumEntities(2);
    mockSearchResult.setEntities(new SearchEntityArray());
    SearchEntity searchEntity1 = new SearchEntity();
    searchEntity1.setEntity(assertionUrn1);
    SearchEntity searchEntity2 = new SearchEntity();
    searchEntity2.setEntity(assertionUrn2);
    mockSearchResult.getEntities().add(searchEntity1);
    mockSearchResult.getEntities().add(searchEntity2);

    // Mock the search call
    Mockito.when(
            mockEntityClient.search(
                any(OperationContext.class),
                Mockito.eq(Constants.ASSERTION_ENTITY_NAME),
                Mockito.anyString(),
                Mockito.any(),
                Mockito.any(),
                Mockito.anyInt(),
                Mockito.anyInt()))
        .thenReturn(mockSearchResult);

    // Create assertion aspects for both assertions
    Map<Urn, EntityResponse> mockEntityResponses = new HashMap<>();

    // First assertion
    Map<String, com.linkedin.entity.EnvelopedAspect> assertion1Aspects = new HashMap<>();
    assertion1Aspects.put(
        Constants.ASSERTION_KEY_ASPECT_NAME,
        new com.linkedin.entity.EnvelopedAspect()
            .setValue(new Aspect(new AssertionKey().setAssertionId("test-guid-1").data())));
    assertion1Aspects.put(
        Constants.ASSERTION_INFO_ASPECT_NAME,
        new com.linkedin.entity.EnvelopedAspect()
            .setValue(
                new Aspect(
                    new AssertionInfo()
                        .setType(AssertionType.DATASET)
                        .setDatasetAssertion(
                            new DatasetAssertionInfo()
                                .setDataset(datasetUrn)
                                .setScope(DatasetAssertionScope.DATASET_COLUMN)
                                .setAggregation(AssertionStdAggregation.MAX)
                                .setOperator(AssertionStdOperator.EQUAL_TO)
                                .setFields(
                                    new UrnArray(
                                        ImmutableList.of(
                                            Urn.createFromString(
                                                "urn:li:schemaField:(urn:li:dataset:(test,test,test),fieldPath)"))))
                                .setParameters(
                                    new AssertionStdParameters()
                                        .setValue(
                                            new AssertionStdParameter()
                                                .setValue("10")
                                                .setType(AssertionStdParameterType.NUMBER))))
                        .data())));
    assertion1Aspects.put(
        Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME,
        new com.linkedin.entity.EnvelopedAspect()
            .setValue(
                new Aspect(
                    new DataPlatformInstance()
                        .setPlatform(Urn.createFromString("urn:li:dataPlatform:hive"))
                        .data())));

    // Second assertion
    Map<String, com.linkedin.entity.EnvelopedAspect> assertion2Aspects = new HashMap<>();
    assertion2Aspects.put(
        Constants.ASSERTION_KEY_ASPECT_NAME,
        new com.linkedin.entity.EnvelopedAspect()
            .setValue(new Aspect(new AssertionKey().setAssertionId("test-guid-2").data())));
    assertion2Aspects.put(
        Constants.ASSERTION_INFO_ASPECT_NAME,
        new com.linkedin.entity.EnvelopedAspect()
            .setValue(
                new Aspect(
                    new AssertionInfo()
                        .setType(AssertionType.DATASET)
                        .setDatasetAssertion(
                            new DatasetAssertionInfo()
                                .setDataset(datasetUrn)
                                .setScope(DatasetAssertionScope.DATASET_SCHEMA)
                                .setAggregation(AssertionStdAggregation.MIN)
                                .setOperator(AssertionStdOperator.GREATER_THAN)
                                .setFields(
                                    new UrnArray(
                                        ImmutableList.of(
                                            Urn.createFromString(
                                                "urn:li:schemaField:(urn:li:dataset:(test,test,test),otherField)"))))
                                .setParameters(
                                    new AssertionStdParameters()
                                        .setValue(
                                            new AssertionStdParameter()
                                                .setValue("5")
                                                .setType(AssertionStdParameterType.NUMBER))))
                        .data())));
    assertion2Aspects.put(
        Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME,
        new com.linkedin.entity.EnvelopedAspect()
            .setValue(
                new Aspect(
                    new DataPlatformInstance()
                        .setPlatform(Urn.createFromString("urn:li:dataPlatform:snowflake"))
                        .data())));

    // Populate entity responses map
    mockEntityResponses.put(
        assertionUrn1,
        new EntityResponse()
            .setEntityName(Constants.ASSERTION_ENTITY_NAME)
            .setUrn(assertionUrn1)
            .setAspects(new EnvelopedAspectMap(assertion1Aspects)));

    mockEntityResponses.put(
        assertionUrn2,
        new EntityResponse()
            .setEntityName(Constants.ASSERTION_ENTITY_NAME)
            .setUrn(assertionUrn2)
            .setAspects(new EnvelopedAspectMap(assertion2Aspects)));

    // Mock batch get response
    Mockito.when(
            mockEntityClient.batchGetV2(
                any(OperationContext.class),
                Mockito.eq(Constants.ASSERTION_ENTITY_NAME),
                Mockito.eq(ImmutableSet.copyOf(assertionUrns)),
                Mockito.any(),
                Mockito.anyBoolean()))
        .thenReturn(mockEntityResponses);

    // Create the resolver
    ListAssertionsResolver resolver = new ListAssertionsResolver(mockEntityClient, mockGraphClient);

    // Mock environment
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    OperationContext mockOpContext = Mockito.mock(OperationContext.class);
    Mockito.when(mockContext.getOperationContext()).thenReturn(mockOpContext);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    // Mock arguments
    Mockito.when(mockEnv.getArgument("query")).thenReturn("test query");
    Mockito.when(mockEnv.getArgumentOrDefault(Mockito.eq("start"), Mockito.anyInt())).thenReturn(0);
    Mockito.when(mockEnv.getArgumentOrDefault(Mockito.eq("count"), Mockito.anyInt()))
        .thenReturn(10);
    Mockito.when(mockEnv.getArgument("filter")).thenReturn(null);
    Mockito.when(mockEnv.getArgument("sort")).thenReturn(null);

    // Execute resolver
    ListAssertionsResult result = resolver.get(mockEnv).get();

    // Verify calls
    Mockito.verify(mockEntityClient, Mockito.times(1))
        .search(
            any(OperationContext.class),
            Mockito.eq(Constants.ASSERTION_ENTITY_NAME),
            Mockito.anyString(),
            Mockito.any(),
            Mockito.any(),
            Mockito.anyInt(),
            Mockito.anyInt());

    Mockito.verify(mockEntityClient, Mockito.times(1))
        .batchGetV2(
            any(OperationContext.class),
            Mockito.eq(Constants.ASSERTION_ENTITY_NAME),
            Mockito.any(),
            Mockito.any(),
            Mockito.anyBoolean());

    // Assert the results
    assertEquals(result.getStart(), 0);
    assertEquals(result.getCount(), 10);
    assertEquals(result.getTotal(), 2);
    assertEquals(result.getAssertions().size(), 2);

    // Verify first assertion
    com.linkedin.datahub.graphql.generated.Assertion assertion1 = result.getAssertions().get(0);
    assertEquals(assertion1.getUrn(), assertionUrn1.toString());
    assertEquals(assertion1.getType(), EntityType.ASSERTION);
    assertEquals(assertion1.getPlatform().getUrn(), "urn:li:dataPlatform:hive");
    assertEquals(
        assertion1.getInfo().getType(),
        com.linkedin.datahub.graphql.generated.AssertionType.DATASET);
    assertEquals(assertion1.getInfo().getDatasetAssertion().getDatasetUrn(), datasetUrn.toString());
    assertEquals(
        assertion1.getInfo().getDatasetAssertion().getScope(),
        com.linkedin.datahub.graphql.generated.DatasetAssertionScope.DATASET_COLUMN);
    assertEquals(
        assertion1.getInfo().getDatasetAssertion().getAggregation(),
        com.linkedin.datahub.graphql.generated.AssertionStdAggregation.MAX);
    assertEquals(
        assertion1.getInfo().getDatasetAssertion().getOperator(),
        com.linkedin.datahub.graphql.generated.AssertionStdOperator.EQUAL_TO);
    assertEquals(
        assertion1.getInfo().getDatasetAssertion().getParameters().getValue().getType(),
        com.linkedin.datahub.graphql.generated.AssertionStdParameterType.NUMBER);
    assertEquals(
        assertion1.getInfo().getDatasetAssertion().getParameters().getValue().getValue(), "10");

    // Verify second assertion
    com.linkedin.datahub.graphql.generated.Assertion assertion2 = result.getAssertions().get(1);
    assertEquals(assertion2.getUrn(), assertionUrn2.toString());
    assertEquals(assertion2.getType(), EntityType.ASSERTION);
    assertEquals(assertion2.getPlatform().getUrn(), "urn:li:dataPlatform:snowflake");
    assertEquals(
        assertion2.getInfo().getType(),
        com.linkedin.datahub.graphql.generated.AssertionType.DATASET);
    assertEquals(assertion2.getInfo().getDatasetAssertion().getDatasetUrn(), datasetUrn.toString());
    assertEquals(
        assertion2.getInfo().getDatasetAssertion().getScope(),
        com.linkedin.datahub.graphql.generated.DatasetAssertionScope.DATASET_SCHEMA);
    assertEquals(
        assertion2.getInfo().getDatasetAssertion().getAggregation(),
        com.linkedin.datahub.graphql.generated.AssertionStdAggregation.MIN);
    assertEquals(
        assertion2.getInfo().getDatasetAssertion().getOperator(),
        com.linkedin.datahub.graphql.generated.AssertionStdOperator.GREATER_THAN);
    assertEquals(
        assertion2.getInfo().getDatasetAssertion().getParameters().getValue().getType(),
        com.linkedin.datahub.graphql.generated.AssertionStdParameterType.NUMBER);
    assertEquals(
        assertion2.getInfo().getDatasetAssertion().getParameters().getValue().getValue(), "5");
  }

  @Test
  public void testGetEmptyResults() throws Exception {
    // Mock clients
    EntityClient mockEntityClient = Mockito.mock(EntityClient.class);
    GraphClient mockGraphClient = Mockito.mock(GraphClient.class);

    // Setup empty search result
    SearchResult mockSearchResult = new SearchResult();
    mockSearchResult.setFrom(0);
    mockSearchResult.setPageSize(10);
    mockSearchResult.setNumEntities(0);
    mockSearchResult.setEntities(new SearchEntityArray());

    // Mock the search call
    Mockito.when(
            mockEntityClient.search(
                any(OperationContext.class),
                Mockito.eq(Constants.ASSERTION_ENTITY_NAME),
                Mockito.anyString(),
                Mockito.any(),
                Mockito.any(),
                Mockito.anyInt(),
                Mockito.anyInt()))
        .thenReturn(mockSearchResult);

    // Create the resolver
    ListAssertionsResolver resolver = new ListAssertionsResolver(mockEntityClient, mockGraphClient);

    // Mock environment
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    OperationContext mockOpContext = Mockito.mock(OperationContext.class);
    Mockito.when(mockContext.getOperationContext()).thenReturn(mockOpContext);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    // Mock arguments
    Mockito.when(mockEnv.getArgument("query")).thenReturn("test query");
    Mockito.when(mockEnv.getArgumentOrDefault(Mockito.eq("start"), Mockito.anyInt())).thenReturn(0);
    Mockito.when(mockEnv.getArgumentOrDefault(Mockito.eq("count"), Mockito.anyInt()))
        .thenReturn(10);
    Mockito.when(mockEnv.getArgument("filter")).thenReturn(null);
    Mockito.when(mockEnv.getArgument("sort")).thenReturn(null);

    // Execute resolver
    ListAssertionsResult result = resolver.get(mockEnv).get();

    // Verify calls
    Mockito.verify(mockEntityClient, Mockito.times(1))
        .search(
            any(OperationContext.class),
            Mockito.eq(Constants.ASSERTION_ENTITY_NAME),
            Mockito.anyString(),
            Mockito.any(),
            Mockito.any(),
            Mockito.anyInt(),
            Mockito.anyInt());

    // No batch get should be called since there are no results
    Mockito.verify(mockEntityClient, Mockito.times(0))
        .batchGetV2(
            any(OperationContext.class),
            Mockito.anyString(),
            Mockito.any(),
            Mockito.any(),
            Mockito.anyBoolean());

    // Assert the results
    assertEquals(result.getStart(), 0);
    assertEquals(result.getCount(), 10);
    assertEquals(result.getTotal(), 0);
    assertEquals(result.getAssertions().size(), 0);
  }

  @Test
  public void testGetWithFilter() throws Exception {
    // Mock clients
    EntityClient mockEntityClient = Mockito.mock(EntityClient.class);
    GraphClient mockGraphClient = Mockito.mock(GraphClient.class);

    // Create test URNs
    Urn datasetUrn = Urn.createFromString("urn:li:dataset:(test,test,test)");
    Urn assertionUrn = Urn.createFromString("urn:li:assertion:test-guid-1");
    List<Urn> assertionUrns = ImmutableList.of(assertionUrn);

    // Setup search result
    SearchResult mockSearchResult = new SearchResult();
    mockSearchResult.setFrom(0);
    mockSearchResult.setPageSize(10);
    mockSearchResult.setNumEntities(1);
    mockSearchResult.setEntities(new SearchEntityArray());
    SearchEntity searchEntity = new SearchEntity();
    searchEntity.setEntity(assertionUrn);
    mockSearchResult.getEntities().add(searchEntity);

    // Mock the search call with filter verification
    Mockito.when(
            mockEntityClient.search(
                any(OperationContext.class),
                Mockito.eq(Constants.ASSERTION_ENTITY_NAME),
                Mockito.anyString(),
                Mockito.any(),
                Mockito.any(),
                Mockito.anyInt(),
                Mockito.anyInt()))
        .thenReturn(mockSearchResult);

    // Create assertion aspects
    Map<Urn, EntityResponse> mockEntityResponses = new HashMap<>();
    Map<String, com.linkedin.entity.EnvelopedAspect> assertionAspects = new HashMap<>();
    assertionAspects.put(
        Constants.ASSERTION_KEY_ASPECT_NAME,
        new com.linkedin.entity.EnvelopedAspect()
            .setValue(new Aspect(new AssertionKey().setAssertionId("test-guid-1").data())));
    assertionAspects.put(
        Constants.ASSERTION_INFO_ASPECT_NAME,
        new com.linkedin.entity.EnvelopedAspect()
            .setValue(
                new Aspect(
                    new AssertionInfo()
                        .setType(AssertionType.DATASET)
                        .setDatasetAssertion(
                            new DatasetAssertionInfo()
                                .setDataset(datasetUrn)
                                .setScope(DatasetAssertionScope.DATASET_COLUMN)
                                .setAggregation(AssertionStdAggregation.MAX)
                                .setOperator(AssertionStdOperator.EQUAL_TO)
                                .setFields(
                                    new UrnArray(
                                        ImmutableList.of(
                                            Urn.createFromString(
                                                "urn:li:schemaField:(urn:li:dataset:(test,test,test),fieldPath)"))))
                                .setParameters(
                                    new AssertionStdParameters()
                                        .setValue(
                                            new AssertionStdParameter()
                                                .setValue("10")
                                                .setType(AssertionStdParameterType.NUMBER))))
                        .data())));
    assertionAspects.put(
        Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME,
        new com.linkedin.entity.EnvelopedAspect()
            .setValue(
                new Aspect(
                    new DataPlatformInstance()
                        .setPlatform(Urn.createFromString("urn:li:dataPlatform:hive"))
                        .data())));

    mockEntityResponses.put(
        assertionUrn,
        new EntityResponse()
            .setEntityName(Constants.ASSERTION_ENTITY_NAME)
            .setUrn(assertionUrn)
            .setAspects(new EnvelopedAspectMap(assertionAspects)));

    // Mock batch get response
    Mockito.when(
            mockEntityClient.batchGetV2(
                any(OperationContext.class),
                Mockito.eq(Constants.ASSERTION_ENTITY_NAME),
                Mockito.eq(ImmutableSet.copyOf(assertionUrns)),
                Mockito.any(),
                Mockito.anyBoolean()))
        .thenReturn(mockEntityResponses);

    // Create the resolver
    ListAssertionsResolver resolver = new ListAssertionsResolver(mockEntityClient, mockGraphClient);

    // Mock environment
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    OperationContext mockOpContext = Mockito.mock(OperationContext.class);
    Mockito.when(mockContext.getOperationContext()).thenReturn(mockOpContext);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    // Create a filter
    AssertionsQueryFilterInput filter = new AssertionsQueryFilterInput();
    filter.setCreator(ImmutableList.of("test-creator"));

    // Mock arguments with filter
    Mockito.when(mockEnv.getArgument("query")).thenReturn("test query");
    Mockito.when(mockEnv.getArgumentOrDefault(Mockito.eq("start"), Mockito.anyInt())).thenReturn(0);
    Mockito.when(mockEnv.getArgumentOrDefault(Mockito.eq("count"), Mockito.anyInt()))
        .thenReturn(10);
    Mockito.when(mockEnv.getArgument("filter")).thenReturn(filter);
    Mockito.when(mockEnv.getArgument("sort")).thenReturn(null);

    // Execute resolver
    ListAssertionsResult result = resolver.get(mockEnv).get();

    // Verify the results
    assertEquals(result.getStart(), 0);
    assertEquals(result.getCount(), 10);
    assertEquals(result.getTotal(), 1);
    assertEquals(result.getAssertions().size(), 1);

    // Verify the assertion
    com.linkedin.datahub.graphql.generated.Assertion assertion = result.getAssertions().get(0);
    assertEquals(assertion.getUrn(), assertionUrn.toString());
    assertEquals(assertion.getType(), EntityType.ASSERTION);
  }

  @Test
  public void testInputToFilter() {
    // Mock clients
    EntityClient mockEntityClient = Mockito.mock(EntityClient.class);
    GraphClient mockGraphClient = Mockito.mock(GraphClient.class);

    // Create the resolver
    ListAssertionsResolver resolver = new ListAssertionsResolver(mockEntityClient, mockGraphClient);

    // Test case 1: No time ranges, just non-time range filters
    AssertionsQueryFilterInput filter1 = new AssertionsQueryFilterInput();
    filter1.setCreator(ImmutableList.of("test-creator"));
    filter1.setTags(ImmutableList.of("test-tag"));

    Filter result1 = resolver.inputToFilter(filter1);
    assertNotNull(result1);
    assertEquals(result1.getOr().size(), 1);
    assertEquals(result1.getOr().get(0).getAnd().size(), 2);
    assertEquals(result1.getOr().get(0).getAnd().get(0).getField(), "creator");
    assertEquals(result1.getOr().get(0).getAnd().get(1).getField(), "tags");

    // Test case 2: Time ranges with AND operator
    AssertionsQueryFilterInput filter2 = new AssertionsQueryFilterInput();
    filter2.setCreator(ImmutableList.of("test-creator"));

    // Create time range filters
    FacetFilterInput timeRange1 = new FacetFilterInput();
    timeRange1.setField("assertionLastFailed");
    timeRange1.setCondition(FilterOperator.GREATER_THAN);
    timeRange1.setValues(ImmutableList.of("2024-01-01"));

    FacetFilterInput timeRange2 = new FacetFilterInput();
    timeRange2.setField("assertionLastPassed");
    timeRange2.setCondition(FilterOperator.LESS_THAN);
    timeRange2.setValues(ImmutableList.of("2024-02-01"));

    // Create the time range group
    AssertionsTimeRangeFilter timeRangeGroup = new AssertionsTimeRangeFilter();
    timeRangeGroup.setOperator(LogicalOperator.AND);
    timeRangeGroup.setFilter(ImmutableList.of(timeRange1, timeRange2));

    // Set the time ranges in the filter
    filter2.setTimeRanges(timeRangeGroup);

    Filter result2 = resolver.inputToFilter(filter2);
    assertNotNull(result2);
    assertEquals(result2.getOr().size(), 1);
    assertEquals(result2.getOr().get(0).getAnd().size(), 3); // creator + 2 time ranges
    assertEquals(result2.getOr().get(0).getAnd().get(0).getField(), "creator");
    assertEquals(result2.getOr().get(0).getAnd().get(1).getField(), "assertionLastFailed");
    assertEquals(result2.getOr().get(0).getAnd().get(2).getField(), "assertionLastPassed");

    // Test case 3: Time ranges with OR operator
    AssertionsQueryFilterInput filter3 = new AssertionsQueryFilterInput();
    filter3.setCreator(ImmutableList.of("test-creator"));

    // Create a new time range group with OR operator
    AssertionsTimeRangeFilter timeRangeGroup2 = new AssertionsTimeRangeFilter();
    timeRangeGroup2.setOperator(LogicalOperator.OR);
    timeRangeGroup2.setFilter(ImmutableList.of(timeRange1, timeRange2));

    // Set the time ranges in the filter
    filter3.setTimeRanges(timeRangeGroup2);

    Filter result3 = resolver.inputToFilter(filter3);
    assertNotNull(result3);
    assertEquals(result3.getOr().size(), 2); // One for each time range
    assertEquals(result3.getOr().get(0).getAnd().size(), 2); // creator + first time range
    assertEquals(result3.getOr().get(1).getAnd().size(), 2); // creator + second time range
    assertEquals(result3.getOr().get(0).getAnd().get(0).getField(), "creator");
    assertEquals(result3.getOr().get(0).getAnd().get(1).getField(), "assertionLastFailed");
    assertEquals(result3.getOr().get(1).getAnd().get(0).getField(), "creator");
    assertEquals(result3.getOr().get(1).getAnd().get(1).getField(), "assertionLastPassed");
  }
}
