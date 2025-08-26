package com.linkedin.datahub.graphql.resolvers.test;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.datahub.graphql.resolvers.test.TestResultsSummaryResolver.*;
import static com.linkedin.metadata.AcrylConstants.*;
import static com.linkedin.metadata.test.util.TestUtils.*;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.TestResultsSummary;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import graphql.schema.DataFetchingEnvironment;
import javax.annotation.Nonnull;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class TestResultsSummaryResolverTest {

  private static final Urn TEST_URN = UrnUtils.getUrn("urn:li:test:test");

  @Test
  public void testGetSuccess() throws Exception {
    // Create resolver
    EntitySearchService mockService = Mockito.mock(EntitySearchService.class);
    TimeseriesAspectService mockTsService = Mockito.mock(TimeseriesAspectService.class);
    EntityService mockEntityService = Mockito.mock(EntityService.class);

    // set up passing mock
    Mockito.when(
            mockService.aggregateByValue(
                Mockito.any(),
                Mockito.eq(null),
                Mockito.eq(PASSING_TESTS_FIELD),
                Mockito.eq(buildTestPassingFilter(TEST_URN, null, null)),
                Mockito.eq(MAX_AGGREGATION_LIMIT)))
        .thenReturn(ImmutableMap.of(TEST_URN.toString(), 50L));

    // set up failing mock
    Mockito.when(
            mockService.aggregateByValue(
                Mockito.any(),
                Mockito.eq(null),
                Mockito.eq(FAILING_TESTS_FIELD),
                Mockito.eq(buildTestFailingFilter(TEST_URN, null, null)),
                Mockito.eq(MAX_AGGREGATION_LIMIT)))
        .thenReturn(ImmutableMap.of(TEST_URN.toString(), 20L));

    TestResultsSummaryResolver resolver =
        new TestResultsSummaryResolver(mockService, mockEntityService, mockTsService);

    com.linkedin.datahub.graphql.generated.Test parentTest =
        new com.linkedin.datahub.graphql.generated.Test();
    parentTest.setUrn(TEST_URN.toString());

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getSource()).thenReturn(parentTest);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    final TestResultsSummary result = resolver.get(mockEnv).get();
    assertEquals((long) result.getPassingCount(), 50L);
    assertEquals((long) result.getFailingCount(), 20L);
  }

  @Test
  public void testGetServiceError() throws Exception {
    // Create resolver
    EntitySearchService mockService = Mockito.mock(EntitySearchService.class);
    EntityService mockEntityService = Mockito.mock(EntityService.class);
    TimeseriesAspectService mockTsService = Mockito.mock(TimeseriesAspectService.class);

    // set up passing mock
    Mockito.when(
            mockService.aggregateByValue(
                Mockito.any(),
                Mockito.eq(null),
                Mockito.eq(PASSING_TESTS_FIELD),
                Mockito.eq(buildFilter(PASSING_TESTS_FIELD)),
                Mockito.eq(MAX_AGGREGATION_LIMIT)))
        .thenThrow(new RuntimeException("Error!"));

    // set up failing mock
    Mockito.when(
            mockService.aggregateByValue(
                Mockito.any(),
                Mockito.eq(null),
                Mockito.eq(FAILING_TESTS_FIELD),
                Mockito.eq(buildFilter(FAILING_TESTS_FIELD)),
                Mockito.eq(MAX_AGGREGATION_LIMIT)))
        .thenThrow(new RuntimeException("Error!"));

    TestResultsSummaryResolver resolver =
        new TestResultsSummaryResolver(mockService, mockEntityService, mockTsService);

    com.linkedin.datahub.graphql.generated.Test parentTest =
        new com.linkedin.datahub.graphql.generated.Test();
    parentTest.setUrn(TEST_URN.toString());

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getSource()).thenReturn(parentTest);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    final TestResultsSummary result = resolver.get(mockEnv).get();
    assertEquals((long) result.getPassingCount(), 0L); // Default in error case is 0L
    assertEquals((long) result.getFailingCount(), 0L); // Default in error case is 0L
  }

  private static Filter buildFilter(@Nonnull final String fieldName) {
    final Filter result = new Filter();
    final String fieldNameWithSuffix = ESUtils.toKeywordField(fieldName, false, null);
    result.setOr(
        new ConjunctiveCriterionArray(
            ImmutableList.of(
                new ConjunctiveCriterion()
                    .setAnd(
                        new CriterionArray(
                            ImmutableList.of(
                                new Criterion()
                                    .setNegated(false)
                                    .setField(fieldNameWithSuffix)
                                    .setValues(
                                        new StringArray(
                                            ImmutableList.of(
                                                TestResultsSummaryResolverTest.TEST_URN
                                                    .toString())))
                                    .setValue(TestResultsSummaryResolverTest.TEST_URN.toString())
                                    .setCondition(Condition.EQUAL)))))));
    return result;
  }
}
