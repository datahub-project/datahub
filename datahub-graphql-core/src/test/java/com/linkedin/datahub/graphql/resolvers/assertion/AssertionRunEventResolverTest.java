package com.linkedin.datahub.graphql.resolvers.assertion;

import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.linkedin.assertion.AssertionResult;
import com.linkedin.assertion.AssertionResultType;
import com.linkedin.assertion.AssertionRunEvent;
import com.linkedin.assertion.AssertionRunStatus;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Assertion;
import com.linkedin.datahub.graphql.generated.AssertionRunEventsResult;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.SystemMetadata;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class AssertionRunEventResolverTest {
  @Test
  public void testGetSuccess() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    final Urn assertionUrn = Urn.createFromString("urn:li:assertion:guid-1");
    final Urn asserteeUrn = Urn.createFromString("urn:li:dataset:(test,test,test)");
    final AssertionRunEvent gmsRunEvent =
        new AssertionRunEvent()
            .setTimestampMillis(12L)
            .setAssertionUrn(assertionUrn)
            .setRunId("test-id")
            .setAsserteeUrn(asserteeUrn)
            .setStatus(AssertionRunStatus.COMPLETE)
            .setResult(
                new AssertionResult()
                    .setActualAggValue(10)
                    .setMissingCount(0L)
                    .setRowCount(1L)
                    .setType(AssertionResultType.SUCCESS)
                    .setUnexpectedCount(2L));

    Mockito.when(
            mockClient.getTimeseriesAspectValues(
                any(),
                Mockito.eq(assertionUrn.toString()),
                Mockito.eq(Constants.ASSERTION_ENTITY_NAME),
                Mockito.eq(Constants.ASSERTION_RUN_EVENT_ASPECT_NAME),
                Mockito.eq(0L),
                Mockito.eq(10L),
                Mockito.eq(5),
                Mockito.eq(
                    AssertionRunEventResolver.buildFilter(
                        null, AssertionRunStatus.COMPLETE.toString(), null))))
        .thenReturn(
            ImmutableList.of(
                new EnvelopedAspect()
                    .setAspect(GenericRecordUtils.serializeAspect(gmsRunEvent))
                    .setSystemMetadata(new SystemMetadata().setLastObserved(12L))));

    AssertionRunEventResolver resolver = new AssertionRunEventResolver(mockClient);

    // Execute resolver
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getOperationContext())
        .thenReturn(Mockito.mock(OperationContext.class));

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);

    Mockito.when(mockEnv.getArgumentOrDefault(Mockito.eq("status"), Mockito.eq(null)))
        .thenReturn("COMPLETE");
    Mockito.when(mockEnv.getArgumentOrDefault(Mockito.eq("startTimeMillis"), Mockito.eq(null)))
        .thenReturn(0L);
    Mockito.when(mockEnv.getArgumentOrDefault(Mockito.eq("endTimeMillis"), Mockito.eq(null)))
        .thenReturn(10L);
    Mockito.when(mockEnv.getArgumentOrDefault(Mockito.eq("limit"), Mockito.eq(null))).thenReturn(5);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Assertion parentAssertion = new Assertion();
    parentAssertion.setUrn(assertionUrn.toString());
    Mockito.when(mockEnv.getSource()).thenReturn(parentAssertion);

    AssertionRunEventsResult result = resolver.get(mockEnv).get();

    Mockito.verify(mockClient, Mockito.times(1))
        .getTimeseriesAspectValues(
            any(),
            Mockito.eq(assertionUrn.toString()),
            Mockito.eq(Constants.ASSERTION_ENTITY_NAME),
            Mockito.eq(Constants.ASSERTION_RUN_EVENT_ASPECT_NAME),
            Mockito.eq(0L),
            Mockito.eq(10L),
            Mockito.eq(5),
            Mockito.any(Filter.class));

    // Assert that GraphQL assertion run event matches expectations
    assertEquals(result.getTotal(), 1);
    assertEquals(result.getFailed(), 0);
    assertEquals(result.getSucceeded(), 1);
    assertEquals(result.getErrored(), 0);

    com.linkedin.datahub.graphql.generated.AssertionRunEvent graphqlRunEvent =
        resolver.get(mockEnv).get().getRunEvents().get(0);
    assertEquals(graphqlRunEvent.getAssertionUrn(), assertionUrn.toString());
    assertEquals(graphqlRunEvent.getAsserteeUrn(), asserteeUrn.toString());
    assertEquals(graphqlRunEvent.getRunId(), "test-id");
    assertEquals(
        graphqlRunEvent.getStatus(),
        com.linkedin.datahub.graphql.generated.AssertionRunStatus.COMPLETE);
    assertEquals((float) graphqlRunEvent.getTimestampMillis(), 12L);
    assertEquals((float) graphqlRunEvent.getLastObservedMillis(), 12L);
    assertEquals((float) graphqlRunEvent.getResult().getActualAggValue(), 10);
    assertEquals((long) graphqlRunEvent.getResult().getMissingCount(), 0L);
    assertEquals((long) graphqlRunEvent.getResult().getRowCount(), 1L);
    assertEquals((long) graphqlRunEvent.getResult().getUnexpectedCount(), 2L);
    assertEquals(
        graphqlRunEvent.getResult().getType(),
        com.linkedin.datahub.graphql.generated.AssertionResultType.SUCCESS);
  }
}
