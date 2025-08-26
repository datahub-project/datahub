package com.linkedin.datahub.graphql.resolvers.test;

import static org.mockito.ArgumentMatchers.nullable;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.BatchTestRunEventsResult;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.AcrylConstants;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.test.BatchTestRunEvent;
import com.linkedin.test.BatchTestRunResult;
import com.linkedin.test.BatchTestRunStatus;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class BatchTestRunEventsResolverTest {

  @Test
  public void testGetSuccess() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    final Urn testUrn = Urn.createFromString("urn:li:test:guid-1");
    final BatchTestRunEvent gmsRunEvent =
        new BatchTestRunEvent()
            .setTimestampMillis(12L)
            .setStatus(BatchTestRunStatus.COMPLETE)
            .setResult(new BatchTestRunResult().setFailingCount(10).setPassingCount(20));

    Mockito.when(
            mockClient.getTimeseriesAspectValues(
                nullable(OperationContext.class),
                Mockito.eq(testUrn.toString()),
                Mockito.eq(Constants.TEST_ENTITY_NAME),
                Mockito.eq(AcrylConstants.BATCH_TEST_RUN_EVENT_ASPECT_NAME),
                Mockito.eq(0L),
                Mockito.eq(10L),
                Mockito.eq(5),
                Mockito.eq(null)))
        .thenReturn(
            ImmutableList.of(
                new EnvelopedAspect().setAspect(GenericRecordUtils.serializeAspect(gmsRunEvent))));

    BatchTestRunEventsResolver resolver = new BatchTestRunEventsResolver(mockClient);

    // Execute resolver
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);

    Mockito.when(mockEnv.getArgumentOrDefault(Mockito.eq("startTimeMillis"), Mockito.eq(null)))
        .thenReturn(0L);
    Mockito.when(mockEnv.getArgumentOrDefault(Mockito.eq("endTimeMillis"), Mockito.eq(null)))
        .thenReturn(10L);
    Mockito.when(mockEnv.getArgumentOrDefault(Mockito.eq("limit"), Mockito.eq(null))).thenReturn(5);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    com.linkedin.datahub.graphql.generated.Test parentAssertion =
        new com.linkedin.datahub.graphql.generated.Test();
    parentAssertion.setUrn(testUrn.toString());
    Mockito.when(mockEnv.getSource()).thenReturn(parentAssertion);

    BatchTestRunEventsResult result = resolver.get(mockEnv).get();

    Mockito.verify(mockClient, Mockito.times(1))
        .getTimeseriesAspectValues(
            nullable(OperationContext.class),
            Mockito.eq(testUrn.toString()),
            Mockito.eq(Constants.TEST_ENTITY_NAME),
            Mockito.eq(AcrylConstants.BATCH_TEST_RUN_EVENT_ASPECT_NAME),
            Mockito.eq(0L),
            Mockito.eq(10L),
            Mockito.eq(5),
            Mockito.eq(null));

    // Assert that GraphQL assertion run event matches expectations
    com.linkedin.datahub.graphql.generated.BatchTestRunEvent graphqlRunEvent =
        resolver.get(mockEnv).get().getBatchRunEvents().get(0);
    assertEquals(
        (long) graphqlRunEvent.getTimestampMillis(), (long) gmsRunEvent.getTimestampMillis());
    assertEquals(
        graphqlRunEvent.getStatus(),
        com.linkedin.datahub.graphql.generated.BatchTestRunStatus.valueOf(
            gmsRunEvent.getStatus().toString()));
    assertEquals(
        graphqlRunEvent.getResult().getPassingCount(), gmsRunEvent.getResult().getPassingCount());
    assertEquals(
        graphqlRunEvent.getResult().getFailingCount(), gmsRunEvent.getResult().getFailingCount());
  }
}
