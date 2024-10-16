package com.linkedin.datahub.graphql.resolvers.assertion;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableSet;
import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionStdOperator;
import com.linkedin.assertion.AssertionType;
import com.linkedin.assertion.DatasetAssertionInfo;
import com.linkedin.assertion.DatasetAssertionScope;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class DeleteAssertionResolverTest {

  private static final String TEST_ASSERTION_URN = "urn:li:assertion:test-guid";
  private static final String TEST_DATASET_URN = "urn:li:dataset:(test,test,test)";

  @Test
  public void testGetSuccess() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    EntityService<?> mockService = getMockEntityService();
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ASSERTION_URN)), eq(true)))
        .thenReturn(true);
    Mockito.when(
            mockService.getAspect(
                any(),
                eq(Urn.createFromString(TEST_ASSERTION_URN)),
                eq(Constants.ASSERTION_INFO_ASPECT_NAME),
                eq(0L)))
        .thenReturn(
            new AssertionInfo()
                .setType(AssertionType.DATASET)
                .setDatasetAssertion(
                    new DatasetAssertionInfo()
                        .setDataset(Urn.createFromString(TEST_DATASET_URN))
                        .setScope(DatasetAssertionScope.DATASET_COLUMN)
                        .setOperator(AssertionStdOperator.BETWEEN)));

    DeleteAssertionResolver resolver = new DeleteAssertionResolver(mockClient, mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(eq("urn"))).thenReturn(TEST_ASSERTION_URN);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());

    Mockito.verify(mockClient, Mockito.times(1))
        .deleteEntity(any(), eq(Urn.createFromString(TEST_ASSERTION_URN)));

    Mockito.verify(mockService, Mockito.times(1))
        .getAspect(
            any(),
            eq(Urn.createFromString(TEST_ASSERTION_URN)),
            eq(Constants.ASSERTION_INFO_ASPECT_NAME),
            eq(0L));

    Mockito.verify(mockService, Mockito.times(1))
        .exists(any(), eq(Urn.createFromString(TEST_ASSERTION_URN)), eq(true));
  }

  @Test
  public void testGetSuccessNoAssertionInfoFound() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    EntityService<?> mockService = getMockEntityService();
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ASSERTION_URN)), eq(true)))
        .thenReturn(true);
    Mockito.when(
            mockService.getAspect(
                any(),
                eq(Urn.createFromString(TEST_ASSERTION_URN)),
                eq(Constants.ASSERTION_INFO_ASPECT_NAME),
                eq(0L)))
        .thenReturn(null);

    DeleteAssertionResolver resolver = new DeleteAssertionResolver(mockClient, mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(eq("urn"))).thenReturn(TEST_ASSERTION_URN);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());

    Mockito.verify(mockClient, Mockito.times(1))
        .deleteEntity(any(), eq(Urn.createFromString(TEST_ASSERTION_URN)));

    Mockito.verify(mockService, Mockito.times(1))
        .getAspect(
            any(),
            eq(Urn.createFromString(TEST_ASSERTION_URN)),
            eq(Constants.ASSERTION_INFO_ASPECT_NAME),
            eq(0L));

    Mockito.verify(mockService, Mockito.times(1))
        .exists(any(), eq(Urn.createFromString(TEST_ASSERTION_URN)), eq(true));
  }

  @Test
  public void testGetSuccessAssertionAlreadyRemoved() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    EntityService<?> mockService = getMockEntityService();
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ASSERTION_URN)), eq(true)))
        .thenReturn(false);

    DeleteAssertionResolver resolver = new DeleteAssertionResolver(mockClient, mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(eq("urn"))).thenReturn(TEST_ASSERTION_URN);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());

    Mockito.verify(mockClient, Mockito.times(0))
        .deleteEntity(any(), eq(Urn.createFromString(TEST_ASSERTION_URN)));

    Mockito.verify(mockClient, Mockito.times(0))
        .batchGetV2(
            any(),
            eq(Constants.ASSERTION_ENTITY_NAME),
            eq(ImmutableSet.of(Urn.createFromString(TEST_ASSERTION_URN))),
            eq(ImmutableSet.of(Constants.ASSERTION_INFO_ASPECT_NAME)));

    Mockito.verify(mockService, Mockito.times(1))
        .exists(any(), eq(Urn.createFromString(TEST_ASSERTION_URN)), eq(true));
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    EntityService<?> mockService = getMockEntityService();
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ASSERTION_URN)), eq(true)))
        .thenReturn(true);
    Mockito.when(
            mockService.getAspect(
                any(),
                eq(Urn.createFromString(TEST_ASSERTION_URN)),
                eq(Constants.ASSERTION_INFO_ASPECT_NAME),
                eq(0L)))
        .thenReturn(
            new AssertionInfo()
                .setType(AssertionType.DATASET)
                .setDatasetAssertion(
                    new DatasetAssertionInfo()
                        .setDataset(Urn.createFromString(TEST_DATASET_URN))
                        .setScope(DatasetAssertionScope.DATASET_COLUMN)
                        .setOperator(AssertionStdOperator.BETWEEN)));

    DeleteAssertionResolver resolver = new DeleteAssertionResolver(mockClient, mockService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(eq("urn"))).thenReturn(TEST_ASSERTION_URN);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockClient, Mockito.times(0)).deleteEntity(any(), Mockito.any());
  }

  @Test
  public void testGetEntityClientException() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.doThrow(RemoteInvocationException.class)
        .when(mockClient)
        .deleteEntity(any(), Mockito.any());

    EntityService<?> mockService = getMockEntityService();
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ASSERTION_URN)), eq(true)))
        .thenReturn(true);

    DeleteAssertionResolver resolver = new DeleteAssertionResolver(mockClient, mockService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(eq("urn"))).thenReturn(TEST_ASSERTION_URN);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }
}
