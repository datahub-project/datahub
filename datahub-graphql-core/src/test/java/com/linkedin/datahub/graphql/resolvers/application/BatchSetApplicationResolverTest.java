package com.linkedin.datahub.graphql.resolvers.application;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.BatchSetApplicationInput;
import com.linkedin.metadata.service.ApplicationService;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class BatchSetApplicationResolverTest {

  private static final String TEST_ENTITY_URN_1 =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my-test,PROD)";
  private static final String TEST_ENTITY_URN_2 =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my-test-2,PROD)";
  private static final String TEST_APPLICATION_URN = "urn:li:application:test-app-id";
  private static final String TEST_ACTOR_URN = "urn:li:corpuser:test";

  private ApplicationService mockApplicationService;
  private BatchSetApplicationResolver resolver;
  private QueryContext mockContext;
  private DataFetchingEnvironment mockEnv;

  @BeforeMethod
  public void setupTest() {
    mockApplicationService = Mockito.mock(ApplicationService.class);
    resolver = new BatchSetApplicationResolver(mockApplicationService);
    mockContext = getMockAllowContext(TEST_ACTOR_URN);
    mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
  }

  private void mockExists(Urn urn, boolean exists) {
    Mockito.when(mockApplicationService.verifyEntityExists(any(), eq(urn))).thenReturn(exists);
  }

  @Test
  public void testGetSuccessSetApplication() throws Exception {
    mockExists(UrnUtils.getUrn(TEST_ENTITY_URN_1), true);
    mockExists(UrnUtils.getUrn(TEST_ENTITY_URN_2), true);
    mockExists(UrnUtils.getUrn(TEST_APPLICATION_URN), true);

    BatchSetApplicationInput input =
        new BatchSetApplicationInput(
            TEST_APPLICATION_URN, ImmutableList.of(TEST_ENTITY_URN_1, TEST_ENTITY_URN_2));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);

    assertTrue(resolver.get(mockEnv).get());

    Mockito.verify(mockApplicationService, Mockito.times(1))
        .batchSetApplicationAssets(
            any(),
            eq(UrnUtils.getUrn(TEST_APPLICATION_URN)),
            eq(
                ImmutableList.of(
                    UrnUtils.getUrn(TEST_ENTITY_URN_1), UrnUtils.getUrn(TEST_ENTITY_URN_2))),
            eq(UrnUtils.getUrn(TEST_ACTOR_URN)));
  }

  @Test
  public void testGetSuccessUnsetApplication() throws Exception {
    mockExists(UrnUtils.getUrn(TEST_ENTITY_URN_1), true);
    mockExists(UrnUtils.getUrn(TEST_ENTITY_URN_2), true);

    BatchSetApplicationInput input =
        new BatchSetApplicationInput(null, ImmutableList.of(TEST_ENTITY_URN_1, TEST_ENTITY_URN_2));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);

    assertTrue(resolver.get(mockEnv).get());

    Mockito.verify(mockApplicationService, Mockito.times(1))
        .unsetApplication(
            any(), eq(UrnUtils.getUrn(TEST_ENTITY_URN_1)), eq(UrnUtils.getUrn(TEST_ACTOR_URN)));
    Mockito.verify(mockApplicationService, Mockito.times(1))
        .unsetApplication(
            any(), eq(UrnUtils.getUrn(TEST_ENTITY_URN_2)), eq(UrnUtils.getUrn(TEST_ACTOR_URN)));
  }

  @Test
  public void testGetFailureApplicationDoesNotExist() {
    mockExists(UrnUtils.getUrn(TEST_ENTITY_URN_1), true);
    mockExists(UrnUtils.getUrn(TEST_APPLICATION_URN), false); // Application does not exist

    BatchSetApplicationInput input =
        new BatchSetApplicationInput(TEST_APPLICATION_URN, ImmutableList.of(TEST_ENTITY_URN_1));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockApplicationService, Mockito.never())
        .batchSetApplicationAssets(any(), any(), any(), any());
  }

  @Test
  public void testGetFailureResourceDoesNotExist() {
    mockExists(UrnUtils.getUrn(TEST_ENTITY_URN_1), false); // Resource does not exist
    mockExists(UrnUtils.getUrn(TEST_APPLICATION_URN), true);

    BatchSetApplicationInput input =
        new BatchSetApplicationInput(TEST_APPLICATION_URN, ImmutableList.of(TEST_ENTITY_URN_1));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockApplicationService, Mockito.never())
        .batchSetApplicationAssets(any(), any(), any(), any());
  }

  @Test
  public void testGetUnauthorized() {
    // No need to mock exists, as authorization happens first.
    QueryContext mockDenyContext = getMockDenyContext();
    Mockito.when(mockEnv.getContext()).thenReturn(mockDenyContext);

    BatchSetApplicationInput input =
        new BatchSetApplicationInput(TEST_APPLICATION_URN, ImmutableList.of(TEST_ENTITY_URN_1));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockApplicationService, Mockito.never())
        .batchSetApplicationAssets(any(), any(), any(), any());
  }
}
