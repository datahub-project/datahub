package com.linkedin.datahub.graphql.resolvers.application;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.BatchUnsetApplicationInput;
import com.linkedin.metadata.service.ApplicationService;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class BatchUnsetApplicationResolverTest {

  private static final String TEST_ENTITY_URN_1 =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my-test,PROD)";
  private static final String TEST_ENTITY_URN_2 =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my-test-2,PROD)";
  private static final String TEST_APPLICATION_URN = "urn:li:application:test-app-id";
  private static final String TEST_ACTOR_URN = "urn:li:corpuser:test";

  private ApplicationService mockApplicationService;
  private BatchUnsetApplicationResolver resolver;
  private QueryContext mockContext;
  private DataFetchingEnvironment mockEnv;
  private Set<Urn> existingUrns;

  @BeforeMethod
  public void setupTest() {
    mockApplicationService = Mockito.mock(ApplicationService.class);
    resolver = new BatchUnsetApplicationResolver(mockApplicationService);
    mockContext = getMockAllowContext(TEST_ACTOR_URN);
    mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    existingUrns = new HashSet<>();
    // Mirrors the real contract: returns the subset of the requested urns that exist.
    Mockito.when(mockApplicationService.filterExistingEntities(any(), any()))
        .thenAnswer(
            invocation -> {
              Collection<Urn> requested = invocation.getArgument(1);
              return requested.stream().filter(existingUrns::contains).collect(Collectors.toSet());
            });
  }

  private void mockExists(Urn urn, boolean exists) {
    if (exists) {
      existingUrns.add(urn);
    } else {
      existingUrns.remove(urn);
    }
    Mockito.when(mockApplicationService.verifyEntityExists(any(), eq(urn))).thenReturn(exists);
  }

  @Test
  public void testGetSuccessUnsetApplication() throws Exception {
    mockExists(UrnUtils.getUrn(TEST_ENTITY_URN_1), true);
    mockExists(UrnUtils.getUrn(TEST_ENTITY_URN_2), true);
    mockExists(UrnUtils.getUrn(TEST_APPLICATION_URN), true);

    BatchUnsetApplicationInput input =
        new BatchUnsetApplicationInput(
            TEST_APPLICATION_URN, ImmutableList.of(TEST_ENTITY_URN_1, TEST_ENTITY_URN_2));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);

    assertTrue(resolver.get(mockEnv).get());

    Mockito.verify(mockApplicationService, Mockito.times(1))
        .batchUnsetApplication(
            any(),
            eq(UrnUtils.getUrn(TEST_APPLICATION_URN)),
            eq(
                ImmutableList.of(
                    UrnUtils.getUrn(TEST_ENTITY_URN_1), UrnUtils.getUrn(TEST_ENTITY_URN_2))),
            eq(UrnUtils.getUrn(TEST_ACTOR_URN)));
  }

  @Test
  public void testGetSuccessUnsetApplicationSingleResource() throws Exception {
    mockExists(UrnUtils.getUrn(TEST_ENTITY_URN_1), true);
    mockExists(UrnUtils.getUrn(TEST_APPLICATION_URN), true);

    BatchUnsetApplicationInput input =
        new BatchUnsetApplicationInput(TEST_APPLICATION_URN, ImmutableList.of(TEST_ENTITY_URN_1));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);

    assertTrue(resolver.get(mockEnv).get());

    Mockito.verify(mockApplicationService, Mockito.times(1))
        .batchUnsetApplication(
            any(),
            eq(UrnUtils.getUrn(TEST_APPLICATION_URN)),
            eq(ImmutableList.of(UrnUtils.getUrn(TEST_ENTITY_URN_1))),
            eq(UrnUtils.getUrn(TEST_ACTOR_URN)));
  }

  @Test
  public void testGetFailureApplicationDoesNotExist() {
    mockExists(UrnUtils.getUrn(TEST_ENTITY_URN_1), true);
    mockExists(UrnUtils.getUrn(TEST_APPLICATION_URN), false); // Application does not exist

    BatchUnsetApplicationInput input =
        new BatchUnsetApplicationInput(TEST_APPLICATION_URN, ImmutableList.of(TEST_ENTITY_URN_1));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockApplicationService, Mockito.never())
        .batchUnsetApplication(any(), any(), any(), any());
  }

  @Test
  public void testGetFailureResourceDoesNotExist() {
    mockExists(UrnUtils.getUrn(TEST_ENTITY_URN_1), false); // Resource does not exist
    mockExists(UrnUtils.getUrn(TEST_APPLICATION_URN), true);

    BatchUnsetApplicationInput input =
        new BatchUnsetApplicationInput(TEST_APPLICATION_URN, ImmutableList.of(TEST_ENTITY_URN_1));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockApplicationService, Mockito.never())
        .batchUnsetApplication(any(), any(), any(), any());
  }

  @Test
  public void testGetUnauthorized() {
    QueryContext mockDenyContext = getMockDenyContext();
    Mockito.when(mockEnv.getContext()).thenReturn(mockDenyContext);

    BatchUnsetApplicationInput input =
        new BatchUnsetApplicationInput(TEST_APPLICATION_URN, ImmutableList.of(TEST_ENTITY_URN_1));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockApplicationService, Mockito.never())
        .batchUnsetApplication(any(), any(), any(), any());
  }

  @Test
  public void testGetVerifiesAllResourcesBeforeCallingService() throws Exception {
    mockExists(UrnUtils.getUrn(TEST_ENTITY_URN_1), true);
    mockExists(UrnUtils.getUrn(TEST_ENTITY_URN_2), true);
    mockExists(UrnUtils.getUrn(TEST_APPLICATION_URN), true);

    BatchUnsetApplicationInput input =
        new BatchUnsetApplicationInput(
            TEST_APPLICATION_URN, ImmutableList.of(TEST_ENTITY_URN_1, TEST_ENTITY_URN_2));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);

    assertTrue(resolver.get(mockEnv).get());

    // All resources are checked for existence in a single request; the application is checked
    // separately since it is not part of the resource list.
    Mockito.verify(mockApplicationService, Mockito.times(1))
        .filterExistingEntities(
            any(),
            eq(
                ImmutableList.of(
                    UrnUtils.getUrn(TEST_ENTITY_URN_1), UrnUtils.getUrn(TEST_ENTITY_URN_2))));
    Mockito.verify(mockApplicationService, Mockito.times(1))
        .verifyEntityExists(any(), eq(UrnUtils.getUrn(TEST_APPLICATION_URN)));
    Mockito.verify(mockApplicationService, Mockito.never())
        .verifyEntityExists(any(), eq(UrnUtils.getUrn(TEST_ENTITY_URN_1)));

    // Verify batchUnsetApplication was called with correct parameters
    Mockito.verify(mockApplicationService, Mockito.times(1))
        .batchUnsetApplication(
            any(),
            eq(UrnUtils.getUrn(TEST_APPLICATION_URN)),
            eq(
                ImmutableList.of(
                    UrnUtils.getUrn(TEST_ENTITY_URN_1), UrnUtils.getUrn(TEST_ENTITY_URN_2))),
            eq(UrnUtils.getUrn(TEST_ACTOR_URN)));
  }

  @Test
  public void testGetFailureServiceThrowsException() {
    mockExists(UrnUtils.getUrn(TEST_ENTITY_URN_1), true);
    mockExists(UrnUtils.getUrn(TEST_APPLICATION_URN), true);

    Mockito.doThrow(new RuntimeException("Service error"))
        .when(mockApplicationService)
        .batchUnsetApplication(any(), any(), any(), any());

    BatchUnsetApplicationInput input =
        new BatchUnsetApplicationInput(TEST_APPLICATION_URN, ImmutableList.of(TEST_ENTITY_URN_1));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockApplicationService, Mockito.times(1))
        .batchUnsetApplication(any(), any(), any(), any());
  }
}
