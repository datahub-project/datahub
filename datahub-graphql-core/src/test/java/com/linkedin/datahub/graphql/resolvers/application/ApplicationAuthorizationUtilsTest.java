package com.linkedin.datahub.graphql.resolvers.application;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.metadata.service.ApplicationService;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ApplicationAuthorizationUtilsTest {

  private static final String TEST_ENTITY_URN_1 =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my-test,PROD)";
  private static final String TEST_ENTITY_URN_2 =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my-test-2,PROD)";
  private static final String TEST_ACTOR_URN = "urn:li:corpuser:test";

  private ApplicationService mockApplicationService;
  private QueryContext mockAllowContext;
  private QueryContext mockDenyContext;
  private final Set<Urn> existingUrns = new HashSet<>();

  @BeforeMethod
  public void setupTest() {
    mockApplicationService = Mockito.mock(ApplicationService.class);
    mockAllowContext = getMockAllowContext(TEST_ACTOR_URN);
    mockDenyContext = getMockDenyContext(TEST_ACTOR_URN);
    existingUrns.clear();
    Mockito.when(mockApplicationService.filterExistingUrns(Mockito.any(), Mockito.anyCollection()))
        .thenAnswer(
            invocation -> {
              Collection<Urn> requestedUrns = invocation.getArgument(1);
              return requestedUrns.stream()
                  .filter(existingUrns::contains)
                  .collect(Collectors.toSet());
            });
  }

  private void mockExists(Urn urn, boolean exists) {
    if (exists) {
      existingUrns.add(urn);
    } else {
      existingUrns.remove(urn);
    }
  }

  @Test
  public void testVerifyEditApplicationsAuthorizationFailure() {
    Urn resourceUrn = UrnUtils.getUrn(TEST_ENTITY_URN_1);

    assertThrows(
        AuthorizationException.class,
        () ->
            ApplicationAuthorizationUtils.verifyEditApplicationsAuthorization(
                resourceUrn, mockDenyContext));
  }

  @Test
  public void testVerifyResourcesExistAndAuthorizedSuccess() {
    mockExists(UrnUtils.getUrn(TEST_ENTITY_URN_1), true);
    mockExists(UrnUtils.getUrn(TEST_ENTITY_URN_2), true);

    ApplicationAuthorizationUtils.verifyResourcesExistAndAuthorized(
        ImmutableList.of(TEST_ENTITY_URN_1, TEST_ENTITY_URN_2),
        mockApplicationService,
        mockAllowContext,
        "test operation");

    Mockito.verify(mockApplicationService, Mockito.times(1))
        .filterExistingUrns(
            Mockito.any(),
            Mockito.eq(
                ImmutableList.of(
                    UrnUtils.getUrn(TEST_ENTITY_URN_1), UrnUtils.getUrn(TEST_ENTITY_URN_2))));
  }

  @Test
  public void testVerifyResourcesExistAndAuthorizedResourceDoesNotExist() {
    mockExists(UrnUtils.getUrn(TEST_ENTITY_URN_1), false);

    assertThrows(
        RuntimeException.class,
        () ->
            ApplicationAuthorizationUtils.verifyResourcesExistAndAuthorized(
                ImmutableList.of(TEST_ENTITY_URN_1),
                mockApplicationService,
                mockAllowContext,
                "test operation"));
  }

  @Test
  public void testVerifyResourcesExistAndAuthorizedMultipleResourcesOneDoesNotExist() {
    mockExists(UrnUtils.getUrn(TEST_ENTITY_URN_1), true);
    mockExists(UrnUtils.getUrn(TEST_ENTITY_URN_2), false);

    assertThrows(
        RuntimeException.class,
        () ->
            ApplicationAuthorizationUtils.verifyResourcesExistAndAuthorized(
                ImmutableList.of(TEST_ENTITY_URN_1, TEST_ENTITY_URN_2),
                mockApplicationService,
                mockAllowContext,
                "test operation"));
  }

  @Test
  public void testVerifyResourcesExistAndAuthorizedMultipleResourcesUnauthorized() {
    mockExists(UrnUtils.getUrn(TEST_ENTITY_URN_1), true);

    assertThrows(
        AuthorizationException.class,
        () ->
            ApplicationAuthorizationUtils.verifyResourcesExistAndAuthorized(
                ImmutableList.of(TEST_ENTITY_URN_1, TEST_ENTITY_URN_2),
                mockApplicationService,
                mockDenyContext,
                "test operation"));
  }
}
