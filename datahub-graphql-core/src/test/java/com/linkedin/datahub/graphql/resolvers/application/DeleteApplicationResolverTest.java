package com.linkedin.datahub.graphql.resolvers.application;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.service.ApplicationService;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DeleteApplicationResolverTest {

  private static final String TEST_APPLICATION_URN_STRING = "urn:li:application:test-app-id";
  private static final Urn TEST_APPLICATION_URN = UrnUtils.getUrn(TEST_APPLICATION_URN_STRING);
  private static final Urn TEST_ACTOR_URN = UrnUtils.getUrn("urn:li:corpuser:test");

  private ApplicationService mockApplicationService;
  private DeleteApplicationResolver resolver;
  private QueryContext mockContext;
  private DataFetchingEnvironment mockEnv;
  private EntityClient mockEntityClient;

  @BeforeMethod
  public void setupTest() {
    mockApplicationService = Mockito.mock(ApplicationService.class);
    mockEntityClient = Mockito.mock(EntityClient.class);
    resolver = new DeleteApplicationResolver(mockEntityClient, mockApplicationService);
    mockContext = getMockAllowContext(TEST_ACTOR_URN.toString());
    mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_APPLICATION_URN_STRING);
  }

  @Test
  public void testGetSuccess() throws Exception {
    // ApplicationService.deleteApplication does not return a value, so no need to mock a return.
    // It will throw an exception if the delete fails or if the app doesn't exist.
    Mockito.when(mockApplicationService.verifyEntityExists(any(), eq(TEST_APPLICATION_URN)))
        .thenReturn(true);
    // The deleteApplication method in the service will handle the asset check internally.
    // For a success case, we assume it passes this internal check.

    assertTrue(resolver.get(mockEnv).get());

    Mockito.verify(mockApplicationService, Mockito.times(1))
        .deleteApplication(any(), eq(TEST_APPLICATION_URN));
  }

  @Test
  public void testGetFailureApplicationHasAssets() throws Exception {
    Mockito.when(mockApplicationService.verifyEntityExists(any(), eq(TEST_APPLICATION_URN)))
        .thenReturn(true);
    // Mock deleteApplication to throw IllegalStateException as the service would if assets exist.
    Mockito.doThrow(new IllegalStateException("Application has assets and cannot be deleted."))
        .when(mockApplicationService)
        .deleteApplication(any(), eq(TEST_APPLICATION_URN));

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    // deleteApplication should be called by the resolver, which then throws the exception.
    Mockito.verify(mockApplicationService, Mockito.times(1))
        .deleteApplication(any(), eq(TEST_APPLICATION_URN));
  }

  @Test
  public void testGetFailureApplicationDoesNotExist() throws Exception {
    Mockito.when(mockApplicationService.verifyEntityExists(any(), eq(TEST_APPLICATION_URN)))
        .thenReturn(false);
    // If it doesn't exist, deleteApplication in the service should throw an error.
    // We can mock deleteApplication to throw an error like the service would.
    Mockito.doThrow(new RuntimeException("Application does not exist"))
        .when(mockApplicationService)
        .deleteApplication(any(), eq(TEST_APPLICATION_URN));

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    // verifyEntityExists would be called by the resolver first.
    Mockito.verify(mockApplicationService, Mockito.times(1))
        .verifyEntityExists(any(), eq(TEST_APPLICATION_URN));
    Mockito.verify(mockApplicationService, Mockito.never())
        .deleteApplication(any(), eq(TEST_APPLICATION_URN));
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    QueryContext mockDenyContext = getMockDenyContext();
    Mockito.when(mockEnv.getContext()).thenReturn(mockDenyContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockApplicationService, Mockito.never()).deleteApplication(any(), any());
  }
}
