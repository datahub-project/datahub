package com.linkedin.datahub.graphql.resolvers.ownership;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.CreateOwnershipTypeInput;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.OwnershipTypeEntity;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.service.OwnershipTypeService;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class CreateOwnershipTypeResolverTest {

  private static final CreateOwnershipTypeInput TEST_INPUT =
      new CreateOwnershipTypeInput(
          "Custom ownership", "A custom ownership description for testing purposes");

  private static final Urn TEST_OWNERSHIP_TYPE_URN =
      Urn.createFromTuple(Constants.OWNERSHIP_TYPE_ENTITY_NAME, "test");

  @Test
  public void testCreateSuccess() throws Exception {
    // Create resolver
    OwnershipTypeService mockService = initMockService();
    CreateOwnershipTypeResolver resolver = new CreateOwnershipTypeResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    OwnershipTypeEntity ownershipType = resolver.get(mockEnv).get();
    assertEquals(ownershipType.getInfo().getName(), TEST_INPUT.getName());
    assertEquals(ownershipType.getInfo().getDescription(), TEST_INPUT.getDescription());
    assertEquals(ownershipType.getType(), EntityType.CUSTOM_OWNERSHIP_TYPE);

    Mockito.verify(mockService, Mockito.times(1))
        .createOwnershipType(
            any(),
            Mockito.eq(TEST_INPUT.getName()),
            Mockito.eq(TEST_INPUT.getDescription()),
            Mockito.anyLong());
  }

  @Test
  public void testCreateUnauthorized() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    OwnershipTypeService mockService = Mockito.mock(OwnershipTypeService.class);
    CreateOwnershipTypeResolver resolver = new CreateOwnershipTypeResolver(mockService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(AuthorizationException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockClient, Mockito.times(0)).ingestProposal(any(), Mockito.any(), anyBoolean());
  }

  @Test
  public void testCreateOwnershipTypeServiceException() throws Exception {
    // Create resolver
    OwnershipTypeService mockService = Mockito.mock(OwnershipTypeService.class);
    Mockito.doThrow(RuntimeException.class)
        .when(mockService)
        .createOwnershipType(any(), Mockito.any(), Mockito.any(), Mockito.anyLong());

    CreateOwnershipTypeResolver resolver = new CreateOwnershipTypeResolver(mockService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  private OwnershipTypeService initMockService() {
    OwnershipTypeService service = Mockito.mock(OwnershipTypeService.class);
    Mockito.when(
            service.createOwnershipType(
                any(),
                Mockito.eq(TEST_INPUT.getName()),
                Mockito.eq(TEST_INPUT.getDescription()),
                Mockito.anyLong()))
        .thenReturn(TEST_OWNERSHIP_TYPE_URN);
    return service;
  }
}
