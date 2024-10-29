package com.linkedin.datahub.graphql.resolvers.ownership;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableMap;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.OwnershipTypeEntity;
import com.linkedin.datahub.graphql.generated.UpdateOwnershipTypeInput;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.AspectType;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.service.OwnershipTypeService;
import com.linkedin.ownership.OwnershipTypeInfo;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class UpdateOwnershipTypeResolverTest {
  private static final Urn TEST_URN =
      Urn.createFromTuple(Constants.OWNERSHIP_TYPE_ENTITY_NAME, "test");
  private static final Urn TEST_AUTHORIZED_USER = UrnUtils.getUrn("urn:li:corpuser:auth");
  private static final Urn TEST_UNAUTHORIZED_USER = UrnUtils.getUrn("urn:li:corpuser:no-auth");

  private static final UpdateOwnershipTypeInput TEST_INPUT =
      new UpdateOwnershipTypeInput(
          "Custom ownership", "A custom ownership description for testing purposes");

  @Test
  public void testUpdateSuccessOwnershipTypeCanManage() throws Exception {
    OwnershipTypeService mockService = initOwnershipTypeService();
    UpdateOwnershipTypeResolver resolver = new UpdateOwnershipTypeResolver(mockService);

    // Execute resolver - user is allowed since he owns the thing.
    QueryContext mockContext = getMockAllowContext(TEST_UNAUTHORIZED_USER.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_URN.toString());
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    OwnershipTypeEntity ownershipType = resolver.get(mockEnv).get();
    assertEquals(ownershipType.getType(), EntityType.CUSTOM_OWNERSHIP_TYPE);
    assertEquals(ownershipType.getInfo().getName(), TEST_INPUT.getName());
    assertEquals(ownershipType.getInfo().getDescription(), TEST_INPUT.getDescription());

    Mockito.verify(mockService, Mockito.times(1))
        .updateOwnershipType(
            any(),
            Mockito.eq(TEST_URN),
            Mockito.eq(TEST_INPUT.getName()),
            Mockito.eq(TEST_INPUT.getDescription()),
            Mockito.anyLong());
  }

  @Test
  public void testUpdateOwnershipTypeServiceException() throws Exception {
    // Update resolver
    OwnershipTypeService mockService = Mockito.mock(OwnershipTypeService.class);
    Mockito.doThrow(RuntimeException.class)
        .when(mockService)
        .updateOwnershipType(
            any(), Mockito.any(Urn.class), Mockito.any(), Mockito.any(), Mockito.anyLong());

    UpdateOwnershipTypeResolver resolver = new UpdateOwnershipTypeResolver(mockService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_URN.toString());
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  @Test
  public void testUpdateUnauthorized() throws Exception {
    // Update resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    OwnershipTypeService mockService = initOwnershipTypeService();
    UpdateOwnershipTypeResolver resolver = new UpdateOwnershipTypeResolver(mockService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockDenyContext(TEST_UNAUTHORIZED_USER.toString());
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_URN.toString());
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(AuthorizationException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockClient, Mockito.times(0)).ingestProposal(any(), Mockito.any(), anyBoolean());
  }

  private static OwnershipTypeService initOwnershipTypeService() {
    OwnershipTypeService mockService = Mockito.mock(OwnershipTypeService.class);

    OwnershipTypeInfo testInfo =
        new OwnershipTypeInfo()
            .setName(TEST_INPUT.getName())
            .setDescription(TEST_INPUT.getDescription())
            .setCreated(new AuditStamp().setActor(TEST_AUTHORIZED_USER).setTime(0L))
            .setLastModified(new AuditStamp().setActor(TEST_AUTHORIZED_USER).setTime(0L));

    EntityResponse testEntityResponse =
        new EntityResponse()
            .setUrn(TEST_URN)
            .setEntityName(Constants.OWNERSHIP_TYPE_ENTITY_NAME)
            .setAspects(
                new EnvelopedAspectMap(
                    ImmutableMap.of(
                        Constants.OWNERSHIP_TYPE_INFO_ASPECT_NAME,
                        new EnvelopedAspect()
                            .setName(Constants.OWNERSHIP_TYPE_INFO_ASPECT_NAME)
                            .setType(AspectType.VERSIONED)
                            .setValue(new Aspect(testInfo.data())))));

    Mockito.when(mockService.getOwnershipTypeInfo(any(), Mockito.eq(TEST_URN)))
        .thenReturn(testInfo);

    Mockito.when(mockService.getOwnershipTypeEntityResponse(any(), Mockito.eq(TEST_URN)))
        .thenReturn(testEntityResponse);

    return mockService;
  }
}
