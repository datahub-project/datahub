package com.linkedin.datahub.graphql.resolvers.ownership;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.*;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.service.OwnershipTypeService;
import com.linkedin.ownership.OwnershipTypeInfo;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class DeleteOwnershipTypeResolverTest {

  private static final Urn TEST_URN =
      Urn.createFromTuple(Constants.OWNERSHIP_TYPE_ENTITY_NAME, "test");
  private static final Urn TEST_AUTHORIZED_USER = UrnUtils.getUrn("urn:li:corpuser:auth");
  private static final Urn TEST_UNAUTHORIZED_USER = UrnUtils.getUrn("urn:li:corpuser:no-auth");

  @Test
  public void testGetSuccessOwnershipTypeCanManage() throws Exception {
    OwnershipTypeService mockService = initOwnershipTypeService();
    DeleteOwnershipTypeResolver resolver = new DeleteOwnershipTypeResolver(mockService);

    // Execute resolver - user is allowed since they have the Manage Global OwnershipType priv
    // even though they are not the owner.
    QueryContext mockContext = getMockAllowContext(TEST_UNAUTHORIZED_USER.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_URN.toString());
    Mockito.when(mockEnv.getArgument(Mockito.eq("deleteReferences"))).thenReturn(true);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());

    Mockito.verify(mockService, Mockito.times(1))
        .deleteOwnershipType(Mockito.eq(TEST_URN), anyBoolean(), Mockito.any(Authentication.class));
  }

  @Test
  public void testGetFailureOwnershipTypeCanNotManager() throws Exception {
    OwnershipTypeService mockService = initOwnershipTypeService();
    DeleteOwnershipTypeResolver resolver = new DeleteOwnershipTypeResolver(mockService);

    // Execute resolver - user is allowed since he owns the thing.
    QueryContext mockContext = getMockDenyContext(TEST_UNAUTHORIZED_USER.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_URN.toString());
    Mockito.when(mockEnv.getArgument(Mockito.eq("deleteReferences"))).thenReturn(true);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(AuthorizationException.class, () -> resolver.get(mockEnv).get());

    Mockito.verify(mockService, Mockito.times(0))
        .deleteOwnershipType(Mockito.eq(TEST_URN), anyBoolean(), Mockito.any(Authentication.class));
  }

  @Test
  public void testGetOwnershipTypeServiceException() throws Exception {
    // Create resolver
    OwnershipTypeService mockService = Mockito.mock(OwnershipTypeService.class);
    Mockito.doThrow(RuntimeException.class)
        .when(mockService)
        .deleteOwnershipType(Mockito.any(), anyBoolean(), Mockito.any(Authentication.class));

    DeleteOwnershipTypeResolver resolver = new DeleteOwnershipTypeResolver(mockService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_URN.toString());
    Mockito.when(mockEnv.getArgument(Mockito.eq("deleteReferences"))).thenReturn(true);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  private static OwnershipTypeService initOwnershipTypeService() {
    OwnershipTypeService mockService = Mockito.mock(OwnershipTypeService.class);

    OwnershipTypeInfo testInfo =
        new OwnershipTypeInfo()
            .setName("test-name")
            .setDescription("test-description")
            .setCreated(new AuditStamp().setActor(TEST_AUTHORIZED_USER).setTime(0L))
            .setLastModified(new AuditStamp().setActor(TEST_AUTHORIZED_USER).setTime(0L));

    Mockito.when(
            mockService.getOwnershipTypeInfo(
                Mockito.eq(TEST_URN), Mockito.any(Authentication.class)))
        .thenReturn(testInfo);

    return mockService;
  }
}
