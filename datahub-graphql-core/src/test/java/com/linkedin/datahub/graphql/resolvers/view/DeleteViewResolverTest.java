package com.linkedin.datahub.graphql.resolvers.view;

import com.datahub.authentication.Authentication;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.service.ViewService;
import com.linkedin.view.DataHubViewDefinition;
import com.linkedin.view.DataHubViewInfo;
import com.linkedin.view.DataHubViewType;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.testng.Assert.*;


public class DeleteViewResolverTest {

  private static final Urn TEST_URN = UrnUtils.getUrn("urn:li:dataHubView:test-id");
  private static final Urn TEST_AUTHORIZED_USER = UrnUtils.getUrn("urn:li:corpuser:auth");
  private static final Urn TEST_UNAUTHORIZED_USER = UrnUtils.getUrn("urn:li:corpuser:no-auth");

  @Test
  public void testGetSuccessGlobalViewIsCreator() throws Exception {
    ViewService mockService = initViewService(DataHubViewType.GLOBAL);
    DeleteViewResolver resolver = new DeleteViewResolver(mockService);

    // Execute resolver - user is allowed since he owns the thing.
    QueryContext mockContext = getMockDenyContext(TEST_AUTHORIZED_USER.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_URN.toString());
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());

    Mockito.verify(mockService, Mockito.times(1)).deleteView(
        Mockito.eq(TEST_URN),
        Mockito.any(Authentication.class)
    );
  }

  @Test
  public void testGetSuccessGlobalViewCanManager() throws Exception {
    ViewService mockService = initViewService(DataHubViewType.GLOBAL);
    DeleteViewResolver resolver = new DeleteViewResolver(mockService);

    // Execute resolver - user is allowed since they have the Manage Global View priv
    // even though they are not the owner.
    QueryContext mockContext = getMockAllowContext(TEST_UNAUTHORIZED_USER.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_URN.toString());
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());

    Mockito.verify(mockService, Mockito.times(1)).deleteView(
        Mockito.eq(TEST_URN),
        Mockito.any(Authentication.class)
    );
  }

  @Test
  public void testGetFailureGlobalViewIsNotCreatorOrManager() throws Exception {
    ViewService mockService = initViewService(DataHubViewType.GLOBAL);
    DeleteViewResolver resolver = new DeleteViewResolver(mockService);

    // Execute resolver - user is allowed since he owns the thing.
    QueryContext mockContext = getMockDenyContext(TEST_UNAUTHORIZED_USER.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_URN.toString());
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(ExecutionException.class, () -> resolver.get(mockEnv).get());

    Mockito.verify(mockService, Mockito.times(0)).deleteView(
        Mockito.eq(TEST_URN),
        Mockito.any(Authentication.class)
    );
  }


  @Test
  public void testGetSuccessPersonalViewIsCreator() throws Exception {
    ViewService mockService = initViewService(DataHubViewType.PERSONAL);
    DeleteViewResolver resolver = new DeleteViewResolver(mockService);

    // Execute resolver - user is allowed since he owns the thing.
    QueryContext mockContext = getMockDenyContext(TEST_AUTHORIZED_USER.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_URN.toString());
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());

    Mockito.verify(mockService, Mockito.times(1)).deleteView(
        Mockito.eq(TEST_URN),
        Mockito.any(Authentication.class)
    );
  }

  @Test
  public void testGetFailurePersonalViewIsNotCreator() throws Exception {
    ViewService mockService = initViewService(DataHubViewType.PERSONAL);
    DeleteViewResolver resolver = new DeleteViewResolver(mockService);

    // Execute resolver - user is allowed since he owns the thing.
    QueryContext mockContext = getMockDenyContext(TEST_UNAUTHORIZED_USER.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_URN.toString());
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(ExecutionException.class, () -> resolver.get(mockEnv).get());

    Mockito.verify(mockService, Mockito.times(0)).deleteView(
        Mockito.eq(TEST_URN),
        Mockito.any(Authentication.class)
    );
  }

  @Test
  public void testGetViewServiceException() throws Exception {
    // Create resolver
    ViewService mockService = Mockito.mock(ViewService.class);
    Mockito.doThrow(RuntimeException.class).when(mockService).deleteView(
        Mockito.any(),
        Mockito.any(Authentication.class));

    DeleteViewResolver resolver = new DeleteViewResolver(mockService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_URN.toString());
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  private static ViewService initViewService(DataHubViewType viewType) {
    ViewService mockService = Mockito.mock(ViewService.class);

    DataHubViewInfo testInfo = new DataHubViewInfo()
        .setType(viewType)
        .setName("test-name")
        .setDescription("test-description")
        .setCreated(new AuditStamp().setActor(TEST_AUTHORIZED_USER).setTime(0L))
        .setLastModified(new AuditStamp().setActor(TEST_AUTHORIZED_USER).setTime(0L))
        .setDefinition(new DataHubViewDefinition().setEntityTypes(new StringArray()).setFilter(new Filter()));

    Mockito.when(mockService.getViewInfo(
        Mockito.eq(TEST_URN),
        Mockito.any(Authentication.class)))
        .thenReturn(testInfo);

    return mockService;
  }
}