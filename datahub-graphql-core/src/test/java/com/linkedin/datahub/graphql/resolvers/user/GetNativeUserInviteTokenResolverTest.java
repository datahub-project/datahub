package com.linkedin.datahub.graphql.resolvers.user;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.user.NativeUserService;
import com.linkedin.datahub.graphql.QueryContext;
import graphql.schema.DataFetchingEnvironment;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class GetNativeUserInviteTokenResolverTest {

  private static final String INVITE_TOKEN = "inviteToken";

  private NativeUserService _nativeUserService;
  private GetNativeUserInviteTokenResolver _resolver;
  private DataFetchingEnvironment _dataFetchingEnvironment;
  private Authentication _authentication;

  @BeforeMethod
  public void setupTest() {
    _nativeUserService = mock(NativeUserService.class);
    _dataFetchingEnvironment = mock(DataFetchingEnvironment.class);
    _authentication = mock(Authentication.class);

    _resolver = new GetNativeUserInviteTokenResolver(_nativeUserService);
  }

  @Test
  public void testFailsCannotManageUserCredentials() {
    QueryContext mockContext = getMockDenyContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    assertThrows(() -> _resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testPasses() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(mockContext.getAuthentication()).thenReturn(_authentication);
    when(_nativeUserService.getNativeUserInviteToken(any())).thenReturn(INVITE_TOKEN);

    assertEquals(INVITE_TOKEN, _resolver.get(_dataFetchingEnvironment).join().getInviteToken());
  }
}
