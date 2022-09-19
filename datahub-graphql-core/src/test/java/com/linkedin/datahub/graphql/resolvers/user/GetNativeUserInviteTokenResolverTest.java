package com.linkedin.datahub.graphql.resolvers.user;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.invite.InviteTokenService;
import com.linkedin.datahub.graphql.QueryContext;
import graphql.schema.DataFetchingEnvironment;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class GetNativeUserInviteTokenResolverTest {

  private static final String INVITE_TOKEN = "inviteToken";

  private InviteTokenService _inviteTokenService;
  private GetNativeUserInviteTokenResolver _resolver;
  private DataFetchingEnvironment _dataFetchingEnvironment;
  private Authentication _authentication;

  @BeforeMethod
  public void setupTest() {
    _inviteTokenService = mock(InviteTokenService.class);
    _dataFetchingEnvironment = mock(DataFetchingEnvironment.class);
    _authentication = mock(Authentication.class);

    _resolver = new GetNativeUserInviteTokenResolver(_inviteTokenService);
  }

  @Test
  public void testFailsDenyContext() {
    QueryContext mockContext = getMockDenyContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    assertThrows(() -> _resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testPasses() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(mockContext.getAuthentication()).thenReturn(_authentication);
    when(_inviteTokenService.getInviteToken(any(), eq(false), eq(_authentication))).thenReturn(INVITE_TOKEN);

    assertEquals(INVITE_TOKEN, _resolver.get(_dataFetchingEnvironment).join().getInviteToken());
  }
}
