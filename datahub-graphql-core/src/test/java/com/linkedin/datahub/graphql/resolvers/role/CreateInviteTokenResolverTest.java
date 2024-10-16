package com.linkedin.datahub.graphql.resolvers.role;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.invite.InviteTokenService;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.CreateInviteTokenInput;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class CreateInviteTokenResolverTest {
  private static final String ROLE_URN_STRING = "urn:li:dataHubRole:Admin";
  private static final String INVITE_TOKEN_STRING = "inviteToken";
  private InviteTokenService _inviteTokenService;
  private CreateInviteTokenResolver _resolver;
  private DataFetchingEnvironment _dataFetchingEnvironment;
  private Authentication _authentication;

  @BeforeMethod
  public void setupTest() throws Exception {
    _inviteTokenService = mock(InviteTokenService.class);
    _dataFetchingEnvironment = mock(DataFetchingEnvironment.class);
    _authentication = mock(Authentication.class);

    _resolver = new CreateInviteTokenResolver(_inviteTokenService);
  }

  @Test
  public void testNotAuthorizedFails() {
    QueryContext mockContext = getMockDenyContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    assertThrows(() -> _resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testPasses() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(mockContext.getAuthentication()).thenReturn(_authentication);
    when(_inviteTokenService.getInviteToken(any(OperationContext.class), any(), eq(true)))
        .thenReturn(INVITE_TOKEN_STRING);

    CreateInviteTokenInput input = new CreateInviteTokenInput();
    input.setRoleUrn(ROLE_URN_STRING);
    when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);

    assertEquals(
        _resolver.get(_dataFetchingEnvironment).join().getInviteToken(), INVITE_TOKEN_STRING);
  }
}
