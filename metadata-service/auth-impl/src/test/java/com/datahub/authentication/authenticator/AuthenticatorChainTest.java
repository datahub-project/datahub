package com.datahub.authentication.authenticator;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationException;
import com.datahub.authentication.AuthenticationExpiredException;
import com.datahub.authentication.AuthenticationRequest;
import com.datahub.authentication.AuthenticationUnavailableException;
import com.datahub.plugins.auth.authentication.Authenticator;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class AuthenticatorChainTest {

  @Test
  public void testAuthenticateSuccess() throws Exception {
    final AuthenticatorChain authenticatorChain = new AuthenticatorChain();
    final Authenticator mockAuthenticator1 = Mockito.mock(Authenticator.class);
    final Authenticator mockAuthenticator2 = Mockito.mock(Authenticator.class);

    final Authentication mockAuthentication = Mockito.mock(Authentication.class);
    Mockito.when(mockAuthenticator1.authenticate(Mockito.any())).thenReturn(mockAuthentication);
    Mockito.when(mockAuthenticator2.authenticate(Mockito.any()))
        .thenThrow(new AuthenticationException("Failed to authenticate"));

    authenticatorChain.register(mockAuthenticator1);
    authenticatorChain.register(mockAuthenticator2);

    // Verify that the mock authentication is returned on Authenticate.
    final AuthenticationRequest mockContext = Mockito.mock(AuthenticationRequest.class);

    Authentication result = authenticatorChain.authenticate(mockContext, false);

    // Verify that the authentication matches the mock returned by authenticator1
    assertSame(result, mockAuthentication);

    // Verify that authenticator2 has not been invoked (short circuit)
    verify(mockAuthenticator2, times(0)).authenticate(any());
  }

  @Test
  public void testAuthenticateFailure() throws Exception {
    final AuthenticatorChain authenticatorChain = new AuthenticatorChain();
    final Authenticator mockAuthenticator = Mockito.mock(Authenticator.class);
    Mockito.when(mockAuthenticator.authenticate(Mockito.any()))
        .thenThrow(new AuthenticationException("Failed to authenticate"));

    authenticatorChain.register(mockAuthenticator);

    // Verify that the mock authentication is returned on Authenticate.
    final AuthenticationRequest mockContext = Mockito.mock(AuthenticationRequest.class);

    Authentication result = authenticatorChain.authenticate(mockContext, false);

    // If the authenticator throws, verify that null is returned to indicate failure to
    // authenticate.
    assertNull(result);
  }

  @Test
  public void testAuthenticateUnavailableThrowsAfterChain() throws Exception {
    final AuthenticatorChain authenticatorChain = new AuthenticatorChain();
    final Authenticator unavailableAuthenticator = Mockito.mock(Authenticator.class);
    final Authenticator failingAuthenticator = Mockito.mock(Authenticator.class);
    Mockito.when(unavailableAuthenticator.authenticate(Mockito.any()))
        .thenThrow(new AuthenticationUnavailableException("store down", null));
    Mockito.when(failingAuthenticator.authenticate(Mockito.any()))
        .thenThrow(new AuthenticationException("Failed to authenticate"));
    authenticatorChain.register(unavailableAuthenticator);
    authenticatorChain.register(failingAuthenticator);

    final AuthenticationRequest mockContext = Mockito.mock(AuthenticationRequest.class);

    assertThrows(
        AuthenticationUnavailableException.class,
        () -> authenticatorChain.authenticate(mockContext, false));
    // The rest of the chain still ran.
    verify(failingAuthenticator, times(1)).authenticate(any());
  }

  @Test
  public void testAuthenticateUnavailableThenSuccess() throws Exception {
    final AuthenticatorChain authenticatorChain = new AuthenticatorChain();
    final Authenticator unavailableAuthenticator = Mockito.mock(Authenticator.class);
    final Authenticator successAuthenticator = Mockito.mock(Authenticator.class);
    final Authentication mockAuthentication = Mockito.mock(Authentication.class);
    Mockito.when(unavailableAuthenticator.authenticate(Mockito.any()))
        .thenThrow(new AuthenticationUnavailableException("store down", null));
    Mockito.when(successAuthenticator.authenticate(Mockito.any())).thenReturn(mockAuthentication);
    authenticatorChain.register(unavailableAuthenticator);
    authenticatorChain.register(successAuthenticator);

    final AuthenticationRequest mockContext = Mockito.mock(AuthenticationRequest.class);

    assertSame(authenticatorChain.authenticate(mockContext, false), mockAuthentication);
  }

  @Test
  public void testAuthenticateThrows() throws Exception {
    final AuthenticatorChain authenticatorChain = new AuthenticatorChain();
    final Authenticator mockAuthenticator = Mockito.mock(Authenticator.class);
    Mockito.when(mockAuthenticator.authenticate(Mockito.any()))
        .thenThrow(new AuthenticationExpiredException("Failed to authenticate, token has expired"));

    authenticatorChain.register(mockAuthenticator);

    // Verify that the mock authentication is returned on Authenticate.
    final AuthenticationRequest mockContext = Mockito.mock(AuthenticationRequest.class);

    assertThrows(
        AuthenticationExpiredException.class,
        () -> authenticatorChain.authenticate(mockContext, false));
  }
}
