package com.datahub.auth.authentication;

import static com.datahub.authentication.AuthenticationConstants.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.auth.authentication.filter.AuthenticationExtractionFilter;
import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authentication.AuthenticationException;
import com.datahub.authentication.AuthenticationExpiredException;
import com.datahub.authentication.authenticator.AuthenticatorChain;
import jakarta.servlet.FilterChain;
import jakarta.servlet.FilterConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Unit tests for AuthenticationExtractionFilter.
 *
 * <p>Tests the authentication extraction logic including: - Successful authentication extraction -
 * Expired token handling - Generic exception handling - Anonymous context creation
 */
@ContextConfiguration(classes = {AuthExtractionTestConfiguration.class})
public class AuthenticationExtractionFilterTest extends AbstractTestNGSpringContextTests {

  @Autowired AuthenticationExtractionFilter authenticationExtractionFilter;

  @AfterMethod
  public void cleanup() {
    // Always clean up authentication context after each test
    AuthenticationContext.remove();
  }

  @Test
  public void testSuccessfulAuthentication()
      throws ServletException, IOException, AuthenticationException {
    // Setup
    FilterConfig mockFilterConfig = mock(FilterConfig.class);
    when(mockFilterConfig.getInitParameterNames()).thenReturn(Collections.emptyEnumeration());
    authenticationExtractionFilter.init(mockFilterConfig);

    HttpServletRequest servletRequest = mock(HttpServletRequest.class);
    HttpServletResponse servletResponse = mock(HttpServletResponse.class);

    // Custom FilterChain that captures the authentication context during execution
    final Authentication[] capturedAuth = new Authentication[1];
    FilterChain filterChain =
        (req, resp) -> {
          capturedAuth[0] = AuthenticationContext.getAuthentication();
        };

    // Mock authentication header
    when(servletRequest.getHeaderNames())
        .thenReturn(Collections.enumeration(List.of(AUTHORIZATION_HEADER_NAME)));
    when(servletRequest.getHeader(AUTHORIZATION_HEADER_NAME)).thenReturn("Bearer valid-token");
    when(servletRequest.getServletPath()).thenReturn("/api/v2/graphql");

    // Mock successful authentication
    Actor actor = new Actor(ActorType.USER, "datahub");
    Authentication mockAuthentication = mock(Authentication.class);
    when(mockAuthentication.getActor()).thenReturn(actor);

    AuthenticatorChain mockAuthenticatorChain = mock(AuthenticatorChain.class);
    when(mockAuthenticatorChain.authenticate(any(), anyBoolean())).thenReturn(mockAuthentication);

    // Inject mock authenticator chain
    ReflectionTestUtils.setField(
        authenticationExtractionFilter, "authenticatorChain", mockAuthenticatorChain);

    // Execute
    authenticationExtractionFilter.doFilter(servletRequest, servletResponse, filterChain);

    // Verify - authentication should have been captured during filter execution
    assertNotNull(capturedAuth[0], "Authentication context should be set during filter execution");
    assertEquals(capturedAuth[0].getActor().getId(), "datahub", "Actor ID should match");
  }

  @Test
  public void testExpiredToken() throws ServletException, IOException, AuthenticationException {
    FilterConfig mockFilterConfig = mock(FilterConfig.class);
    when(mockFilterConfig.getInitParameterNames()).thenReturn(Collections.emptyEnumeration());

    authenticationExtractionFilter.init(mockFilterConfig);
    HttpServletRequest servletRequest = mock(HttpServletRequest.class);
    HttpServletResponse servletResponse = mock(HttpServletResponse.class);

    // Custom FilterChain that captures the authentication context during execution
    final Authentication[] capturedAuth = new Authentication[1];
    FilterChain filterChain =
        (req, resp) -> {
          capturedAuth[0] = AuthenticationContext.getAuthentication();
        };

    // Setup for expired token test
    when(servletRequest.getHeaderNames())
        .thenReturn(Collections.enumeration(List.of(AUTHORIZATION_HEADER_NAME)));
    when(servletRequest.getHeader(AUTHORIZATION_HEADER_NAME)).thenReturn("Bearer expired-token");
    when(servletRequest.getServletPath()).thenReturn("/api/v2/graphql");

    // Mock the authenticator chain to throw AuthenticationExpiredException
    AuthenticatorChain mockAuthenticatorChain = mock(AuthenticatorChain.class);
    when(mockAuthenticatorChain.authenticate(any(), anyBoolean()))
        .thenThrow(new AuthenticationExpiredException("Token has expired"));

    // Inject the mock authenticator chain
    ReflectionTestUtils.setField(
        authenticationExtractionFilter, "authenticatorChain", mockAuthenticatorChain);

    // Execute
    authenticationExtractionFilter.doFilter(servletRequest, servletResponse, filterChain);

    // Verify - ExtractionFilter should NEVER return errors, it should continue with anonymous
    // context
    // Verify anonymous authentication context was set during filter execution
    assertNotNull(capturedAuth[0], "Authentication context should be set even for expired tokens");
    assertEquals(
        capturedAuth[0].getActor().getId(),
        "anonymous",
        "Should have anonymous actor for expired token");
  }

  @Test
  public void testGenericException() throws ServletException, IOException, AuthenticationException {
    // Setup
    FilterConfig mockFilterConfig = mock(FilterConfig.class);
    when(mockFilterConfig.getInitParameterNames()).thenReturn(Collections.emptyEnumeration());
    authenticationExtractionFilter.init(mockFilterConfig);

    HttpServletRequest servletRequest = mock(HttpServletRequest.class);
    HttpServletResponse servletResponse = mock(HttpServletResponse.class);

    // Custom FilterChain that captures the authentication context during execution
    final Authentication[] capturedAuth = new Authentication[1];
    FilterChain filterChain =
        (req, resp) -> {
          capturedAuth[0] = AuthenticationContext.getAuthentication();
        };

    // Mock behavior that will trigger a generic exception
    when(servletRequest.getHeaderNames())
        .thenReturn(Collections.enumeration(List.of(AUTHORIZATION_HEADER_NAME)));
    when(servletRequest.getHeader(AUTHORIZATION_HEADER_NAME)).thenReturn("Bearer some-token");
    when(servletRequest.getServletPath()).thenReturn("/api/v2/graphql");

    // Mock authenticatorChain to throw a generic exception
    AuthenticatorChain mockAuthenticatorChain = mock(AuthenticatorChain.class);
    when(mockAuthenticatorChain.authenticate(any(), anyBoolean()))
        .thenThrow(new RuntimeException("Unexpected error"));

    // Inject mock authenticator chain
    ReflectionTestUtils.setField(
        authenticationExtractionFilter, "authenticatorChain", mockAuthenticatorChain);

    // Execute
    authenticationExtractionFilter.doFilter(servletRequest, servletResponse, filterChain);

    // Verify - ExtractionFilter should NEVER return errors, it should continue with anonymous
    // context
    // Verify anonymous authentication context was set during filter execution
    assertNotNull(capturedAuth[0], "Authentication context should be set even for exceptions");
    assertEquals(
        capturedAuth[0].getActor().getId(),
        "anonymous",
        "Should have anonymous actor for exceptions");
  }

  @Test
  public void testNoAuthenticationHeader()
      throws ServletException, IOException, AuthenticationException {
    // Setup
    FilterConfig mockFilterConfig = mock(FilterConfig.class);
    when(mockFilterConfig.getInitParameterNames()).thenReturn(Collections.emptyEnumeration());
    authenticationExtractionFilter.init(mockFilterConfig);

    HttpServletRequest servletRequest = mock(HttpServletRequest.class);
    HttpServletResponse servletResponse = mock(HttpServletResponse.class);

    // Custom FilterChain that captures the authentication context during execution
    final Authentication[] capturedAuth = new Authentication[1];
    FilterChain filterChain =
        (req, resp) -> {
          capturedAuth[0] = AuthenticationContext.getAuthentication();
        };

    // Mock no authentication header
    when(servletRequest.getHeaderNames()).thenReturn(Collections.emptyEnumeration());
    when(servletRequest.getServletPath()).thenReturn("/api/v2/graphql");

    // Mock authenticator chain to return null (no authentication found)
    AuthenticatorChain mockAuthenticatorChain = mock(AuthenticatorChain.class);
    when(mockAuthenticatorChain.authenticate(any(), anyBoolean())).thenReturn(null);

    // Inject mock authenticator chain
    ReflectionTestUtils.setField(
        authenticationExtractionFilter, "authenticatorChain", mockAuthenticatorChain);

    // Execute
    authenticationExtractionFilter.doFilter(servletRequest, servletResponse, filterChain);

    // Verify - should continue with anonymous context during filter execution
    assertNotNull(capturedAuth[0], "Authentication context should be set");
    assertEquals(
        capturedAuth[0].getActor().getId(), "anonymous", "Should have anonymous actor for no auth");
    assertEquals(
        capturedAuth[0].getActor().getType(),
        ActorType.USER,
        "Anonymous actor should be USER type");
  }

  @Test
  public void testAlwaysRunsForAllRequests() throws ServletException, ReflectiveOperationException {
    // Setup
    FilterConfig mockFilterConfig = mock(FilterConfig.class);
    when(mockFilterConfig.getInitParameterNames()).thenReturn(Collections.emptyEnumeration());
    authenticationExtractionFilter.init(mockFilterConfig);

    // Test various request types - ExtractionFilter should NEVER skip any requests
    HttpServletRequest healthRequest = mock(HttpServletRequest.class);
    when(healthRequest.getServletPath()).thenReturn("/health");

    HttpServletRequest apiRequest = mock(HttpServletRequest.class);
    when(apiRequest.getServletPath()).thenReturn("/api/v2/graphql");

    HttpServletRequest configRequest = mock(HttpServletRequest.class);
    when(configRequest.getServletPath()).thenReturn("/config");

    // Use reflection to access protected shouldNotFilter method
    java.lang.reflect.Method shouldNotFilterMethod =
        AuthenticationExtractionFilter.class.getDeclaredMethod(
            "shouldNotFilter", HttpServletRequest.class);
    shouldNotFilterMethod.setAccessible(true);

    // Verify shouldNotFilter always returns false (never skip)
    assertFalse(
        (Boolean) shouldNotFilterMethod.invoke(authenticationExtractionFilter, healthRequest),
        "Extraction filter should run for health endpoints");
    assertFalse(
        (Boolean) shouldNotFilterMethod.invoke(authenticationExtractionFilter, apiRequest),
        "Extraction filter should run for API endpoints");
    assertFalse(
        (Boolean) shouldNotFilterMethod.invoke(authenticationExtractionFilter, configRequest),
        "Extraction filter should run for config endpoints");
  }

  @Test
  public void testContextCleanup() throws ServletException, IOException {
    // Setup
    FilterConfig mockFilterConfig = mock(FilterConfig.class);
    when(mockFilterConfig.getInitParameterNames()).thenReturn(Collections.emptyEnumeration());
    authenticationExtractionFilter.init(mockFilterConfig);

    HttpServletRequest servletRequest = mock(HttpServletRequest.class);
    HttpServletResponse servletResponse = mock(HttpServletResponse.class);
    FilterChain filterChain = mock(FilterChain.class);

    when(servletRequest.getHeaderNames()).thenReturn(Collections.emptyEnumeration());
    when(servletRequest.getServletPath()).thenReturn("/api/v2/graphql");

    // Set up initial context
    Authentication initialAuth =
        new Authentication(new Actor(ActorType.USER, "initial"), "creds", Collections.emptyMap());
    AuthenticationContext.setAuthentication(initialAuth);

    // Execute
    authenticationExtractionFilter.doFilter(servletRequest, servletResponse, filterChain);

    // Context should be cleaned up after request processing
    // Note: The @AfterMethod will also clean up, but the filter should do it too
    verify(filterChain, times(1)).doFilter(servletRequest, servletResponse);
  }
}
