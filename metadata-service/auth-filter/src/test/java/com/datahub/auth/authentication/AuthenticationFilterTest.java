package com.datahub.auth.authentication;

import static com.datahub.authentication.AuthenticationConstants.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.datahub.auth.authentication.filter.AuthenticationFilter;
import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationException;
import com.datahub.authentication.AuthenticationExpiredException;
import com.datahub.authentication.authenticator.AuthenticatorChain;
import jakarta.servlet.FilterChain;
import jakarta.servlet.FilterConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.Test;

@ContextConfiguration(classes = {AuthTestConfiguration.class})
public class AuthenticationFilterTest extends AbstractTestNGSpringContextTests {

  @Autowired AuthenticationFilter authenticationFilter;

  @Test
  public void testExpiredToken() throws ServletException, IOException, AuthenticationException {
    FilterConfig mockFilterConfig = mock(FilterConfig.class);
    when(mockFilterConfig.getInitParameterNames()).thenReturn(Collections.emptyEnumeration());

    authenticationFilter.init(mockFilterConfig);
    HttpServletRequest servletRequest = mock(HttpServletRequest.class);
    HttpServletResponse servletResponse = mock(HttpServletResponse.class);
    FilterChain filterChain = mock(FilterChain.class);

    // Setup for expired token test
    when(servletRequest.getHeaderNames())
        .thenReturn(Collections.enumeration(List.of(AUTHORIZATION_HEADER_NAME)));
    when(servletRequest.getHeader(AUTHORIZATION_HEADER_NAME)).thenReturn("Bearer expired-token");

    // Mock the authenticator chain to throw AuthenticationExpiredException
    AuthenticatorChain mockAuthenticatorChain = mock(AuthenticatorChain.class);
    when(mockAuthenticatorChain.authenticate(any(), anyBoolean()))
        .thenThrow(new AuthenticationExpiredException("Token has expired"));

    // Inject the mock authenticator chain
    ReflectionTestUtils.setField(
        authenticationFilter, "authenticatorChain", mockAuthenticatorChain);

    authenticationFilter.doFilter(servletRequest, servletResponse, filterChain);
    verify(servletResponse, times(1))
        .sendError(
            eq(HttpServletResponse.SC_UNAUTHORIZED),
            eq("Unauthorized to perform this action due to expired auth."));
  }

  @Test
  public void testExcludedPaths() throws ServletException {
    // Mock configuration setup
    FilterConfig mockFilterConfig = mock(FilterConfig.class);
    when(mockFilterConfig.getInitParameterNames()).thenReturn(Collections.emptyEnumeration());
    authenticationFilter.init(mockFilterConfig);

    // Test cases for different path patterns
    HttpServletRequest exactPathRequest = mock(HttpServletRequest.class);
    when(exactPathRequest.getServletPath()).thenReturn("/health");

    HttpServletRequest wildcardPathRequest = mock(HttpServletRequest.class);
    when(wildcardPathRequest.getServletPath()).thenReturn("/schema-registry/api/config");

    HttpServletRequest nonExcludedRequest = mock(HttpServletRequest.class);
    when(nonExcludedRequest.getServletPath()).thenReturn("/protected/resource");

    // Set excluded paths in the filter
    ReflectionTestUtils.setField(
        authenticationFilter,
        "excludedPathPatterns",
        new HashSet<>(Arrays.asList("/health", "/schema-registry/*")));

    // Verify exact path match
    assertTrue(
        authenticationFilter.shouldNotFilter(exactPathRequest),
        "Exact path match should be excluded from filtering");

    // Verify wildcard path match
    assertTrue(
        authenticationFilter.shouldNotFilter(wildcardPathRequest),
        "Path matching wildcard pattern should be excluded from filtering");

    // Verify non-excluded path
    assertFalse(
        authenticationFilter.shouldNotFilter(nonExcludedRequest),
        "Non-excluded path should not be excluded from filtering");
  }

  @Test
  public void testAuthenticationException() throws ServletException, IOException {
    // Setup
    FilterConfig mockFilterConfig = mock(FilterConfig.class);
    when(mockFilterConfig.getInitParameterNames()).thenReturn(Collections.emptyEnumeration());
    authenticationFilter.init(mockFilterConfig);

    HttpServletRequest servletRequest = mock(HttpServletRequest.class);
    HttpServletResponse servletResponse = mock(HttpServletResponse.class);
    FilterChain filterChain = mock(FilterChain.class);

    // Mock an invalid token that will trigger AuthenticationException
    when(servletRequest.getHeaderNames())
        .thenReturn(Collections.enumeration(List.of(AUTHORIZATION_HEADER_NAME)));
    when(servletRequest.getHeader(AUTHORIZATION_HEADER_NAME))
        .thenReturn("Bearer invalid-token-format");

    // Execute
    authenticationFilter.doFilter(servletRequest, servletResponse, filterChain);

    // Verify
    verify(servletResponse, times(1))
        .sendError(
            eq(HttpServletResponse.SC_UNAUTHORIZED), eq("Unauthorized to perform this action."));
    verify(filterChain, never()).doFilter(any(), any());
  }

  @Test
  public void testGenericException() throws ServletException, IOException, AuthenticationException {
    // Setup
    FilterConfig mockFilterConfig = mock(FilterConfig.class);
    when(mockFilterConfig.getInitParameterNames()).thenReturn(Collections.emptyEnumeration());
    authenticationFilter.init(mockFilterConfig);

    HttpServletRequest servletRequest = mock(HttpServletRequest.class);
    HttpServletResponse servletResponse = mock(HttpServletResponse.class);
    FilterChain filterChain = mock(FilterChain.class);

    // Mock behavior that will trigger a generic exception
    when(servletRequest.getHeaderNames())
        .thenReturn(Collections.enumeration(List.of(AUTHORIZATION_HEADER_NAME)));
    when(servletRequest.getHeader(AUTHORIZATION_HEADER_NAME)).thenReturn("Bearer some-token");

    // Mock authenticatorChain to throw a generic exception
    AuthenticatorChain mockAuthenticatorChain = mock(AuthenticatorChain.class);
    when(mockAuthenticatorChain.authenticate(any(), anyBoolean()))
        .thenThrow(new RuntimeException("Unexpected error"));

    // Inject mock authenticator chain
    ReflectionTestUtils.setField(
        authenticationFilter, "authenticatorChain", mockAuthenticatorChain);

    // Execute
    authenticationFilter.doFilter(servletRequest, servletResponse, filterChain);

    // Verify
    verify(servletResponse, times(1))
        .sendError(
            eq(HttpServletResponse.SC_UNAUTHORIZED), eq("Unauthorized to perform this action."));
    verify(filterChain, never()).doFilter(any(), any());
  }

  @Test
  public void testSuccessfulAuthentication()
      throws ServletException, IOException, AuthenticationException {
    // Setup
    FilterConfig mockFilterConfig = mock(FilterConfig.class);
    when(mockFilterConfig.getInitParameterNames()).thenReturn(Collections.emptyEnumeration());
    authenticationFilter.init(mockFilterConfig);

    HttpServletRequest servletRequest = mock(HttpServletRequest.class);
    HttpServletResponse servletResponse = mock(HttpServletResponse.class);
    FilterChain filterChain = mock(FilterChain.class);

    // Mock authentication header
    when(servletRequest.getHeaderNames())
        .thenReturn(Collections.enumeration(List.of(AUTHORIZATION_HEADER_NAME)));
    when(servletRequest.getHeader(AUTHORIZATION_HEADER_NAME)).thenReturn("Bearer valid-token");

    // Mock successful authentication
    Actor actor = new Actor(ActorType.USER, "datahub");
    Authentication mockAuthentication = mock(Authentication.class);
    when(mockAuthentication.getActor()).thenReturn(actor);

    AuthenticatorChain mockAuthenticatorChain = mock(AuthenticatorChain.class);
    when(mockAuthenticatorChain.authenticate(any(), anyBoolean())).thenReturn(mockAuthentication);

    // Inject mock authenticator chain
    ReflectionTestUtils.setField(
        authenticationFilter, "authenticatorChain", mockAuthenticatorChain);

    // Execute
    authenticationFilter.doFilter(servletRequest, servletResponse, filterChain);

    // Verify
    verify(filterChain, times(1)).doFilter(servletRequest, servletResponse);
    verify(servletResponse, never()).sendError(anyInt(), anyString());
  }
}
