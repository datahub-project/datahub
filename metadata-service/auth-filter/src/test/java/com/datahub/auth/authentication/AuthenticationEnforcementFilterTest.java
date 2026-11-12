package com.datahub.auth.authentication;

import static com.linkedin.metadata.Constants.ANONYMOUS_ACTOR_ID;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.datahub.auth.authentication.filter.AuthenticationEnforcementFilter;
import com.datahub.auth.authentication.filter.AuthenticationExtractionFilter;
import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import jakarta.servlet.FilterChain;
import jakarta.servlet.FilterConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Random;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@ContextConfiguration(classes = {AuthTestConfiguration.class})
public class AuthenticationEnforcementFilterTest extends AbstractTestNGSpringContextTests {

  @Autowired AuthenticationEnforcementFilter authenticationEnforcementFilter;

  @AfterMethod
  public void cleanup() {
    // Always clean up authentication context after each test
    AuthenticationContext.remove();
  }

  @Test
  public void testEnforcementWithAuthenticatedUser() throws ServletException, IOException {
    // Setup
    FilterConfig mockFilterConfig = mock(FilterConfig.class);
    when(mockFilterConfig.getInitParameterNames()).thenReturn(Collections.emptyEnumeration());
    authenticationEnforcementFilter.init(mockFilterConfig);

    HttpServletRequest servletRequest = mock(HttpServletRequest.class);
    HttpServletResponse servletResponse = mock(HttpServletResponse.class);
    FilterChain filterChain = mock(FilterChain.class);

    // Mock a protected path (not excluded)
    when(servletRequest.getServletPath()).thenReturn("/api/v2/graphql");

    // Set up authenticated user context (simulating what AuthenticationExtractionFilter would do)
    Actor actor = new Actor(ActorType.USER, "datahub");
    Authentication authentication =
        new Authentication(actor, "credentials", Collections.emptyMap());
    AuthenticationContext.setAuthentication(authentication);

    // Execute
    authenticationEnforcementFilter.doFilter(servletRequest, servletResponse, filterChain);

    // Verify - should proceed with authenticated user
    verify(filterChain, times(1)).doFilter(servletRequest, servletResponse);
    verify(servletResponse, never()).sendError(anyInt(), anyString());
  }

  @Test
  public void testExcludedPaths() throws ServletException {
    // Mock configuration setup
    FilterConfig mockFilterConfig = mock(FilterConfig.class);
    when(mockFilterConfig.getInitParameterNames()).thenReturn(Collections.emptyEnumeration());
    authenticationEnforcementFilter.init(mockFilterConfig);

    // Test cases for different path patterns
    HttpServletRequest exactPathRequest = mock(HttpServletRequest.class);
    when(exactPathRequest.getServletPath()).thenReturn("/health");

    HttpServletRequest livenessPathRequest = mock(HttpServletRequest.class);
    when(livenessPathRequest.getServletPath()).thenReturn("/health/live");

    HttpServletRequest wildcardPathRequest = mock(HttpServletRequest.class);
    when(wildcardPathRequest.getServletPath()).thenReturn("/schema-registry/api/config");

    HttpServletRequest nonExcludedRequest = mock(HttpServletRequest.class);
    when(nonExcludedRequest.getServletPath()).thenReturn("/protected/resource");

    // Set excluded paths in the filter
    ReflectionTestUtils.setField(
        authenticationEnforcementFilter,
        "excludedPathPatterns",
        new HashSet<>(Arrays.asList("/health", "/health/live", "/schema-registry/*")));

    // Verify exact path match
    assertTrue(
        authenticationEnforcementFilter.shouldNotFilter(exactPathRequest),
        "Exact path match should be excluded from filtering");

    // Verify liveness endpoint is excluded
    assertTrue(
        authenticationEnforcementFilter.shouldNotFilter(livenessPathRequest),
        "Liveness endpoint should be excluded from filtering");

    // Verify wildcard path match
    assertTrue(
        authenticationEnforcementFilter.shouldNotFilter(wildcardPathRequest),
        "Path matching wildcard pattern should be excluded from filtering");

    // Verify non-excluded path
    assertFalse(
        authenticationEnforcementFilter.shouldNotFilter(nonExcludedRequest),
        "Non-excluded path should not be excluded from filtering");
  }

  @Test
  public void testEnforcementWithAnonymousUser() throws ServletException, IOException {
    // Setup
    FilterConfig mockFilterConfig = mock(FilterConfig.class);
    when(mockFilterConfig.getInitParameterNames()).thenReturn(Collections.emptyEnumeration());
    authenticationEnforcementFilter.init(mockFilterConfig);

    HttpServletRequest servletRequest = mock(HttpServletRequest.class);
    HttpServletResponse servletResponse = mock(HttpServletResponse.class);
    FilterChain filterChain = mock(FilterChain.class);

    // Mock a protected path (not excluded)
    when(servletRequest.getServletPath()).thenReturn("/api/v2/graphql");

    // Set up anonymous user context (simulating what AuthenticationExtractionFilter would do)
    Actor anonymousActor = new Actor(ActorType.USER, ANONYMOUS_ACTOR_ID);
    Authentication authentication = new Authentication(anonymousActor, "", Collections.emptyMap());
    AuthenticationContext.setAuthentication(authentication);

    // Execute
    authenticationEnforcementFilter.doFilter(servletRequest, servletResponse, filterChain);

    // Verify - should return 401 for anonymous user on protected path
    verify(servletResponse, times(1))
        .sendError(
            eq(HttpServletResponse.SC_UNAUTHORIZED), eq("Unauthorized to perform this action."));
    verify(filterChain, never()).doFilter(any(), any());
  }

  @Test
  public void testEnforcementWhenAuthenticationUnavailableReturns503()
      throws ServletException, IOException {
    FilterConfig mockFilterConfig = mock(FilterConfig.class);
    when(mockFilterConfig.getInitParameterNames()).thenReturn(Collections.emptyEnumeration());
    authenticationEnforcementFilter.init(mockFilterConfig);

    HttpServletRequest servletRequest = mock(HttpServletRequest.class);
    HttpServletResponse servletResponse = mock(HttpServletResponse.class);
    FilterChain filterChain = mock(FilterChain.class);

    when(servletRequest.getServletPath()).thenReturn("/api/v2/graphql");
    // Extraction filter could not check the credentials (e.g. token store unreachable).
    when(servletRequest.getAttribute(
            AuthenticationExtractionFilter.AUTHENTICATION_UNAVAILABLE_ATTRIBUTE))
        .thenReturn(Boolean.TRUE);
    Actor anonymousActor = new Actor(ActorType.USER, ANONYMOUS_ACTOR_ID);
    AuthenticationContext.setAuthentication(
        new Authentication(anonymousActor, "", Collections.emptyMap()));

    authenticationEnforcementFilter.doFilter(servletRequest, servletResponse, filterChain);

    // Defaults: 3s base plus up to 50% (ceil 2s) jitter.
    final ArgumentCaptor<String> retryAfter = ArgumentCaptor.forClass(String.class);
    verify(servletResponse, times(1)).setHeader(eq("Retry-After"), retryAfter.capture());
    final int seconds = Integer.parseInt(retryAfter.getValue());
    assertTrue(seconds >= 3 && seconds <= 5, "Retry-After was " + seconds);
    verify(servletResponse, times(1))
        .sendError(eq(HttpServletResponse.SC_SERVICE_UNAVAILABLE), anyString());
    verify(servletResponse, never()).sendError(eq(HttpServletResponse.SC_UNAUTHORIZED), any());
    verify(filterChain, never()).doFilter(any(), any());
  }

  @Test
  public void testRetryAfterJitterStaysInRange() {
    final Random random = new Random(42);
    for (int i = 0; i < 200; i++) {
      final int seconds = AuthenticationEnforcementFilter.retryAfterSeconds(3, 50, random);
      assertTrue(seconds >= 3 && seconds <= 5, "Retry-After was " + seconds);
    }
    assertEquals(AuthenticationEnforcementFilter.retryAfterSeconds(3, 0, random), 3);
    final int one = AuthenticationEnforcementFilter.retryAfterSeconds(1, 100, new Random(0));
    assertTrue(one >= 1 && one <= 2, "Retry-After was " + one);
  }

  @Test
  public void testExcludedPathIgnoresAuthenticationUnavailable()
      throws ServletException, IOException {
    FilterConfig mockFilterConfig = mock(FilterConfig.class);
    when(mockFilterConfig.getInitParameterNames()).thenReturn(Collections.emptyEnumeration());
    authenticationEnforcementFilter.init(mockFilterConfig);
    ReflectionTestUtils.setField(
        authenticationEnforcementFilter,
        "excludedPathPatterns",
        new HashSet<>(Collections.singletonList("/health")));

    HttpServletRequest servletRequest = mock(HttpServletRequest.class);
    HttpServletResponse servletResponse = mock(HttpServletResponse.class);
    FilterChain filterChain = mock(FilterChain.class);

    when(servletRequest.getServletPath()).thenReturn("/health");
    when(servletRequest.getAttribute(
            AuthenticationExtractionFilter.AUTHENTICATION_UNAVAILABLE_ATTRIBUTE))
        .thenReturn(Boolean.TRUE);

    authenticationEnforcementFilter.doFilter(servletRequest, servletResponse, filterChain);

    verify(filterChain, times(1)).doFilter(servletRequest, servletResponse);
    verify(servletResponse, never()).sendError(anyInt(), anyString());
  }

  @Test
  public void testEnforcementWithNoAuthenticationContext() throws ServletException, IOException {
    // Setup
    FilterConfig mockFilterConfig = mock(FilterConfig.class);
    when(mockFilterConfig.getInitParameterNames()).thenReturn(Collections.emptyEnumeration());
    authenticationEnforcementFilter.init(mockFilterConfig);

    HttpServletRequest servletRequest = mock(HttpServletRequest.class);
    HttpServletResponse servletResponse = mock(HttpServletResponse.class);
    FilterChain filterChain = mock(FilterChain.class);

    // Mock a protected path (not excluded)
    when(servletRequest.getServletPath()).thenReturn("/api/v2/graphql");

    // No authentication context set (simulating edge case)
    // AuthenticationContext.getAuthentication() will return null

    // Execute
    authenticationEnforcementFilter.doFilter(servletRequest, servletResponse, filterChain);

    // Verify - should return 401 when no authentication context
    verify(servletResponse, times(1))
        .sendError(
            eq(HttpServletResponse.SC_UNAUTHORIZED), eq("Unauthorized to perform this action."));
    verify(filterChain, never()).doFilter(any(), any());
  }

  @Test
  public void testExcludedPathsSkipEnforcement() throws ServletException, IOException {
    // Setup
    FilterConfig mockFilterConfig = mock(FilterConfig.class);
    when(mockFilterConfig.getInitParameterNames()).thenReturn(Collections.emptyEnumeration());
    authenticationEnforcementFilter.init(mockFilterConfig);

    HttpServletRequest servletRequest = mock(HttpServletRequest.class);
    HttpServletResponse servletResponse = mock(HttpServletResponse.class);
    FilterChain filterChain = mock(FilterChain.class);

    // Mock an excluded path
    when(servletRequest.getServletPath()).thenReturn("/health");

    // Set excluded paths in the filter
    ReflectionTestUtils.setField(
        authenticationEnforcementFilter,
        "excludedPathPatterns",
        new HashSet<>(Arrays.asList("/health", "/health/live", "/schema-registry/*")));

    // No authentication context set (simulating anonymous request to excluded path)
    // AuthenticationContext.getAuthentication() will return null

    // Execute
    authenticationEnforcementFilter.doFilter(servletRequest, servletResponse, filterChain);

    // Verify - should proceed without authentication for excluded paths
    verify(filterChain, times(1)).doFilter(servletRequest, servletResponse);
    verify(servletResponse, never()).sendError(anyInt(), anyString());
  }
}
