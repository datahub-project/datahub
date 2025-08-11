package com.datahub.auth.authentication;

import static com.linkedin.metadata.Constants.ANONYMOUS_ACTOR_ID;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.datahub.auth.authentication.filter.AuthenticationEnforcementFilter;
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

    HttpServletRequest wildcardPathRequest = mock(HttpServletRequest.class);
    when(wildcardPathRequest.getServletPath()).thenReturn("/schema-registry/api/config");

    HttpServletRequest nonExcludedRequest = mock(HttpServletRequest.class);
    when(nonExcludedRequest.getServletPath()).thenReturn("/protected/resource");

    // Set excluded paths in the filter
    ReflectionTestUtils.setField(
        authenticationEnforcementFilter,
        "excludedPathPatterns",
        new HashSet<>(Arrays.asList("/health", "/schema-registry/*")));

    // Verify exact path match
    assertTrue(
        authenticationEnforcementFilter.shouldNotFilter(exactPathRequest),
        "Exact path match should be excluded from filtering");

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
        new HashSet<>(Arrays.asList("/health", "/schema-registry/*")));

    // No authentication context set (simulating anonymous request to excluded path)
    // AuthenticationContext.getAuthentication() will return null

    // Execute
    authenticationEnforcementFilter.doFilter(servletRequest, servletResponse, filterChain);

    // Verify - should proceed without authentication for excluded paths
    verify(filterChain, times(1)).doFilter(servletRequest, servletResponse);
    verify(servletResponse, never()).sendError(anyInt(), anyString());
  }
}
