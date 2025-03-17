package com.datahub.auth.authentication;

import static com.datahub.authentication.AuthenticationConstants.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.datahub.auth.authentication.filter.AuthenticationFilter;
import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.token.StatefulTokenService;
import com.datahub.authentication.token.TokenException;
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

  @Autowired StatefulTokenService statefulTokenService;

  @Test
  public void testExpiredToken() throws ServletException, IOException, TokenException {
    FilterConfig mockFilterConfig = mock(FilterConfig.class);
    when(mockFilterConfig.getInitParameterNames()).thenReturn(Collections.emptyEnumeration());

    authenticationFilter.init(mockFilterConfig);
    HttpServletRequest servletRequest = mock(HttpServletRequest.class);
    HttpServletResponse servletResponse = mock(HttpServletResponse.class);
    FilterChain filterChain = mock(FilterChain.class);
    Actor actor = new Actor(ActorType.USER, "datahub");
    //    String token = _statefulTokenService.generateAccessToken(TokenType.SESSION, actor, 0L,
    // System.currentTimeMillis(), "token",
    //        "token", actor.toUrnStr());
    // Token generated 9/11/23, invalid for all future dates
    String token =
        "eyJhbGciOiJIUzI1NiJ9.eyJhY3RvclR5cGUiOiJVU0VSIZCI6ImRhdGFodWIiLCJ0eXBlIjoiU0VTU0lPTiIsInZlcnNpb24iOiIxIiwian"
            + "RpIjoiMmI0MzZkZDAtYjEwOS00N2UwLWJmYTEtMzM2ZmU4MTU4MDE1Iiwic3ViIjoiZGF0YWh1YiIsImV4cCI6MTY5NDU0NzA2OCwiaXNzIjoiZGF"
            + "0YWh1Yi1tZXRhZGF0YS1zZXJ2aWNlIn0.giqx7J5a9mxuubG6rXdAMoaGlcII-fqY-W82Wm7OlLI";
    when(servletRequest.getHeaderNames())
        .thenReturn(Collections.enumeration(List.of(AUTHORIZATION_HEADER_NAME)));
    when(servletRequest.getHeader(AUTHORIZATION_HEADER_NAME)).thenReturn("Bearer " + token);

    authenticationFilter.doFilter(servletRequest, servletResponse, filterChain);
    verify(servletResponse, times(1))
        .sendError(eq(HttpServletResponse.SC_UNAUTHORIZED), anyString());
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
}
