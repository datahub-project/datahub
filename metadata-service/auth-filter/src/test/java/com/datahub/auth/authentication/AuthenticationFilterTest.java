package com.datahub.auth.authentication;

import static com.datahub.authentication.AuthenticationConstants.*;
import static org.mockito.Mockito.*;

import com.datahub.auth.authentication.filter.AuthenticationFilter;
import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.token.StatefulTokenService;
import com.datahub.authentication.token.TokenException;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

@ContextConfiguration(classes = {AuthTestConfiguration.class})
public class AuthenticationFilterTest extends AbstractTestNGSpringContextTests {

  @Autowired AuthenticationFilter _authenticationFilter;

  @Autowired StatefulTokenService _statefulTokenService;

  @Test
  public void testExpiredToken() throws ServletException, IOException, TokenException {
    _authenticationFilter.init(null);
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

    _authenticationFilter.doFilter(servletRequest, servletResponse, filterChain);
    verify(servletResponse, times(1))
        .sendError(eq(HttpServletResponse.SC_UNAUTHORIZED), anyString());
  }
}
