package com.linkedin.gms.factory.usage;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.linkedin.metadata.usage.UsageTestFixtures;
import com.linkedin.metadata.usage.instrumentation.UsageMetricsSessionEnricher;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import jakarta.servlet.FilterChain;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ScimUsageSessionFilterTest {

  @Test
  public void testScimGetRequestOpensUsageSession() throws Exception {
    UsageMetricsSessionEnricher enricher = mock(UsageMetricsSessionEnricher.class);
    OperationContext systemContext = TestOperationContexts.systemContextNoValidate();
    ScimUsageSessionFilter filter = new ScimUsageSessionFilter(systemContext, enricher);

    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse response = mock(HttpServletResponse.class);
    FilterChain chain = mock(FilterChain.class);
    when(request.getRequestURI()).thenReturn("/openapi/scim/v2/Users");
    when(request.getMethod()).thenReturn("GET");
    when(request.getRemoteAddr()).thenReturn("127.0.0.1");
    when(request.getHeader(any())).thenReturn(null);

    Authentication authentication =
        new Authentication(
            new Actor(ActorType.USER, UsageTestFixtures.REGULAR_CORP_USER_ID), "test");
    try (MockedStatic<AuthenticationContext> authContext =
        Mockito.mockStatic(AuthenticationContext.class)) {
      authContext.when(AuthenticationContext::getAuthentication).thenReturn(authentication);

      filter.doFilter(request, response, chain);
    }

    verify(enricher).recordTaggedServletRequest(any(), any(), any());
    verify(chain).doFilter(request, response);
  }

  @Test
  public void testScimPostRequestUsesOtherWrite() throws Exception {
    UsageMetricsSessionEnricher enricher = mock(UsageMetricsSessionEnricher.class);
    OperationContext systemContext = TestOperationContexts.systemContextNoValidate();
    ScimUsageSessionFilter filter = new ScimUsageSessionFilter(systemContext, enricher);

    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse response = mock(HttpServletResponse.class);
    FilterChain chain = mock(FilterChain.class);
    when(request.getRequestURI()).thenReturn("/openapi/scim/v2/Users");
    when(request.getMethod()).thenReturn("POST");
    when(request.getRemoteAddr()).thenReturn("127.0.0.1");
    when(request.getHeader(any())).thenReturn(null);

    Authentication authentication =
        new Authentication(
            new Actor(ActorType.USER, UsageTestFixtures.REGULAR_CORP_USER_ID), "test");
    try (MockedStatic<AuthenticationContext> authContext =
        Mockito.mockStatic(AuthenticationContext.class)) {
      authContext.when(AuthenticationContext::getAuthentication).thenReturn(authentication);

      filter.doFilter(request, response, chain);
    }

    ArgumentCaptor<RequestContext.RequestContextBuilder> builderCaptor =
        ArgumentCaptor.forClass(RequestContext.RequestContextBuilder.class);
    verify(enricher).recordTaggedServletRequest(any(), builderCaptor.capture(), any());
    Assert.assertEquals(
        builderCaptor.getValue().peekUsageOperation(),
        io.datahubproject.metadata.context.usage.UsageOperation.OTHER_WRITE.key());
  }

  @Test
  public void testNonScimPathSkipsUsageSession() throws Exception {
    UsageMetricsSessionEnricher enricher = mock(UsageMetricsSessionEnricher.class);
    ScimUsageSessionFilter filter =
        new ScimUsageSessionFilter(TestOperationContexts.systemContextNoValidate(), enricher);

    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse response = mock(HttpServletResponse.class);
    FilterChain chain = mock(FilterChain.class);
    when(request.getRequestURI()).thenReturn("/openapi/v3/entity/dataset");

    filter.doFilter(request, response, chain);

    verify(enricher, never()).recordTaggedServletRequest(any(), any(), any());
    verify(chain).doFilter(request, response);
  }

  @Test
  public void testScimPathSkipsWhenUnauthenticated() throws Exception {
    UsageMetricsSessionEnricher enricher = mock(UsageMetricsSessionEnricher.class);
    ScimUsageSessionFilter filter =
        new ScimUsageSessionFilter(TestOperationContexts.systemContextNoValidate(), enricher);

    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse response = mock(HttpServletResponse.class);
    FilterChain chain = mock(FilterChain.class);
    when(request.getRequestURI()).thenReturn("/openapi/scim/v2/Users");

    try (MockedStatic<AuthenticationContext> authContext =
        Mockito.mockStatic(AuthenticationContext.class)) {
      authContext.when(AuthenticationContext::getAuthentication).thenReturn(null);

      filter.doFilter(request, response, chain);
    }

    verify(enricher, never()).recordTaggedServletRequest(any(), any(), any());
    verify(chain).doFilter(request, response);
  }
}
