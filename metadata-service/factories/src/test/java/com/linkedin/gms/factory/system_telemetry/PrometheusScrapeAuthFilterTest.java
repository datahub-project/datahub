package com.linkedin.gms.factory.system_telemetry;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import jakarta.servlet.FilterChain;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PrometheusScrapeAuthFilterTest {

  private PrometheusScrapeAuthSettings settings;
  private PrometheusScrapeAuthFilter filter;
  private HttpServletRequest request;
  private HttpServletResponse response;
  private FilterChain chain;

  @BeforeMethod
  public void setUp() {
    settings = PrometheusScrapeAuthSettings.resolve(true, true, "prometheus", "secret");
    filter = new PrometheusScrapeAuthFilter(settings);
    filter.validateConfiguration();
    request = mock(HttpServletRequest.class);
    response = mock(HttpServletResponse.class);
    chain = mock(FilterChain.class);
  }

  @Test
  public void testDisabledFilterSkipsPrometheusPath() {
    settings = PrometheusScrapeAuthSettings.resolve(false, false, null, null);
    filter = new PrometheusScrapeAuthFilter(settings);
    when(request.getServletPath()).thenReturn(PrometheusScrapeAuthFilter.PROMETHEUS_ACTUATOR_PATH);
    assertTrue(filter.shouldNotFilter(request));
  }

  @Test
  public void testEnabledFilterAppliesToPrometheusPathOnly() {
    when(request.getServletPath()).thenReturn(PrometheusScrapeAuthFilter.PROMETHEUS_ACTUATOR_PATH);
    assertFalse(filter.shouldNotFilter(request));
    when(request.getServletPath()).thenReturn("/actuator/health");
    assertTrue(filter.shouldNotFilter(request));
  }

  @Test
  public void testMissingCredentialsReturns401() throws Exception {
    when(request.getServletPath()).thenReturn(PrometheusScrapeAuthFilter.PROMETHEUS_ACTUATOR_PATH);
    when(request.getHeader("Authorization")).thenReturn(null);

    filter.doFilterInternal(request, response, chain);

    Mockito.verify(response).setHeader("WWW-Authenticate", "Basic realm=\"Prometheus\"");
    Mockito.verify(response).sendError(HttpServletResponse.SC_UNAUTHORIZED, "Unauthorized");
    Mockito.verify(chain, Mockito.never()).doFilter(request, response);
  }

  @Test
  public void testValidBasicAuthAllowsRequest() throws Exception {
    when(request.getServletPath()).thenReturn(PrometheusScrapeAuthFilter.PROMETHEUS_ACTUATOR_PATH);
    when(request.getHeader("Authorization")).thenReturn(basicHeader("prometheus", "secret"));

    filter.doFilterInternal(request, response, chain);

    Mockito.verify(chain).doFilter(request, response);
    Mockito.verify(response, Mockito.never()).sendError(Mockito.anyInt(), Mockito.anyString());
  }

  @Test
  public void testDefaultCredentialsAllowRequest() throws Exception {
    settings = PrometheusScrapeAuthSettings.resolve(true, null, null, null);
    filter = new PrometheusScrapeAuthFilter(settings);
    when(request.getServletPath()).thenReturn(PrometheusScrapeAuthFilter.PROMETHEUS_ACTUATOR_PATH);
    when(request.getHeader("Authorization"))
        .thenReturn(
            basicHeader(
                PrometheusScrapeAuthSettings.DEFAULT_USERNAME,
                PrometheusScrapeAuthSettings.DEFAULT_PASSWORD));

    filter.doFilterInternal(request, response, chain);

    Mockito.verify(chain).doFilter(request, response);
  }

  @Test
  public void testInvalidPasswordReturns401() throws Exception {
    when(request.getServletPath()).thenReturn(PrometheusScrapeAuthFilter.PROMETHEUS_ACTUATOR_PATH);
    when(request.getHeader("Authorization")).thenReturn(basicHeader("prometheus", "wrong"));

    filter.doFilterInternal(request, response, chain);

    Mockito.verify(response).sendError(HttpServletResponse.SC_UNAUTHORIZED, "Unauthorized");
    Mockito.verify(chain, Mockito.never()).doFilter(request, response);
  }

  private static String basicHeader(String username, String password) {
    String token =
        Base64.getEncoder()
            .encodeToString((username + ":" + password).getBytes(StandardCharsets.UTF_8));
    return "Basic " + token;
  }
}
