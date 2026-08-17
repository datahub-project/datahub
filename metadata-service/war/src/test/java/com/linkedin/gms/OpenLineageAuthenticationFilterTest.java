package com.linkedin.gms;

import static com.linkedin.metadata.Constants.ANONYMOUS_ACTOR_ID;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.datahub.auth.authentication.filter.AuthenticationEnforcementFilter;
import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.datahubproject.openapi.openlineage.config.OpenLineageAuthenticationFilter;
import io.datahubproject.openapi.openlineage.config.OpenLineageServletConfig;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

public class OpenLineageAuthenticationFilterTest {
  private static final String ENDPOINT = "/openapi/openlineage/api/v1/lineage";

  @AfterMethod
  public void cleanup() {
    AuthenticationContext.remove();
  }

  @Test
  public void unauthenticatedRequestUsesStructuredOpenLineageResponse() throws Exception {
    AuthenticationContext.setAuthentication(
        new Authentication(
            new Actor(ActorType.USER, ANONYMOUS_ACTOR_ID), "", Collections.emptyMap()));
    MockHttpServletRequest request = new MockHttpServletRequest("POST", ENDPOINT);
    MockHttpServletResponse response = new MockHttpServletResponse();
    AtomicBoolean controllerInvoked = new AtomicBoolean();

    new OpenLineageAuthenticationFilter()
        .doFilter(
            request, response, (ignoredRequest, ignoredResponse) -> controllerInvoked.set(true));

    assertEquals(response.getStatus(), 401);
    JsonNode body = new ObjectMapper().readTree(response.getContentAsByteArray());
    assertEquals(body.path("code").textValue(), "AUTHENTICATION_REQUIRED");
    assertTrue(body.path("details").isObject());
    assertFalse(controllerInvoked.get());
  }

  @Test
  public void authenticatedRequestContinues() throws Exception {
    AuthenticationContext.setAuthentication(
        new Authentication(new Actor(ActorType.USER, "datahub"), "", Collections.emptyMap()));
    AtomicBoolean controllerInvoked = new AtomicBoolean();
    new OpenLineageAuthenticationFilter()
        .doFilter(
            new MockHttpServletRequest("POST", ENDPOINT),
            new MockHttpServletResponse(),
            (ignoredRequest, ignoredResponse) -> controllerInvoked.set(true));
    assertTrue(controllerInvoked.get());
  }

  @Test
  public void authenticationGateIsRegisteredBetweenExtractionAndEnforcement() {
    FilterRegistrationBean<OpenLineageAuthenticationFilter> gateRegistration =
        new OpenLineageServletConfig(null).openLineageAuthenticationFilter();
    FilterRegistrationBean<AuthenticationEnforcementFilter> enforcementRegistration =
        new ServletConfig().authFilter(new AuthenticationEnforcementFilter());

    assertTrue(gateRegistration.getUrlPatterns().contains("/openapi/openlineage/*"));
    assertEquals(gateRegistration.getOrder(), Integer.MIN_VALUE + 2);
    assertTrue(gateRegistration.getOrder() < enforcementRegistration.getOrder());
  }
}
