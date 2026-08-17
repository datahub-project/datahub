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
import io.datahubproject.openapi.openlineage.config.OpenLineageAuthenticationErrorFilter;
import io.datahubproject.openapi.openlineage.config.OpenLineageServletConfig;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.util.ReflectionTestUtils;
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
    AuthenticationEnforcementFilter authenticationFilter = new AuthenticationEnforcementFilter();
    ReflectionTestUtils.setField(authenticationFilter, "excludedPathPatterns", Set.of());
    AuthenticationContext.setAuthentication(
        new Authentication(
            new Actor(ActorType.USER, ANONYMOUS_ACTOR_ID), "", Collections.emptyMap()));

    MockHttpServletRequest request = new MockHttpServletRequest("POST", ENDPOINT);
    request.setServletPath(ENDPOINT);
    MockHttpServletResponse response = new MockHttpServletResponse();
    AtomicBoolean controllerInvoked = new AtomicBoolean();

    new OpenLineageAuthenticationErrorFilter()
        .doFilter(
            request,
            response,
            (wrappedRequest, wrappedResponse) ->
                authenticationFilter.doFilter(
                    wrappedRequest,
                    wrappedResponse,
                    (ignoredRequest, ignoredResponse) -> controllerInvoked.set(true)));

    assertEquals(response.getStatus(), 401);
    assertEquals(response.getContentType(), "application/json;charset=UTF-8");
    JsonNode body = new ObjectMapper().readTree(response.getContentAsByteArray());
    assertEquals(body.path("code").textValue(), "AUTHENTICATION_REQUIRED");
    assertEquals(body.path("message").textValue(), "Authentication required");
    assertTrue(body.path("details").isObject());
    assertFalse(controllerInvoked.get());
  }

  @Test
  public void responseFilterIsRegisteredBeforeAuthenticationEnforcement() {
    FilterRegistrationBean<OpenLineageAuthenticationErrorFilter> responseRegistration =
        new OpenLineageServletConfig(null).openLineageAuthenticationErrorFilter();
    FilterRegistrationBean<AuthenticationEnforcementFilter> enforcementRegistration =
        new ServletConfig().authFilter(new AuthenticationEnforcementFilter());

    assertTrue(responseRegistration.getUrlPatterns().contains("/openapi/openlineage/*"));
    assertTrue(responseRegistration.getOrder() < enforcementRegistration.getOrder());
  }
}
