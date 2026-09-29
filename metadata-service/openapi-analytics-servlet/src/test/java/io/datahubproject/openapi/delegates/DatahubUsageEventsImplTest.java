package io.datahubproject.openapi.delegates;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.AuthorizerChain;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.authorization.PoliciesConfig;
import io.datahubproject.openapi.config.OpenAPIAnalyticsTestConfiguration;
import io.datahubproject.openapi.config.SpringWebConfig;
import io.datahubproject.openapi.exception.UnauthorizedException;
import io.datahubproject.openapi.v2.generated.controller.DatahubUsageEventsApiController;
import java.util.Set;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

// AuthUtil is imported with REST API authorization enabled, as in production, so the privilege
// check below is actually evaluated rather than short-circuited to allow.
@SpringBootTest(
    classes = {SpringWebConfig.class},
    properties = {"authorization.restApiAuthorization=true"})
@ComponentScan(basePackages = {"io.datahubproject.openapi.v2.generated.controller"})
@Import({DatahubUsageEventsImpl.class, OpenAPIAnalyticsTestConfiguration.class, AuthUtil.class})
public class DatahubUsageEventsImplTest extends AbstractTestNGSpringContextTests {

  @Autowired private DatahubUsageEventsApiController analyticsController;

  @Autowired private AuthorizerChain authorizerChain;

  @MockitoBean private ConfigurationProvider configurationProvider;

  @MockitoBean private io.datahubproject.openapi.config.TracingInterceptor tracingInterceptor;

  @AfterMethod
  public void restoreAllowAll() {
    doReturn(new AuthorizationResult(null, AuthorizationResult.Type.ALLOW, ""))
        .when(authorizerChain)
        .authorize(any());
    actAs("datahub");
  }

  /**
   * Authorization decisions are cached per session actor, so each privilege scenario runs as its
   * own actor to keep the mock authorizer's answers from leaking between tests.
   */
  private static void actAs(String userId) {
    AuthenticationContext.setAuthentication(
        new Authentication(new Actor(ActorType.USER, userId), ""));
  }

  @Test
  public void initTest() {
    assertNotNull(analyticsController);
  }

  @Test
  public void analyticsControllerTest() {
    ResponseEntity<String> resp = analyticsController.raw("");
    assertEquals(resp.getStatusCode(), HttpStatus.OK);
  }

  @Test
  public void testDeniedWithoutPrivileges() {
    actAs("no-privileges");
    grantOnly(Set.of());
    assertThrows(UnauthorizedException.class, () -> analyticsController.raw(""));
  }

  /** View Analytics is granted to every user by default and must not be enough for raw search. */
  @Test
  public void testViewAnalyticsAloneIsDenied() {
    actAs("view-analytics-only");
    grantOnly(Set.of("VIEW_ANALYTICS"));
    assertThrows(UnauthorizedException.class, () -> analyticsController.raw(""));
  }

  @Test
  public void testAnalyticsApiAccessAloneIsAllowed() {
    actAs("analytics-api-only");
    grantOnly(Set.of(PoliciesConfig.GET_ANALYTICS_PRIVILEGE.getType()));
    assertEquals(analyticsController.raw("").getStatusCode(), HttpStatus.OK);
  }

  @Test
  public void testManageSystemOperationsAloneIsAllowed() {
    actAs("system-operations-only");
    grantOnly(Set.of(PoliciesConfig.MANAGE_SYSTEM_OPERATIONS_PRIVILEGE.getType()));
    assertEquals(analyticsController.raw("").getStatusCode(), HttpStatus.OK);
  }

  private void grantOnly(Set<String> privileges) {
    doAnswer(
            invocation -> {
              AuthorizationRequest request = invocation.getArgument(0);
              AuthorizationResult.Type type =
                  request != null && privileges.contains(request.getPrivilege())
                      ? AuthorizationResult.Type.ALLOW
                      : AuthorizationResult.Type.DENY;
              return new AuthorizationResult(request, type, "");
            })
        .when(authorizerChain)
        .authorize(any());
  }
}
