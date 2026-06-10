package io.datahubproject.openapi.operations.ratelimit;

import static io.datahubproject.test.metadata.context.TestOperationContexts.TEST_USER_AUTH;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizerChain;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.config.ratelimit.RateLimitProperties;
import com.linkedin.metadata.ratelimit.RateLimitEngine;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Map;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RateLimitControllerTest {

  private MockMvc mockMvc;
  private RateLimitEngine rateLimitEngine;
  private AuthorizerChain authorizerChain;
  private MockedStatic<AuthUtil> authUtilMock;
  private MockedStatic<AuthenticationContext> authContextMock;

  @BeforeMethod
  public void setup() {
    rateLimitEngine = mock(RateLimitEngine.class);
    authorizerChain = mock(AuthorizerChain.class);
    OperationContext operationContext =
        TestOperationContexts.userContextNoSearchAuthorization(authorizerChain, TEST_USER_AUTH);

    authContextMock = Mockito.mockStatic(AuthenticationContext.class);
    authContextMock.when(AuthenticationContext::getAuthentication).thenReturn(TEST_USER_AUTH);

    authUtilMock = Mockito.mockStatic(AuthUtil.class);
    authUtilMock
        .when(
            () ->
                AuthUtil.isAPIAuthorized(
                    any(OperationContext.class), any(PoliciesConfig.Privilege.class)))
        .thenReturn(true);

    RateLimitController controller =
        new RateLimitController(operationContext, authorizerChain, rateLimitEngine);
    mockMvc =
        MockMvcBuilders.standaloneSetup(controller)
            .setMessageConverters(new MappingJackson2HttpMessageConverter())
            .build();
  }

  @AfterMethod
  public void tearDown() {
    authUtilMock.close();
    authContextMock.close();
  }

  @Test
  public void testConfigEndpoint() throws Exception {
    RateLimitProperties config = new RateLimitProperties();
    config.getCapacity().setEnabled(true);
    when(rateLimitEngine.getConfig()).thenReturn(config);

    mockMvc
        .perform(get("/openapi/v1/rate-limits/config"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.capacity.enabled").value(true));
    authUtilMock
        .when(
            () ->
                AuthUtil.isAPIAuthorized(
                    any(OperationContext.class), any(PoliciesConfig.Privilege.class)))
        .thenReturn(false);

    mockMvc.perform(get("/openapi/v1/rate-limits/config")).andExpect(status().isForbidden());
  }

  @Test
  public void testStatusEndpoint() throws Exception {
    when(rateLimitEngine.statusSnapshot()).thenReturn(Map.of("capacityEnabled", true));

    mockMvc
        .perform(get("/openapi/v1/rate-limits/status"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.capacityEnabled").value(true));
  }
}
