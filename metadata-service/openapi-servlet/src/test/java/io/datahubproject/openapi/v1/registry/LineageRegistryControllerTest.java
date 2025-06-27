package io.datahubproject.openapi.v1.registry;

import static io.datahubproject.test.metadata.context.TestOperationContexts.TEST_USER_AUTH;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizerChain;
import com.linkedin.metadata.authorization.PoliciesConfig;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class LineageRegistryControllerTest {
  private MockMvc mockMvc;
  private LineageRegistryController controller;
  private AuthorizerChain mockAuthorizerChain;
  private OperationContext operationContext;
  private MockedStatic<AuthUtil> authUtilMock;
  private MockedStatic<AuthenticationContext> authContextMock;

  private static final String TEST_ENTITY_NAME = "dataset";
  private static final String TEST_DOWNSTREAM_ENTITY = "chart";

  @BeforeMethod
  public void setup() {
    // Create mocks
    mockAuthorizerChain = mock(AuthorizerChain.class);

    operationContext =
        TestOperationContexts.userContextNoSearchAuthorization(mockAuthorizerChain, TEST_USER_AUTH);

    authContextMock = Mockito.mockStatic(AuthenticationContext.class);
    authContextMock.when(AuthenticationContext::getAuthentication).thenReturn(TEST_USER_AUTH);

    // Create controller
    controller = new LineageRegistryController(mockAuthorizerChain, operationContext);

    // Setup MockMvc
    mockMvc = MockMvcBuilders.standaloneSetup(controller).build();

    // Mock AuthUtil static methods
    authUtilMock = Mockito.mockStatic(AuthUtil.class);
  }

  @AfterMethod
  public void tearDown() {
    authUtilMock.close();
    authContextMock.close();
  }

  @Test
  public void testGetLineageSpecsWithAuthorization() throws Exception {
    // Setup authorization to return true
    authUtilMock
        .when(
            () ->
                AuthUtil.isAPIOperationsAuthorized(
                    any(OperationContext.class), any(PoliciesConfig.Privilege.class)))
        .thenReturn(true);

    // Execute test
    mockMvc
        .perform(
            get("/openapi/v1/registry/lineage/specifications").accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.dataset").exists())
        .andExpect(jsonPath("$.datajob").exists());
  }

  @Test
  public void testGetLineageSpecsUnauthorized() throws Exception {
    // Setup authorization to return false
    authUtilMock
        .when(
            () ->
                AuthUtil.isAPIOperationsAuthorized(
                    any(OperationContext.class), any(PoliciesConfig.Privilege.class)))
        .thenReturn(false);

    // Execute test
    mockMvc
        .perform(
            get("/openapi/v1/registry/lineage/specifications").accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isForbidden());
  }

  @Test
  public void testGetLineageSpecByEntityWithAuthorization() throws Exception {
    // Setup authorization to return true
    authUtilMock
        .when(
            () ->
                AuthUtil.isAPIOperationsAuthorized(
                    any(OperationContext.class), any(PoliciesConfig.Privilege.class)))
        .thenReturn(true);

    // Execute test
    mockMvc
        .perform(
            get("/openapi/v1/registry/lineage/specifications/{entityName}", TEST_ENTITY_NAME)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk());
  }

  @Test
  public void testGetLineageSpecByEntityUnauthorized() throws Exception {
    // Setup authorization to return false
    authUtilMock
        .when(
            () ->
                AuthUtil.isAPIOperationsAuthorized(
                    any(OperationContext.class), any(PoliciesConfig.Privilege.class)))
        .thenReturn(false);

    // Execute test
    mockMvc
        .perform(
            get("/openapi/v1/registry/lineage/specifications/{entityName}", TEST_ENTITY_NAME)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isForbidden());
  }

  @Test
  public void testGetLineageEdgesWithAuthorization() throws Exception {
    // Setup authorization to return true
    authUtilMock
        .when(
            () ->
                AuthUtil.isAPIOperationsAuthorized(
                    any(OperationContext.class), any(PoliciesConfig.Privilege.class)))
        .thenReturn(true);

    // Execute test - verify we have at least 5 elements and check some specific combinations exist
    mockMvc
        .perform(
            get("/openapi/v1/registry/lineage/edges/{entityName}", TEST_ENTITY_NAME)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.length()", greaterThanOrEqualTo(5)))
        // Check that certain type/direction/entity combinations exist somewhere in the array
        .andExpect(
            jsonPath(
                    "$[?(@.type == 'DownstreamOf' && @.direction == 'OUTGOING' && @.opposingEntityType == 'dataset')]")
                .exists())
        .andExpect(
            jsonPath(
                    "$[?(@.type == 'Produces' && @.direction == 'INCOMING' && @.opposingEntityType == 'datajob')]")
                .exists())
        .andExpect(
            jsonPath(
                    "$[?(@.type == 'Consumes' && @.direction == 'INCOMING' && @.opposingEntityType == 'chart')]")
                .exists())
        .andExpect(
            jsonPath(
                    "$[?(@.type == 'Consumes' && @.direction == 'INCOMING' && @.opposingEntityType == 'dashboard')]")
                .exists())
        .andExpect(
            jsonPath(
                    "$[?(@.type == 'DownstreamOf' && @.direction == 'INCOMING' && @.opposingEntityType == 'dataset')]")
                .exists());
  }

  @Test
  public void testGetLineageEdgesUnauthorized() throws Exception {
    // Setup authorization to return false
    authUtilMock
        .when(
            () ->
                AuthUtil.isAPIOperationsAuthorized(
                    any(OperationContext.class), any(PoliciesConfig.Privilege.class)))
        .thenReturn(false);

    // Execute test
    mockMvc
        .perform(
            get("/openapi/v1/registry/lineage/edges/{entityName}", TEST_ENTITY_NAME)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isForbidden());
  }

  @Test
  public void testGetLineageDirectedEdgesUpstreamWithAuthorization() throws Exception {
    // Setup authorization to return true
    authUtilMock
        .when(
            () ->
                AuthUtil.isAPIOperationsAuthorized(
                    any(OperationContext.class), any(PoliciesConfig.Privilege.class)))
        .thenReturn(true);

    // Execute test
    mockMvc
        .perform(
            get(
                    "/openapi/v1/registry/lineage/edges/{entityName}/{direction}",
                    TEST_ENTITY_NAME,
                    "UPSTREAM")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.length()", greaterThanOrEqualTo(3)))
        // Check that certain type/direction/entity combinations exist somewhere in the array
        .andExpect(
            jsonPath(
                    "$[?(@.type == 'DownstreamOf' && @.direction == 'OUTGOING' && @.opposingEntityType == 'dataset')]")
                .exists())
        .andExpect(
            jsonPath(
                    "$[?(@.type == 'DataProcessInstanceProduces' && @.direction == 'INCOMING' && @.opposingEntityType == 'dataprocessinstance')]")
                .exists())
        .andExpect(
            jsonPath(
                    "$[?(@.type == 'Produces' && @.direction == 'INCOMING' && @.opposingEntityType == 'datajob')]")
                .exists());
  }

  @Test
  public void testGetLineageDirectedEdgesDownstreamWithAuthorization() throws Exception {
    // Setup authorization to return true
    authUtilMock
        .when(
            () ->
                AuthUtil.isAPIOperationsAuthorized(
                    any(OperationContext.class), any(PoliciesConfig.Privilege.class)))
        .thenReturn(true);

    // Execute test
    mockMvc
        .perform(
            get(
                    "/openapi/v1/registry/lineage/edges/{entityName}/{direction}",
                    TEST_ENTITY_NAME,
                    "DOWNSTREAM")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.length()", greaterThanOrEqualTo(8)))
        // Check that certain type/direction/entity combinations exist somewhere in the array
        .andExpect(
            jsonPath(
                    "$[?(@.type == 'DownstreamOf' && @.direction == 'INCOMING' && @.opposingEntityType == 'dataset')]")
                .exists())
        .andExpect(
            jsonPath(
                    "$[?(@.type == 'DataProcessInstanceConsumes' && @.direction == 'INCOMING' && @.opposingEntityType == 'dataprocessinstance')]")
                .exists())
        .andExpect(
            jsonPath(
                    "$[?(@.type == 'Consumes' && @.direction == 'INCOMING' && @.opposingEntityType == 'datajob')]")
                .exists());
  }

  @Test
  public void testGetLineageDirectedEdgesUnauthorized() throws Exception {
    // Setup authorization to return false
    authUtilMock
        .when(
            () ->
                AuthUtil.isAPIOperationsAuthorized(
                    any(OperationContext.class), any(PoliciesConfig.Privilege.class)))
        .thenReturn(false);

    // Execute test
    mockMvc
        .perform(
            get(
                    "/openapi/v1/registry/lineage/edges/{entityName}/{direction}",
                    TEST_ENTITY_NAME,
                    "UPSTREAM")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isForbidden());
  }
}
