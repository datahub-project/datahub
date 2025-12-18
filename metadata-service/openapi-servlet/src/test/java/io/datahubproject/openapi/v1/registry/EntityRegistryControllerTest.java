package io.datahubproject.openapi.v1.registry;

import static io.datahubproject.test.metadata.context.TestOperationContexts.TEST_USER_AUTH;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
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
import jakarta.servlet.ServletException;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class EntityRegistryControllerTest {
  private MockMvc mockMvc;
  private EntityRegistryController controller;
  private AuthorizerChain mockAuthorizerChain;
  private OperationContext operationContext;
  private MockedStatic<AuthUtil> authUtilMock;
  private MockedStatic<AuthenticationContext> authContextMock;

  private static final String TEST_ENTITY_NAME = "dataset";
  private static final String TEST_ASPECT_NAME = "datasetProperties";
  private static final String NON_EXISTENT_ENTITY = "nonExistentEntity";
  private static final String NON_EXISTENT_ASPECT = "nonExistentAspect";

  @BeforeMethod
  public void setup() {
    // Create mocks
    mockAuthorizerChain = mock(AuthorizerChain.class);

    operationContext =
        TestOperationContexts.userContextNoSearchAuthorization(mockAuthorizerChain, TEST_USER_AUTH);

    authContextMock = Mockito.mockStatic(AuthenticationContext.class);
    authContextMock.when(AuthenticationContext::getAuthentication).thenReturn(TEST_USER_AUTH);

    // Create controller
    controller = new EntityRegistryController(mockAuthorizerChain, operationContext);

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
  public void testGetEntitySpecsWithAuthorizationDefaultPagination() throws Exception {
    // Setup authorization to return true
    authUtilMock
        .when(
            () ->
                AuthUtil.isAPIOperationsAuthorized(
                    any(OperationContext.class), any(PoliciesConfig.Privilege.class)))
        .thenReturn(true);

    // Execute test - default pagination (start=0, count=20)
    mockMvc
        .perform(
            get("/openapi/v1/registry/models/entity/specifications")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.start", is(0)))
        .andExpect(jsonPath("$.count", is(5)))
        .andExpect(jsonPath("$.total", greaterThanOrEqualTo(50)))
        .andExpect(jsonPath("$.elements", hasSize(5)));
  }

  @Test
  public void testGetEntitySpecsWithCustomPagination() throws Exception {
    // Setup authorization to return true
    authUtilMock
        .when(
            () ->
                AuthUtil.isAPIOperationsAuthorized(
                    any(OperationContext.class), any(PoliciesConfig.Privilege.class)))
        .thenReturn(true);

    // Execute test with custom pagination
    mockMvc
        .perform(
            get("/openapi/v1/registry/models/entity/specifications")
                .param("start", "10")
                .param("count", "5")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.start", is(10)))
        .andExpect(jsonPath("$.count", is(5)))
        .andExpect(jsonPath("$.total", greaterThanOrEqualTo(50)))
        .andExpect(jsonPath("$.elements", hasSize(5)));
  }

  @Test
  public void testGetEntitySpecsWithLargeCount() throws Exception {
    // Setup authorization to return true
    authUtilMock
        .when(
            () ->
                AuthUtil.isAPIOperationsAuthorized(
                    any(OperationContext.class), any(PoliciesConfig.Privilege.class)))
        .thenReturn(true);

    // Execute test with count larger than total
    mockMvc
        .perform(
            get("/openapi/v1/registry/models/entity/specifications")
                .param("start", "0")
                .param("count", "100")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.start", is(0)))
        .andExpect(jsonPath("$.count", greaterThanOrEqualTo(50)))
        .andExpect(jsonPath("$.total", greaterThanOrEqualTo(50)))
        .andExpect(jsonPath("$.elements.length()", greaterThanOrEqualTo(50)));
  }

  @Test
  public void testGetEntitySpecsUnauthorized() throws Exception {
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
            get("/openapi/v1/registry/models/entity/specifications")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isForbidden());
  }

  @Test
  public void testGetEntitySpecByNameWithAuthorization() throws Exception {
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
            get("/openapi/v1/registry/models/entity/specifications/{entityName}", TEST_ENTITY_NAME)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.name", is(TEST_ENTITY_NAME)));
  }

  @Test
  public void testGetEntitySpecByNameNotFound() throws Exception {
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
                    "/openapi/v1/registry/models/entity/specifications/{entityName}",
                    NON_EXISTENT_ENTITY)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isNotFound());
  }

  @Test
  public void testGetEntitySpecByNameUnauthorized() throws Exception {
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
            get("/openapi/v1/registry/models/entity/specifications/{entityName}", TEST_ENTITY_NAME)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isForbidden());
  }

  @Test
  public void testGetEntityAspectSpecsWithAuthorization() throws Exception {
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
                    "/openapi/v1/registry/models/entity/specifications/{entityName}/aspects",
                    TEST_ENTITY_NAME)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$." + TEST_ASPECT_NAME).exists());
  }

  @Test
  public void testGetEntityAspectSpecsNotFound() throws Exception {
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
                    "/openapi/v1/registry/models/entity/specifications/{entityName}/aspects",
                    NON_EXISTENT_ENTITY)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isNotFound());
  }

  @Test
  public void testGetEntityAspectSpecsUnauthorized() throws Exception {
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
                    "/openapi/v1/registry/models/entity/specifications/{entityName}/aspects",
                    TEST_ENTITY_NAME)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isForbidden());
  }

  @Test
  public void testGetAspectSpecsWithAuthorizationDefaultPagination() throws Exception {
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
            get("/openapi/v1/registry/models/aspect/specifications")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.start", is(0)))
        .andExpect(jsonPath("$.count", is(20)))
        .andExpect(jsonPath("$.total", greaterThanOrEqualTo(200)))
        .andExpect(jsonPath("$.elements", hasSize(20)));
  }

  @Test
  public void testGetAspectSpecsWithCustomPagination() throws Exception {
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
            get("/openapi/v1/registry/models/aspect/specifications")
                .param("start", "5")
                .param("count", "10")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.start", is(5)))
        .andExpect(jsonPath("$.count", is(10)))
        .andExpect(jsonPath("$.total", greaterThanOrEqualTo(200)))
        .andExpect(jsonPath("$.elements", hasSize(10)));
  }

  @Test
  public void testGetAspectSpecsUnauthorized() throws Exception {
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
            get("/openapi/v1/registry/models/aspect/specifications")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isForbidden());
  }

  @Test
  public void testGetAspectSpecByNameWithAuthorization() throws Exception {
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
            get("/openapi/v1/registry/models/aspect/specifications/{aspectName}", TEST_ASPECT_NAME)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.aspectAnnotation.name", is(TEST_ASPECT_NAME)));
  }

  @Test
  public void testGetAspectSpecByNameNotFound() throws Exception {
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
                    "/openapi/v1/registry/models/aspect/specifications/{aspectName}",
                    NON_EXISTENT_ASPECT)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isNotFound());
  }

  @Test
  public void testGetAspectSpecByNameUnauthorized() throws Exception {
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
            get("/openapi/v1/registry/models/aspect/specifications/{aspectName}", TEST_ASPECT_NAME)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isForbidden());
  }

  @Test
  public void testGetEventSpecsWithAuthorizationNoPagination() throws Exception {
    // TODO: Revisit when there are events

    // Setup authorization to return true
    authUtilMock
        .when(
            () ->
                AuthUtil.isAPIOperationsAuthorized(
                    any(OperationContext.class), any(PoliciesConfig.Privilege.class)))
        .thenReturn(true);

    // Execute test - no pagination params should return all
    mockMvc
        .perform(
            get("/openapi/v1/registry/models/event/specifications")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.start", is(0)))
        .andExpect(jsonPath("$.count", is(0)))
        .andExpect(jsonPath("$.total", greaterThanOrEqualTo(0)))
        .andExpect(jsonPath("$.elements.length()", greaterThanOrEqualTo(0)));
  }

  @Test
  public void testGetEventSpecsWithPagination() throws Exception {
    // TODO: Revisit when we have these specs

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
            get("/openapi/v1/registry/models/event/specifications")
                .param("start", "2")
                .param("count", "3")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.start", is(0)))
        .andExpect(jsonPath("$.count", is(0)))
        .andExpect(jsonPath("$.total", greaterThanOrEqualTo(0)))
        .andExpect(jsonPath("$.elements.length()", greaterThanOrEqualTo(0)));
  }

  @Test
  public void testGetEventSpecsUnauthorized() throws Exception {
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
            get("/openapi/v1/registry/models/event/specifications")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isForbidden());
  }

  @Test(
      expectedExceptions = ServletException.class,
      expectedExceptionsMessageRegExp =
          "Request processing failed: java.lang.IllegalArgumentException: Start offset 1000000000 exceeds total .*")
  public void testPaginationWithStartBeyondTotal() throws Exception {
    // Setup authorization to return true
    authUtilMock
        .when(
            () ->
                AuthUtil.isAPIOperationsAuthorized(
                    any(OperationContext.class), any(PoliciesConfig.Privilege.class)))
        .thenReturn(true);

    // Execute test with start beyond total items
    mockMvc
        .perform(
            get("/openapi/v1/registry/models/aspect/specifications")
                .param("start", "1000000000")
                .param("count", "20")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isBadRequest());
  }

  @Test
  public void testPaginationWithZeroCount() throws Exception {
    // Setup authorization to return true
    authUtilMock
        .when(
            () ->
                AuthUtil.isAPIOperationsAuthorized(
                    any(OperationContext.class), any(PoliciesConfig.Privilege.class)))
        .thenReturn(true);

    // Execute test with count = 0
    mockMvc
        .perform(
            get("/openapi/v1/registry/models/aspect/specifications")
                .param("start", "0")
                .param("count", "0")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.start", is(0)))
        .andExpect(jsonPath("$.count", is(0)))
        .andExpect(jsonPath("$.total", greaterThanOrEqualTo(200)))
        .andExpect(jsonPath("$.elements", hasSize(0)));
  }
}
