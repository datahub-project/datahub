package io.datahubproject.openapi.v1.entities;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizerChain;
import com.linkedin.metadata.authorization.ApiGroup;
import com.linkedin.metadata.authorization.ApiOperation;
import com.linkedin.metadata.systemmetadata.KeyAspectEntityCountEntry;
import com.linkedin.metadata.systemmetadata.KeyAspectEntityCountResult;
import com.linkedin.metadata.systemmetadata.KeyAspectEntityCountService;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.openapi.config.GlobalControllerExceptionHandler;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.time.Instant;
import java.util.List;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class EntityCountsControllerTest {

  private MockMvc mockMvc;
  private AuthorizerChain mockAuthorizerChain;
  private KeyAspectEntityCountService keyAspectEntityCountService;
  private OperationContext operationContext;
  private MockedStatic<AuthUtil> authUtilMock;
  private MockedStatic<AuthenticationContext> authContextMock;

  @BeforeMethod
  public void setup() {
    mockAuthorizerChain = mock(AuthorizerChain.class);
    keyAspectEntityCountService = mock(KeyAspectEntityCountService.class);
    operationContext = TestOperationContexts.systemContextNoSearchAuthorization();
    Authentication authentication = mock(Authentication.class);

    authContextMock = Mockito.mockStatic(AuthenticationContext.class);
    authContextMock.when(AuthenticationContext::getAuthentication).thenReturn(authentication);
    when(authentication.getActor()).thenReturn(TestOperationContexts.TEST_USER_AUTH.getActor());

    EntityCountsController controller =
        new EntityCountsController(
            mockAuthorizerChain, operationContext, keyAspectEntityCountService);
    mockMvc =
        MockMvcBuilders.standaloneSetup(controller)
            .setControllerAdvice(new GlobalControllerExceptionHandler())
            .build();

    authUtilMock = Mockito.mockStatic(AuthUtil.class);
  }

  @AfterMethod
  public void tearDown() {
    authUtilMock.close();
    authContextMock.close();
  }

  @Test
  public void getEntityCountsForbiddenWithoutAuth() throws Exception {
    authUtilMock
        .when(
            () ->
                AuthUtil.isAPIAuthorized(
                    any(OperationContext.class), eq(ApiGroup.COUNTS), eq(ApiOperation.READ)))
        .thenReturn(false);

    mockMvc
        .perform(
            get("/openapi/v1/entities/counts?types=dataset").accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isForbidden());
  }

  @Test
  public void getEntityCountsReturnsActiveAndSoftDeletedCounts() throws Exception {
    authUtilMock
        .when(
            () ->
                AuthUtil.isAPIAuthorized(
                    any(OperationContext.class), eq(ApiGroup.COUNTS), eq(ApiOperation.READ)))
        .thenReturn(true);

    KeyAspectEntityCountResult result =
        KeyAspectEntityCountResult.builder()
            .counts(
                List.of(
                    KeyAspectEntityCountEntry.builder()
                        .entityType("dataset")
                        .keyAspect("datasetKey")
                        .activeCount(10L)
                        .softDeletedCount(2L)
                        .build()))
            .requestedTypes(List.of("dataset"))
            .computedAt(Instant.parse("2026-07-07T15:00:00Z"))
            .cacheHit(false)
            .build();
    when(keyAspectEntityCountService.getCounts(
            any(OperationContext.class), eq(List.of("dataset")), eq(false)))
        .thenReturn(result);

    mockMvc
        .perform(
            get("/openapi/v1/entities/counts?types=dataset").accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.counts[0].entityType").value("dataset"))
        .andExpect(jsonPath("$.counts[0].activeCount").value(10))
        .andExpect(jsonPath("$.counts[0].softDeletedCount").value(2))
        .andExpect(jsonPath("$.counts[0].totalCount").doesNotExist())
        .andExpect(jsonPath("$.cacheHit").value(false))
        .andExpect(
            jsonPath("$.computedAtMillis")
                .value(Instant.parse("2026-07-07T15:00:00Z").toEpochMilli()));
  }

  @Test
  public void getEntityCountsIncludeTotalAddsRollups() throws Exception {
    authUtilMock
        .when(
            () ->
                AuthUtil.isAPIAuthorized(
                    any(OperationContext.class), eq(ApiGroup.COUNTS), eq(ApiOperation.READ)))
        .thenReturn(true);

    KeyAspectEntityCountResult result =
        KeyAspectEntityCountResult.builder()
            .counts(
                List.of(
                    KeyAspectEntityCountEntry.builder()
                        .entityType("dataset")
                        .keyAspect("datasetKey")
                        .activeCount(10L)
                        .softDeletedCount(2L)
                        .build()))
            .requestedTypes(List.of("dataset"))
            .computedAt(Instant.parse("2026-07-07T15:00:00Z"))
            .cacheHit(true)
            .build();
    when(keyAspectEntityCountService.getCounts(
            any(OperationContext.class), eq(List.of("dataset")), eq(false)))
        .thenReturn(result);

    mockMvc
        .perform(
            get("/openapi/v1/entities/counts?types=dataset&includeTotal=true")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.counts[0].totalCount").value(12))
        .andExpect(jsonPath("$.activeTotal").value(10))
        .andExpect(jsonPath("$.softDeletedTotal").value(2))
        .andExpect(jsonPath("$.totalCount").value(12))
        .andExpect(jsonPath("$.computedAtMillis").exists());
  }

  @Test
  public void getEntityTypeCountReturnsSingleType() throws Exception {
    authUtilMock
        .when(
            () ->
                AuthUtil.isAPIAuthorized(
                    any(OperationContext.class), eq(ApiGroup.COUNTS), eq(ApiOperation.READ)))
        .thenReturn(true);

    KeyAspectEntityCountResult result =
        KeyAspectEntityCountResult.builder()
            .counts(
                List.of(
                    KeyAspectEntityCountEntry.builder()
                        .entityType("chart")
                        .keyAspect("chartKey")
                        .activeCount(4L)
                        .softDeletedCount(1L)
                        .build()))
            .requestedTypes(List.of("chart"))
            .computedAt(Instant.parse("2026-07-07T15:00:00Z"))
            .cacheHit(false)
            .build();
    when(keyAspectEntityCountService.getCountForEntityType(
            any(OperationContext.class), eq("chart"), eq(true)))
        .thenReturn(result);

    mockMvc
        .perform(
            get("/openapi/v1/entities/chart/count?skipCache=true&includeTotal=true")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.entityType").value("chart"))
        .andExpect(jsonPath("$.activeCount").value(4))
        .andExpect(jsonPath("$.softDeletedCount").value(1))
        .andExpect(jsonPath("$.totalCount").value(5))
        .andExpect(
            jsonPath("$.computedAtMillis")
                .value(Instant.parse("2026-07-07T15:00:00Z").toEpochMilli()));
  }

  @Test
  public void getEntityTypeCountForbiddenWithoutAuth() throws Exception {
    authUtilMock
        .when(
            () ->
                AuthUtil.isAPIAuthorized(
                    any(OperationContext.class), eq(ApiGroup.COUNTS), eq(ApiOperation.READ)))
        .thenReturn(false);

    mockMvc
        .perform(get("/openapi/v1/entities/chart/count").accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isForbidden());
  }
}
