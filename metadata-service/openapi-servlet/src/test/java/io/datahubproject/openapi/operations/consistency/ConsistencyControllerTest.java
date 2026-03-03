package io.datahubproject.openapi.operations.consistency;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.AuthorizerChain;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.metadata.aspect.consistency.ConsistencyCheckRegistry;
import com.linkedin.metadata.aspect.consistency.ConsistencyIssue;
import com.linkedin.metadata.aspect.consistency.ConsistencyService;
import com.linkedin.metadata.aspect.consistency.SystemMetadataFilter;
import com.linkedin.metadata.aspect.consistency.check.CheckBatchRequest;
import com.linkedin.metadata.aspect.consistency.check.CheckContext;
import com.linkedin.metadata.aspect.consistency.check.CheckResult;
import com.linkedin.metadata.aspect.consistency.check.ConsistencyCheck;
import com.linkedin.metadata.aspect.consistency.fix.ConsistencyFixDetail;
import com.linkedin.metadata.aspect.consistency.fix.ConsistencyFixResult;
import com.linkedin.metadata.aspect.consistency.fix.ConsistencyFixType;
import com.linkedin.metadata.config.ConsistencyChecksConfiguration;
import com.linkedin.metadata.config.DataHubAppConfiguration;
import com.linkedin.metadata.models.registry.EntityRegistry;
import io.datahubproject.metadata.context.ObjectMapperContext;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import io.datahubproject.openapi.config.GlobalControllerExceptionHandler;
import io.datahubproject.openapi.config.TracingInterceptor;
import io.datahubproject.openapi.operations.consistency.models.ConsistencyCheckRequest;
import io.datahubproject.openapi.operations.consistency.models.ConsistencyFixIssuesRequest;
import io.datahubproject.openapi.operations.consistency.models.ConsistencyFixRequest;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureWebMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.http.MediaType;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@SpringBootTest(classes = ConsistencyControllerTest.TestConfig.class)
@AutoConfigureWebMvc
@AutoConfigureMockMvc
public class ConsistencyControllerTest extends AbstractTestNGSpringContextTests {

  private static final Urn TEST_URN_1 = UrnUtils.getUrn("urn:li:assertion:test-123");
  private static final Urn TEST_URN_2 = UrnUtils.getUrn("urn:li:assertion:test-456");

  @Autowired private ConsistencyController consistencyController;

  @Autowired private MockMvc mockMvc;

  @Autowired private ConsistencyService mockConsistencyService;

  @Autowired private AuthorizerChain authorizerChain;

  @Autowired private ObjectMapper objectMapper;

  @BeforeMethod
  public void setupMocks() {
    // Setup Authentication
    Authentication authentication = mock(Authentication.class);
    when(authentication.getActor()).thenReturn(new Actor(ActorType.USER, "datahub"));
    AuthenticationContext.setAuthentication(authentication);

    // Setup AuthorizerChain to allow access by default
    when(authorizerChain.authorize(any()))
        .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.ALLOW, ""));
  }

  @Test
  public void initTest() {
    assertNotNull(consistencyController);
  }

  // ============================================================================
  // GET /checks - List Checks
  // ============================================================================

  @Test
  public void testListChecks() throws Exception {
    // Mock registry to return test checks
    ConsistencyCheckRegistry mockRegistry = mock(ConsistencyCheckRegistry.class);
    ConsistencyCheck testCheck = createTestCheck("test-check", "Test Check", "assertion");
    when(mockRegistry.getDefaultChecks()).thenReturn(List.of(testCheck));
    when(mockConsistencyService.getCheckRegistry()).thenReturn(mockRegistry);

    mockMvc
        .perform(
            MockMvcRequestBuilders.get("/openapi/operations/consistency/checks")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(MockMvcResultMatchers.jsonPath("$.checks").isArray())
        .andExpect(MockMvcResultMatchers.jsonPath("$.checks[0].id").value("test-check"))
        .andExpect(MockMvcResultMatchers.jsonPath("$.checks[0].name").value("Test Check"))
        .andExpect(MockMvcResultMatchers.jsonPath("$.checks[0].entityType").value("assertion"));
  }

  @Test
  public void testListChecksWithEntityTypeFilter() throws Exception {
    ConsistencyCheckRegistry mockRegistry = mock(ConsistencyCheckRegistry.class);
    ConsistencyCheck assertionCheck =
        createTestCheck("assertion-check", "Assertion Check", "assertion");
    when(mockRegistry.getDefaultByEntityType("assertion")).thenReturn(List.of(assertionCheck));
    when(mockConsistencyService.getCheckRegistry()).thenReturn(mockRegistry);

    mockMvc
        .perform(
            MockMvcRequestBuilders.get("/openapi/operations/consistency/checks")
                .param("entityType", "assertion")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(MockMvcResultMatchers.jsonPath("$.checks[0].entityType").value("assertion"));

    verify(mockRegistry).getDefaultByEntityType("assertion");
  }

  @Test
  public void testListChecksIncludeOnDemand() throws Exception {
    ConsistencyCheckRegistry mockRegistry = mock(ConsistencyCheckRegistry.class);
    ConsistencyCheck onDemandCheck =
        createOnDemandCheck("on-demand-check", "On Demand Check", "assertion");
    when(mockRegistry.getAll()).thenReturn(List.of(onDemandCheck));
    when(mockConsistencyService.getCheckRegistry()).thenReturn(mockRegistry);

    mockMvc
        .perform(
            MockMvcRequestBuilders.get("/openapi/operations/consistency/checks")
                .param("includeOnDemand", "true")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(MockMvcResultMatchers.jsonPath("$.checks[0].id").value("on-demand-check"))
        .andExpect(MockMvcResultMatchers.jsonPath("$.checks[0].onDemandOnly").value(true));

    verify(mockRegistry).getAll();
    verify(mockRegistry, never()).getDefaultChecks();
  }

  // ============================================================================
  // GET /checks/{checkId} - Get Single Check
  // ============================================================================

  @Test
  public void testGetCheck() throws Exception {
    ConsistencyCheckRegistry mockRegistry = mock(ConsistencyCheckRegistry.class);
    ConsistencyCheck testCheck = createTestCheck("test-check", "Test Check", "assertion");
    when(mockRegistry.getById("test-check")).thenReturn(Optional.of(testCheck));
    when(mockConsistencyService.getCheckRegistry()).thenReturn(mockRegistry);

    mockMvc
        .perform(
            MockMvcRequestBuilders.get("/openapi/operations/consistency/checks/test-check")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(MockMvcResultMatchers.jsonPath("$.id").value("test-check"))
        .andExpect(MockMvcResultMatchers.jsonPath("$.name").value("Test Check"));
  }

  @Test
  public void testGetCheckNotFound() throws Exception {
    ConsistencyCheckRegistry mockRegistry = mock(ConsistencyCheckRegistry.class);
    when(mockRegistry.getById("non-existent")).thenReturn(Optional.empty());
    when(mockConsistencyService.getCheckRegistry()).thenReturn(mockRegistry);

    mockMvc
        .perform(
            MockMvcRequestBuilders.get("/openapi/operations/consistency/checks/non-existent")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isNotFound());
  }

  // ============================================================================
  // GET /entities/{urn} - Check Single Entity
  // ============================================================================

  @Test
  public void testCheckEntity() throws Exception {
    // Mock checkBatch to return result (entity exists)
    when(mockConsistencyService.checkBatch(any(), any(CheckBatchRequest.class)))
        .thenReturn(
            CheckResult.builder().entitiesScanned(1).issuesFound(0).issues(List.of()).build());

    mockMvc
        .perform(
            MockMvcRequestBuilders.get(
                    "/openapi/operations/consistency/entities/urn:li:assertion:test-123")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(MockMvcResultMatchers.jsonPath("$").isArray());
  }

  @Test
  public void testCheckEntityNotFound() throws Exception {
    // Mock checkBatch to return 0 entities scanned (URN not found in ES index)
    when(mockConsistencyService.checkBatch(any(), any(CheckBatchRequest.class)))
        .thenReturn(
            CheckResult.builder().entitiesScanned(0).issuesFound(0).issues(List.of()).build());

    mockMvc
        .perform(
            MockMvcRequestBuilders.get(
                    "/openapi/operations/consistency/entities/urn:li:assertion:non-existent")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isNotFound());
  }

  @Test
  public void testCheckEntityWithIssues() throws Exception {
    // Mock checkBatch to return issues
    List<ConsistencyIssue> issues =
        List.of(
            ConsistencyIssue.builder()
                .entityUrn(TEST_URN_1)
                .entityType("assertion")
                .checkId("test-check")
                .fixType(ConsistencyFixType.SOFT_DELETE)
                .description("Test issue")
                .build());

    when(mockConsistencyService.checkBatch(any(), any(CheckBatchRequest.class)))
        .thenReturn(CheckResult.builder().entitiesScanned(1).issuesFound(1).issues(issues).build());

    mockMvc
        .perform(
            MockMvcRequestBuilders.get(
                    "/openapi/operations/consistency/entities/urn:li:assertion:test-123")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(MockMvcResultMatchers.jsonPath("$").isArray())
        .andExpect(MockMvcResultMatchers.jsonPath("$[0].checkId").value("test-check"));
  }

  @Test
  public void testCheckEntityOrphanDetection() throws Exception {
    // Mock checkBatch to return orphan issue (entity exists in ES but not in SQL)
    Urn orphanUrn = UrnUtils.getUrn("urn:li:assertion:orphan-entity");
    List<ConsistencyIssue> orphanIssues =
        List.of(
            ConsistencyIssue.builder()
                .entityUrn(orphanUrn)
                .entityType("assertion")
                .checkId("orphan-index-document")
                .fixType(ConsistencyFixType.DELETE_INDEX_DOCUMENTS)
                .description("Entity exists in ES but not in SQL")
                .build());

    when(mockConsistencyService.checkBatch(any(), any(CheckBatchRequest.class)))
        .thenReturn(
            CheckResult.builder().entitiesScanned(1).issuesFound(1).issues(orphanIssues).build());

    mockMvc
        .perform(
            MockMvcRequestBuilders.get(
                    "/openapi/operations/consistency/entities/urn:li:assertion:orphan-entity")
                .param("checkIds", "orphan-index-document")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(MockMvcResultMatchers.jsonPath("$").isArray())
        .andExpect(MockMvcResultMatchers.jsonPath("$[0].checkId").value("orphan-index-document"));
  }

  // Note: Invalid URN format handling (400 response) is tested through
  // GlobalControllerExceptionHandler which handles IllegalArgumentException.
  // The controller also explicitly catches and handles IllegalArgumentException.

  // ============================================================================
  // POST /check - Batch Check
  // ============================================================================

  @Test
  public void testBatchCheck() throws Exception {
    CheckResult checkResult =
        CheckResult.builder()
            .entitiesScanned(10)
            .issuesFound(2)
            .issues(
                List.of(
                    ConsistencyIssue.builder()
                        .entityUrn(TEST_URN_1)
                        .entityType("assertion")
                        .checkId("test-check")
                        .fixType(ConsistencyFixType.SOFT_DELETE)
                        .description("Test issue")
                        .build()))
            .scrollId("scroll123")
            .build();

    when(mockConsistencyService.checkBatch(
            any(OperationContext.class), any(CheckBatchRequest.class)))
        .thenReturn(checkResult);

    ConsistencyCheckRequest request = new ConsistencyCheckRequest();
    request.setEntityType("assertion");
    request.setBatchSize(100);

    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/operations/consistency/check")
                .content(objectMapper.writeValueAsString(request))
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(MockMvcResultMatchers.jsonPath("$.entitiesScanned").value(10))
        .andExpect(MockMvcResultMatchers.jsonPath("$.issuesFound").value(2))
        .andExpect(MockMvcResultMatchers.jsonPath("$.scrollId").value("scroll123"))
        .andExpect(MockMvcResultMatchers.jsonPath("$.issues[0].entityUrn").exists());
  }

  @Test
  public void testBatchCheckWithGracePeriod() throws Exception {
    CheckResult checkResult = CheckResult.empty();
    when(mockConsistencyService.checkBatch(
            any(OperationContext.class), any(CheckBatchRequest.class)))
        .thenReturn(checkResult);

    ConsistencyCheckRequest request = new ConsistencyCheckRequest();
    request.setEntityType("assertion");
    request.setGracePeriodSeconds(600L); // 10 minutes

    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/operations/consistency/check")
                .content(objectMapper.writeValueAsString(request))
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk());
  }

  // ============================================================================
  // buildFilterWithGracePeriod Edge Cases
  // ============================================================================

  @Test
  public void testGracePeriodZeroDisablesFilter() throws Exception {
    // When grace period is 0, no filter should be applied
    CheckResult checkResult = CheckResult.empty();

    ArgumentCaptor<CheckBatchRequest> requestCaptor =
        ArgumentCaptor.forClass(CheckBatchRequest.class);

    when(mockConsistencyService.checkBatch(any(OperationContext.class), requestCaptor.capture()))
        .thenReturn(checkResult);

    ConsistencyCheckRequest request = new ConsistencyCheckRequest();
    request.setEntityType("assertion");
    request.setGracePeriodSeconds(0L); // Zero grace period

    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/operations/consistency/check")
                .content(objectMapper.writeValueAsString(request))
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk());

    // Verify null filter was passed (grace period disabled)
    assertNull(requestCaptor.getValue().getFilter());
  }

  @Test
  public void testGracePeriodNegativeDisablesFilter() throws Exception {
    // When grace period is negative, no filter should be applied
    CheckResult checkResult = CheckResult.empty();

    ArgumentCaptor<CheckBatchRequest> requestCaptor =
        ArgumentCaptor.forClass(CheckBatchRequest.class);

    when(mockConsistencyService.checkBatch(any(OperationContext.class), requestCaptor.capture()))
        .thenReturn(checkResult);

    ConsistencyCheckRequest request = new ConsistencyCheckRequest();
    request.setEntityType("assertion");
    request.setGracePeriodSeconds(-1L); // Negative grace period

    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/operations/consistency/check")
                .content(objectMapper.writeValueAsString(request))
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk());

    // Verify null filter was passed (grace period disabled)
    assertNull(requestCaptor.getValue().getFilter());
  }

  @Test
  public void testGracePeriodAppliesLePitEpochMs() throws Exception {
    // When grace period is positive, lePitEpochMs should be set
    CheckResult checkResult = CheckResult.empty();

    ArgumentCaptor<CheckBatchRequest> requestCaptor =
        ArgumentCaptor.forClass(CheckBatchRequest.class);

    when(mockConsistencyService.checkBatch(any(OperationContext.class), requestCaptor.capture()))
        .thenReturn(checkResult);

    long beforeRequest = System.currentTimeMillis();

    ConsistencyCheckRequest request = new ConsistencyCheckRequest();
    request.setEntityType("assertion");
    request.setGracePeriodSeconds(300L); // 5 minutes

    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/operations/consistency/check")
                .content(objectMapper.writeValueAsString(request))
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk());

    long afterRequest = System.currentTimeMillis();

    // Verify filter has lePitEpochMs set (current time - 5 minutes)
    SystemMetadataFilter capturedFilter = requestCaptor.getValue().getFilter();
    assertNotNull(capturedFilter);
    assertNotNull(capturedFilter.getLePitEpochMs());

    // The lePitEpochMs should be approximately (now - 300 seconds)
    long expectedLowerBound = beforeRequest - (300L * 1000L) - 1000L; // 1 second tolerance
    long expectedUpperBound = afterRequest - (300L * 1000L) + 1000L;
    assertTrue(
        capturedFilter.getLePitEpochMs() >= expectedLowerBound,
        "lePitEpochMs should be >= " + expectedLowerBound);
    assertTrue(
        capturedFilter.getLePitEpochMs() <= expectedUpperBound,
        "lePitEpochMs should be <= " + expectedUpperBound);
  }

  @Test
  public void testUserSetLePitEpochMsIsHonored() throws Exception {
    // When user explicitly sets lePitEpochMs, it should be honored over grace period
    CheckResult checkResult = CheckResult.empty();

    ArgumentCaptor<CheckBatchRequest> requestCaptor =
        ArgumentCaptor.forClass(CheckBatchRequest.class);

    when(mockConsistencyService.checkBatch(any(OperationContext.class), requestCaptor.capture()))
        .thenReturn(checkResult);

    long userSetTimestamp = 1700000000000L; // A specific timestamp

    ConsistencyCheckRequest request = new ConsistencyCheckRequest();
    request.setEntityType("assertion");
    request.setGracePeriodSeconds(300L); // 5 minutes - should be ignored

    // Create filter with explicit lePitEpochMs
    io.datahubproject.openapi.operations.consistency.models.SystemMetadataFilter apiFilter =
        new io.datahubproject.openapi.operations.consistency.models.SystemMetadataFilter();
    apiFilter.setLePitEpochMs(userSetTimestamp);
    request.setFilter(apiFilter);

    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/operations/consistency/check")
                .content(objectMapper.writeValueAsString(request))
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk());

    // Verify user's timestamp was preserved
    SystemMetadataFilter capturedFilter = requestCaptor.getValue().getFilter();
    assertNotNull(capturedFilter);
    assertEquals(capturedFilter.getLePitEpochMs(), Long.valueOf(userSetTimestamp));
  }

  @Test
  public void testFilterMergesWithGracePeriod() throws Exception {
    // When user provides filter with other fields, they should be preserved
    // and grace period should add lePitEpochMs
    CheckResult checkResult = CheckResult.empty();

    ArgumentCaptor<CheckBatchRequest> requestCaptor =
        ArgumentCaptor.forClass(CheckBatchRequest.class);

    when(mockConsistencyService.checkBatch(any(OperationContext.class), requestCaptor.capture()))
        .thenReturn(checkResult);

    long gePitTimestamp = 1600000000000L;

    ConsistencyCheckRequest request = new ConsistencyCheckRequest();
    request.setEntityType("assertion");
    request.setGracePeriodSeconds(300L);

    // Create filter with other fields set
    io.datahubproject.openapi.operations.consistency.models.SystemMetadataFilter apiFilter =
        new io.datahubproject.openapi.operations.consistency.models.SystemMetadataFilter();
    apiFilter.setGePitEpochMs(gePitTimestamp);
    apiFilter.setAspectFilters(List.of("assertionInfo"));
    apiFilter.setIncludeSoftDeleted(true);
    request.setFilter(apiFilter);

    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/operations/consistency/check")
                .content(objectMapper.writeValueAsString(request))
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk());

    // Verify all filter fields are preserved
    SystemMetadataFilter capturedFilter = requestCaptor.getValue().getFilter();
    assertNotNull(capturedFilter);
    assertEquals(capturedFilter.getGePitEpochMs(), Long.valueOf(gePitTimestamp));
    assertEquals(capturedFilter.getAspectFilters(), List.of("assertionInfo"));
    assertTrue(capturedFilter.isIncludeSoftDeleted());
    // And lePitEpochMs was added from grace period
    assertNotNull(capturedFilter.getLePitEpochMs());
  }

  @Test
  public void testDefaultGracePeriodFromConfig() throws Exception {
    // When no grace period is specified, config default (300s) should be used
    CheckResult checkResult = CheckResult.empty();

    ArgumentCaptor<CheckBatchRequest> requestCaptor =
        ArgumentCaptor.forClass(CheckBatchRequest.class);

    when(mockConsistencyService.checkBatch(any(OperationContext.class), requestCaptor.capture()))
        .thenReturn(checkResult);

    long beforeRequest = System.currentTimeMillis();

    ConsistencyCheckRequest request = new ConsistencyCheckRequest();
    request.setEntityType("assertion");
    // No grace period set - should use config default (300s)

    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/operations/consistency/check")
                .content(objectMapper.writeValueAsString(request))
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk());

    long afterRequest = System.currentTimeMillis();

    // Verify filter has lePitEpochMs set using config default (300 seconds)
    SystemMetadataFilter capturedFilter = requestCaptor.getValue().getFilter();
    assertNotNull(capturedFilter);
    assertNotNull(capturedFilter.getLePitEpochMs());

    // The lePitEpochMs should be approximately (now - 300 seconds)
    long expectedLowerBound = beforeRequest - (300L * 1000L) - 1000L;
    long expectedUpperBound = afterRequest - (300L * 1000L) + 1000L;
    assertTrue(
        capturedFilter.getLePitEpochMs() >= expectedLowerBound,
        "lePitEpochMs should be >= " + expectedLowerBound);
    assertTrue(
        capturedFilter.getLePitEpochMs() <= expectedUpperBound,
        "lePitEpochMs should be <= " + expectedUpperBound);
  }

  // ============================================================================
  // POST /fix-issues - Fix Specific Issues
  // ============================================================================

  @Test
  public void testFixIssues() throws Exception {
    ConsistencyFixResult fixResult =
        ConsistencyFixResult.builder()
            .dryRun(true)
            .totalProcessed(1)
            .entitiesFixed(1)
            .entitiesFailed(0)
            .fixDetails(
                List.of(
                    ConsistencyFixDetail.builder()
                        .urn(TEST_URN_1)
                        .action(ConsistencyFixType.SOFT_DELETE)
                        .success(true)
                        .build()))
            .build();

    when(mockConsistencyService.fixIssues(any(OperationContext.class), anyList(), eq(true)))
        .thenReturn(fixResult);

    io.datahubproject.openapi.operations.consistency.models.ConsistencyIssue issue =
        io.datahubproject.openapi.operations.consistency.models.ConsistencyIssue.builder()
            .entityUrn(TEST_URN_1.toString())
            .entityType("assertion")
            .checkId("test-check")
            .fixType(ConsistencyFixType.SOFT_DELETE)
            .description("Test issue")
            .build();

    ConsistencyFixIssuesRequest request = new ConsistencyFixIssuesRequest();
    request.setIssues(List.of(issue));
    request.setDryRun(true);

    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/operations/consistency/fix-issues")
                .content(objectMapper.writeValueAsString(request))
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(MockMvcResultMatchers.jsonPath("$.dryRun").value(true))
        .andExpect(MockMvcResultMatchers.jsonPath("$.totalProcessed").value(1))
        .andExpect(MockMvcResultMatchers.jsonPath("$.entitiesFixed").value(1))
        .andExpect(MockMvcResultMatchers.jsonPath("$.fixDetails[0].success").value(true));
  }

  @Test
  public void testFixIssuesWithRelatedUrns() throws Exception {
    ConsistencyFixResult fixResult =
        ConsistencyFixResult.builder()
            .dryRun(false)
            .totalProcessed(1)
            .entitiesFixed(1)
            .entitiesFailed(0)
            .fixDetails(
                List.of(
                    ConsistencyFixDetail.builder()
                        .urn(TEST_URN_1)
                        .action(ConsistencyFixType.HARD_DELETE)
                        .success(true)
                        .build()))
            .build();

    when(mockConsistencyService.fixIssues(any(OperationContext.class), anyList(), eq(false)))
        .thenReturn(fixResult);

    io.datahubproject.openapi.operations.consistency.models.ConsistencyIssue issue =
        io.datahubproject.openapi.operations.consistency.models.ConsistencyIssue.builder()
            .entityUrn(TEST_URN_1.toString())
            .entityType("assertion")
            .checkId("test-check")
            .fixType(ConsistencyFixType.HARD_DELETE)
            .description("Test issue with related URNs")
            .relatedUrns(List.of(TEST_URN_2.toString()))
            .build();

    ConsistencyFixIssuesRequest request = new ConsistencyFixIssuesRequest();
    request.setIssues(List.of(issue));
    request.setDryRun(false);

    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/operations/consistency/fix-issues")
                .content(objectMapper.writeValueAsString(request))
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(MockMvcResultMatchers.jsonPath("$.dryRun").value(false));
  }

  @Test
  public void testFixIssuesWithNullRelatedUrns() throws Exception {
    ConsistencyFixResult fixResult =
        ConsistencyFixResult.builder()
            .dryRun(true)
            .totalProcessed(1)
            .entitiesFixed(1)
            .entitiesFailed(0)
            .fixDetails(
                List.of(
                    ConsistencyFixDetail.builder()
                        .urn(TEST_URN_1)
                        .action(ConsistencyFixType.SOFT_DELETE)
                        .success(true)
                        .build()))
            .build();

    when(mockConsistencyService.fixIssues(any(OperationContext.class), anyList(), eq(true)))
        .thenReturn(fixResult);

    // Create issue with null relatedUrns (explicitly testing toServiceIssue conversion)
    io.datahubproject.openapi.operations.consistency.models.ConsistencyIssue issue =
        io.datahubproject.openapi.operations.consistency.models.ConsistencyIssue.builder()
            .entityUrn(TEST_URN_1.toString())
            .entityType("assertion")
            .checkId("test-check")
            .fixType(ConsistencyFixType.SOFT_DELETE)
            .description("Test issue")
            .relatedUrns(null)
            .build();

    ConsistencyFixIssuesRequest request = new ConsistencyFixIssuesRequest();
    request.setIssues(List.of(issue));
    request.setDryRun(true);

    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/operations/consistency/fix-issues")
                .content(objectMapper.writeValueAsString(request))
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk());
  }

  // ============================================================================
  // POST /fix - Check and Fix Combined
  // ============================================================================

  @Test
  public void testCheckAndFix() throws Exception {
    CheckResult checkResult =
        CheckResult.builder()
            .entitiesScanned(10)
            .issuesFound(1)
            .issues(
                List.of(
                    ConsistencyIssue.builder()
                        .entityUrn(TEST_URN_1)
                        .entityType("assertion")
                        .checkId("test-check")
                        .fixType(ConsistencyFixType.SOFT_DELETE)
                        .description("Test issue")
                        .build()))
            .build();

    ConsistencyFixResult fixResult =
        ConsistencyFixResult.builder()
            .dryRun(true)
            .totalProcessed(1)
            .entitiesFixed(1)
            .entitiesFailed(0)
            .fixDetails(
                List.of(
                    ConsistencyFixDetail.builder()
                        .urn(TEST_URN_1)
                        .action(ConsistencyFixType.SOFT_DELETE)
                        .success(true)
                        .build()))
            .build();

    when(mockConsistencyService.checkBatch(
            any(OperationContext.class), any(CheckBatchRequest.class)))
        .thenReturn(checkResult);

    when(mockConsistencyService.fixIssues(any(OperationContext.class), anyList(), eq(true)))
        .thenReturn(fixResult);

    ConsistencyFixRequest request = new ConsistencyFixRequest();
    request.setEntityType("assertion");
    request.setBatchSize(100);
    request.setDryRun(true);

    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/operations/consistency/fix")
                .content(objectMapper.writeValueAsString(request))
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(MockMvcResultMatchers.jsonPath("$.entitiesScanned").value(10))
        .andExpect(MockMvcResultMatchers.jsonPath("$.issuesFound").value(1))
        .andExpect(MockMvcResultMatchers.jsonPath("$.dryRun").value(true))
        .andExpect(MockMvcResultMatchers.jsonPath("$.entitiesFixed").value(1));
  }

  // ============================================================================
  // toServiceIssue Conversion Tests
  // ============================================================================

  @Test
  @SuppressWarnings("unchecked")
  public void testToServiceIssueConversionWithRelatedUrns() throws Exception {
    // Test that toServiceIssue correctly converts relatedUrns from String to Urn
    ConsistencyFixResult fixResult =
        ConsistencyFixResult.builder()
            .dryRun(true)
            .totalProcessed(1)
            .entitiesFixed(1)
            .entitiesFailed(0)
            .fixDetails(List.of())
            .build();

    ArgumentCaptor<List<ConsistencyIssue>> issuesCaptor = ArgumentCaptor.forClass(List.class);

    when(mockConsistencyService.fixIssues(
            any(OperationContext.class), issuesCaptor.capture(), eq(true)))
        .thenReturn(fixResult);

    io.datahubproject.openapi.operations.consistency.models.ConsistencyIssue issue =
        io.datahubproject.openapi.operations.consistency.models.ConsistencyIssue.builder()
            .entityUrn(TEST_URN_1.toString())
            .entityType("assertion")
            .checkId("test-check")
            .fixType(ConsistencyFixType.HARD_DELETE)
            .description("Test issue")
            .relatedUrns(List.of(TEST_URN_2.toString(), "urn:li:assertion:test-789"))
            .details("Additional details")
            .build();

    ConsistencyFixIssuesRequest request = new ConsistencyFixIssuesRequest();
    request.setIssues(List.of(issue));
    request.setDryRun(true);

    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/operations/consistency/fix-issues")
                .content(objectMapper.writeValueAsString(request))
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk());

    // Verify the conversion
    List<ConsistencyIssue> capturedIssues = issuesCaptor.getValue();
    assertEquals(capturedIssues.size(), 1);

    ConsistencyIssue capturedIssue = capturedIssues.get(0);
    assertEquals(capturedIssue.getEntityUrn(), TEST_URN_1);
    assertEquals(capturedIssue.getEntityType(), "assertion");
    assertEquals(capturedIssue.getCheckId(), "test-check");
    assertEquals(capturedIssue.getFixType(), ConsistencyFixType.HARD_DELETE);
    assertEquals(capturedIssue.getDescription(), "Test issue");
    assertEquals(capturedIssue.getDetails(), "Additional details");

    // Verify relatedUrns were converted to Urn objects
    assertNotNull(capturedIssue.getRelatedUrns());
    assertEquals(capturedIssue.getRelatedUrns().size(), 2);
    assertEquals(capturedIssue.getRelatedUrns().get(0), TEST_URN_2);
    assertEquals(
        capturedIssue.getRelatedUrns().get(1), UrnUtils.getUrn("urn:li:assertion:test-789"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testToServiceIssueConversionWithNullRelatedUrns() throws Exception {
    // Test that toServiceIssue correctly handles null relatedUrns
    ConsistencyFixResult fixResult =
        ConsistencyFixResult.builder()
            .dryRun(true)
            .totalProcessed(1)
            .entitiesFixed(1)
            .entitiesFailed(0)
            .fixDetails(List.of())
            .build();

    ArgumentCaptor<List<ConsistencyIssue>> issuesCaptor = ArgumentCaptor.forClass(List.class);

    when(mockConsistencyService.fixIssues(
            any(OperationContext.class), issuesCaptor.capture(), eq(true)))
        .thenReturn(fixResult);

    io.datahubproject.openapi.operations.consistency.models.ConsistencyIssue issue =
        io.datahubproject.openapi.operations.consistency.models.ConsistencyIssue.builder()
            .entityUrn(TEST_URN_1.toString())
            .entityType("assertion")
            .checkId("test-check")
            .fixType(ConsistencyFixType.SOFT_DELETE)
            .description("Test issue")
            .relatedUrns(null) // Explicitly null
            .build();

    ConsistencyFixIssuesRequest request = new ConsistencyFixIssuesRequest();
    request.setIssues(List.of(issue));
    request.setDryRun(true);

    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/operations/consistency/fix-issues")
                .content(objectMapper.writeValueAsString(request))
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk());

    // Verify the conversion handles null relatedUrns
    List<ConsistencyIssue> capturedIssues = issuesCaptor.getValue();
    assertEquals(capturedIssues.size(), 1);
    assertNull(capturedIssues.get(0).getRelatedUrns());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testToServiceIssueConversionWithEmptyRelatedUrns() throws Exception {
    // Test that toServiceIssue correctly handles empty relatedUrns list
    ConsistencyFixResult fixResult =
        ConsistencyFixResult.builder()
            .dryRun(true)
            .totalProcessed(1)
            .entitiesFixed(1)
            .entitiesFailed(0)
            .fixDetails(List.of())
            .build();

    ArgumentCaptor<List<ConsistencyIssue>> issuesCaptor = ArgumentCaptor.forClass(List.class);

    when(mockConsistencyService.fixIssues(
            any(OperationContext.class), issuesCaptor.capture(), eq(true)))
        .thenReturn(fixResult);

    io.datahubproject.openapi.operations.consistency.models.ConsistencyIssue issue =
        io.datahubproject.openapi.operations.consistency.models.ConsistencyIssue.builder()
            .entityUrn(TEST_URN_1.toString())
            .entityType("assertion")
            .checkId("test-check")
            .fixType(ConsistencyFixType.SOFT_DELETE)
            .description("Test issue")
            .relatedUrns(List.of()) // Empty list
            .build();

    ConsistencyFixIssuesRequest request = new ConsistencyFixIssuesRequest();
    request.setIssues(List.of(issue));
    request.setDryRun(true);

    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/operations/consistency/fix-issues")
                .content(objectMapper.writeValueAsString(request))
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk());

    // Verify the conversion handles empty relatedUrns
    List<ConsistencyIssue> capturedIssues = issuesCaptor.getValue();
    assertEquals(capturedIssues.size(), 1);
    assertNotNull(capturedIssues.get(0).getRelatedUrns());
    assertTrue(capturedIssues.get(0).getRelatedUrns().isEmpty());
  }

  // ============================================================================
  // Error Response Tests
  // ============================================================================

  @Test
  public void testCheckEntityWithSpecificCheckIds() throws Exception {
    // Mock checkBatch to return empty issues list (entity exists, no issues)
    when(mockConsistencyService.checkBatch(any(), any(CheckBatchRequest.class)))
        .thenReturn(
            CheckResult.builder().entitiesScanned(1).issuesFound(0).issues(List.of()).build());

    mockMvc
        .perform(
            MockMvcRequestBuilders.get(
                    "/openapi/operations/consistency/entities/urn:li:assertion:test-123")
                .param("checkIds", "specific-check")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk());

    // Verify checkBatch was called with URN filter
    verify(mockConsistencyService).checkBatch(any(), any(CheckBatchRequest.class));
  }

  @Test
  public void testBatchCheckWithScrollId() throws Exception {
    CheckResult checkResult =
        CheckResult.builder()
            .entitiesScanned(50)
            .issuesFound(0)
            .issues(List.of())
            .scrollId("nextPageToken")
            .build();

    when(mockConsistencyService.checkBatch(
            any(OperationContext.class), any(CheckBatchRequest.class)))
        .thenReturn(checkResult);

    ConsistencyCheckRequest request = new ConsistencyCheckRequest();
    request.setEntityType("assertion");
    request.setScrollId("previousPageToken");

    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/operations/consistency/check")
                .content(objectMapper.writeValueAsString(request))
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(MockMvcResultMatchers.jsonPath("$.scrollId").value("nextPageToken"));
  }

  @Test
  public void testCheckAndFixWithActualFix() throws Exception {
    // Test with dryRun=false to verify actual fix behavior
    CheckResult checkResult =
        CheckResult.builder()
            .entitiesScanned(5)
            .issuesFound(2)
            .issues(
                List.of(
                    ConsistencyIssue.builder()
                        .entityUrn(TEST_URN_1)
                        .entityType("assertion")
                        .checkId("test-check")
                        .fixType(ConsistencyFixType.HARD_DELETE)
                        .description("Issue 1")
                        .build(),
                    ConsistencyIssue.builder()
                        .entityUrn(TEST_URN_2)
                        .entityType("assertion")
                        .checkId("test-check")
                        .fixType(ConsistencyFixType.SOFT_DELETE)
                        .description("Issue 2")
                        .build()))
            .build();

    ConsistencyFixResult fixResult =
        ConsistencyFixResult.builder()
            .dryRun(false)
            .totalProcessed(2)
            .entitiesFixed(2)
            .entitiesFailed(0)
            .fixDetails(
                List.of(
                    ConsistencyFixDetail.builder()
                        .urn(TEST_URN_1)
                        .action(ConsistencyFixType.HARD_DELETE)
                        .success(true)
                        .build(),
                    ConsistencyFixDetail.builder()
                        .urn(TEST_URN_2)
                        .action(ConsistencyFixType.SOFT_DELETE)
                        .success(true)
                        .build()))
            .build();

    when(mockConsistencyService.checkBatch(
            any(OperationContext.class), any(CheckBatchRequest.class)))
        .thenReturn(checkResult);

    when(mockConsistencyService.fixIssues(any(OperationContext.class), anyList(), eq(false)))
        .thenReturn(fixResult);

    ConsistencyFixRequest request = new ConsistencyFixRequest();
    request.setEntityType("assertion");
    request.setBatchSize(50);
    request.setDryRun(false);

    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/operations/consistency/fix")
                .content(objectMapper.writeValueAsString(request))
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(MockMvcResultMatchers.jsonPath("$.dryRun").value(false))
        .andExpect(MockMvcResultMatchers.jsonPath("$.entitiesFixed").value(2))
        .andExpect(MockMvcResultMatchers.jsonPath("$.fixDetails").isArray())
        .andExpect(MockMvcResultMatchers.jsonPath("$.fixDetails.length()").value(2));
  }

  // ============================================================================
  // Helper Methods
  // ============================================================================

  private ConsistencyCheck createTestCheck(String id, String name, String entityType) {
    return new ConsistencyCheck() {
      @Override
      @Nonnull
      public String getId() {
        return id;
      }

      @Override
      @Nonnull
      public String getName() {
        return name;
      }

      @Override
      @Nonnull
      public String getDescription() {
        return "Test description";
      }

      @Override
      @Nonnull
      public String getEntityType() {
        return entityType;
      }

      @Override
      @Nonnull
      public Optional<Set<String>> getRequiredAspects() {
        return Optional.of(Set.of("testAspect"));
      }

      @Override
      @Nonnull
      public List<ConsistencyIssue> check(
          @Nonnull CheckContext ctx, @Nonnull Map<Urn, EntityResponse> entityResponses) {
        return List.of();
      }
    };
  }

  private ConsistencyCheck createOnDemandCheck(String id, String name, String entityType) {
    return new ConsistencyCheck() {
      @Override
      @Nonnull
      public String getId() {
        return id;
      }

      @Override
      @Nonnull
      public String getName() {
        return name;
      }

      @Override
      @Nonnull
      public String getDescription() {
        return "On-demand test description";
      }

      @Override
      @Nonnull
      public String getEntityType() {
        return entityType;
      }

      @Override
      public boolean isOnDemandOnly() {
        return true;
      }

      @Override
      @Nonnull
      public Optional<Set<String>> getRequiredAspects() {
        return Optional.of(Set.of("testAspect"));
      }

      @Override
      @Nonnull
      public List<ConsistencyIssue> check(
          @Nonnull CheckContext ctx, @Nonnull Map<Urn, EntityResponse> entityResponses) {
        return List.of();
      }
    };
  }

  private ConsistencyCheck createRequiresAllAspectsCheck(
      String id, String name, String entityType) {
    return new ConsistencyCheck() {
      @Override
      @Nonnull
      public String getId() {
        return id;
      }

      @Override
      @Nonnull
      public String getName() {
        return name;
      }

      @Override
      @Nonnull
      public String getDescription() {
        return "Check that requires all aspects";
      }

      @Override
      @Nonnull
      public String getEntityType() {
        return entityType;
      }

      @Override
      @Nonnull
      public Optional<Set<String>> getRequiredAspects() {
        return Optional.empty(); // Empty optional = requires all aspects
      }

      @Override
      @Nonnull
      public List<ConsistencyIssue> check(
          @Nonnull CheckContext ctx, @Nonnull Map<Urn, EntityResponse> entityResponses) {
        return List.of();
      }
    };
  }

  // ============================================================================
  // Test Configuration
  // ============================================================================

  @SpringBootConfiguration
  @Import({
    ConsistencyControllerTestConfig.class,
    TracingInterceptor.class,
    GlobalControllerExceptionHandler.class
  })
  @ComponentScan(basePackages = {"io.datahubproject.openapi.operations.consistency"})
  static class TestConfig {}

  @TestConfiguration
  public static class ConsistencyControllerTestConfig {
    @MockBean public ConsistencyService consistencyService;

    @Bean
    public ObjectMapper objectMapper() {
      return new ObjectMapper();
    }

    @Bean(name = "systemOperationContext")
    public OperationContext systemOperationContext(ObjectMapper objectMapper) {
      SystemTelemetryContext systemTelemetryContext = mock(SystemTelemetryContext.class);
      return TestOperationContexts.systemContextTraceNoSearchAuthorization(
          () -> ObjectMapperContext.builder().objectMapper(objectMapper).build(),
          () -> systemTelemetryContext);
    }

    @Bean
    public EntityRegistry entityRegistry(
        @Qualifier("systemOperationContext") OperationContext systemOperationContext) {
      return systemOperationContext.getEntityRegistry();
    }

    @Bean
    @Primary
    public SystemTelemetryContext traceContext(
        @Qualifier("systemOperationContext") OperationContext systemOperationContext) {
      return systemOperationContext.getSystemTelemetryContext();
    }

    @Bean
    public DataHubAppConfiguration dataHubAppConfiguration() {
      DataHubAppConfiguration config = new DataHubAppConfiguration();
      ConsistencyChecksConfiguration checksConfig = new ConsistencyChecksConfiguration();
      checksConfig.setGracePeriodSeconds(300L); // 5 minutes default
      config.setConsistencyChecks(checksConfig);
      return config;
    }

    @Bean
    @Primary
    public AuthorizerChain authorizerChain() {
      AuthorizerChain authorizerChain = mock(AuthorizerChain.class);

      Authentication authentication = mock(Authentication.class);
      when(authentication.getActor()).thenReturn(new Actor(ActorType.USER, "datahub"));
      when(authorizerChain.authorize(any()))
          .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.ALLOW, ""));
      AuthenticationContext.setAuthentication(authentication);

      return authorizerChain;
    }
  }
}
