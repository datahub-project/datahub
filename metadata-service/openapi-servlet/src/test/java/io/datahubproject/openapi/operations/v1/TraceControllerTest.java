package io.datahubproject.openapi.operations.v1;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.testng.Assert.assertNotNull;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.AuthorizerChain;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.systemmetadata.TraceService;
import io.datahubproject.metadata.context.ObjectMapperContext;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.TraceContext;
import io.datahubproject.openapi.config.TracingInterceptor;
import io.datahubproject.openapi.v1.models.TraceRequestV1;
import io.datahubproject.openapi.v1.models.TraceResponseV1;
import io.datahubproject.openapi.v1.models.TraceStatus;
import io.datahubproject.openapi.v1.models.TraceStorageStatus;
import io.datahubproject.openapi.v1.models.TraceWriteStatus;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Map;
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
import org.testng.annotations.Test;

@SpringBootTest(classes = TraceControllerTest.TestConfig.class)
@AutoConfigureWebMvc
@AutoConfigureMockMvc
public class TraceControllerTest extends AbstractTestNGSpringContextTests {
  public static final TraceStatus OK =
      TraceStatus.builder()
          .success(true)
          .primaryStorage(TraceStorageStatus.ok(TraceWriteStatus.ACTIVE_STATE))
          .searchStorage(TraceStorageStatus.ok(TraceWriteStatus.ACTIVE_STATE))
          .build();

  @Autowired private TraceController traceController;

  @Autowired private MockMvc mockMvc;

  @Autowired private TraceService mockTraceService;

  @Test
  public void initTest() {
    assertNotNull(traceController);
  }

  @Test
  public void testGetTrace() throws Exception {
    // Test URNs
    Urn TEST_URN_1 =
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hdfs,/path/to/data,PROD)");
    Urn TEST_URN_2 =
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)");

    // Mock trace service response
    TraceResponseV1 mockTraceResult =
        new TraceResponseV1(
            Map.of(
                TEST_URN_1, Map.of("status", OK, "datasetProperties", OK),
                TEST_URN_2, Map.of("datasetProperties", OK)));

    when(mockTraceService.trace(
            any(OperationContext.class), eq("trace123"), any(), eq(false), eq(false), eq(false)))
        .thenReturn(mockTraceResult);

    // Create test request body
    TraceRequestV1 requestBody = new TraceRequestV1();
    requestBody.put(TEST_URN_1, List.of("status", "datasetProperties"));
    requestBody.put(TEST_URN_2, List.of("datasetProperties"));

    // Test the trace endpoint
    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/v1/trace/write/test-trace123-id")
                .param("onlyIncludeErrors", "false")
                .content(new ObjectMapper().writeValueAsString(requestBody))
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andDo(
            result -> {
              String responseContent = result.getResponse().getContentAsString();
              System.out.println("Response content: " + responseContent);
            })
        .andExpect(
            MockMvcResultMatchers.jsonPath(
                    "$['urn:li:dataset:(urn:li:dataPlatform:hdfs,/path/to/data,PROD)'].datasetProperties.success")
                .value(true))
        .andExpect(
            MockMvcResultMatchers.jsonPath(
                    "$['urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)'].datasetProperties.success")
                .value(true))
        .andExpect(
            MockMvcResultMatchers.jsonPath(
                    "$['urn:li:dataset:(urn:li:dataPlatform:hdfs,/path/to/data,PROD)'].status.success")
                .value(true))
        .andExpect(
            MockMvcResultMatchers.jsonPath(
                    "$['urn:li:dataset:(urn:li:dataPlatform:hdfs,/path/to/data,PROD)'].datasetProperties.primaryStorage.writeStatus")
                .value("ACTIVE_STATE"))
        .andExpect(
            MockMvcResultMatchers.jsonPath(
                    "$['urn:li:dataset:(urn:li:dataPlatform:hdfs,/path/to/data,PROD)'].datasetProperties.searchStorage.writeStatus")
                .value("ACTIVE_STATE"));
  }

  @Test
  public void testGetTraceWithCustomParameters() throws Exception {
    // Test URN
    Urn TEST_URN = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hdfs,/path/to/data,PROD)");

    // Mock trace service response
    TraceResponseV1 mockTraceResult =
        new TraceResponseV1(
            Map.of(
                TEST_URN,
                Map.of(
                    "status",
                    TraceStatus.builder()
                        .success(false)
                        .primaryStorage(
                            TraceStorageStatus.fail(TraceWriteStatus.ERROR, "Mock test error"))
                        .searchStorage(
                            TraceStorageStatus.fail(
                                TraceWriteStatus.ERROR, "Failed to write to primary storage"))
                        .build(),
                    "datasetProfile",
                    TraceStatus.builder()
                        .success(true)
                        .primaryStorage(TraceStorageStatus.ok(TraceWriteStatus.NO_OP))
                        .searchStorage(
                            TraceStorageStatus.ok(TraceWriteStatus.TRACE_NOT_IMPLEMENTED))
                        .build())));

    when(mockTraceService.trace(
            any(OperationContext.class),
            eq("trace123"),
            any(),
            eq(false), // onlyIncludeErrors = false
            eq(true), // detailed = true
            eq(true) // skipCache = true
            ))
        .thenReturn(mockTraceResult);

    // Create test request body
    TraceRequestV1 requestBody = new TraceRequestV1();
    requestBody.put(TEST_URN, List.of("status", "datasetProfile"));

    // Test the trace endpoint with custom parameters
    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/v1/trace/write/test-trace123-id")
                .param("onlyIncludeErrors", "false")
                .param("detailed", "true")
                .param("skipCache", "true")
                .content(new ObjectMapper().writeValueAsString(requestBody))
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andDo(
            result -> {
              String responseContent = result.getResponse().getContentAsString();
              System.out.println("Response content: " + responseContent);
            })
        .andExpect(
            MockMvcResultMatchers.jsonPath(
                    "$['urn:li:dataset:(urn:li:dataPlatform:hdfs,/path/to/data,PROD)'].status.success")
                .value(false))
        .andExpect(
            MockMvcResultMatchers.jsonPath(
                    "$['urn:li:dataset:(urn:li:dataPlatform:hdfs,/path/to/data,PROD)'].datasetProfile.success")
                .value(true))
        .andExpect(
            MockMvcResultMatchers.jsonPath(
                    "$['urn:li:dataset:(urn:li:dataPlatform:hdfs,/path/to/data,PROD)'].status.primaryStorage.writeStatus")
                .value("ERROR"))
        .andExpect(
            MockMvcResultMatchers.jsonPath(
                    "$['urn:li:dataset:(urn:li:dataPlatform:hdfs,/path/to/data,PROD)'].status.primaryStorage.writeMessage")
                .value("Mock test error"))
        .andExpect(
            MockMvcResultMatchers.jsonPath(
                    "$['urn:li:dataset:(urn:li:dataPlatform:hdfs,/path/to/data,PROD)'].status.searchStorage.writeStatus")
                .value("ERROR"))
        .andExpect(
            MockMvcResultMatchers.jsonPath(
                    "$['urn:li:dataset:(urn:li:dataPlatform:hdfs,/path/to/data,PROD)'].status.searchStorage.writeMessage")
                .value("Failed to write to primary storage"))
        .andExpect(
            MockMvcResultMatchers.jsonPath(
                    "$['urn:li:dataset:(urn:li:dataPlatform:hdfs,/path/to/data,PROD)'].datasetProfile.primaryStorage.writeStatus")
                .value("NO_OP"))
        .andExpect(
            MockMvcResultMatchers.jsonPath(
                    "$['urn:li:dataset:(urn:li:dataPlatform:hdfs,/path/to/data,PROD)'].datasetProfile.searchStorage.writeStatus")
                .value("TRACE_NOT_IMPLEMENTED"));
  }

  @SpringBootConfiguration
  @Import({TraceControllerTestConfig.class, TracingInterceptor.class})
  @ComponentScan(basePackages = {"io.datahubproject.openapi.operations.v1"})
  static class TestConfig {}

  @TestConfiguration
  public static class TraceControllerTestConfig {
    @MockBean public TraceService traceService;

    @Bean
    public ObjectMapper objectMapper() {
      return new ObjectMapper();
    }

    @Bean(name = "systemOperationContext")
    public OperationContext systemOperationContext(ObjectMapper objectMapper) {
      TraceContext traceContext = mock(TraceContext.class);
      return TestOperationContexts.systemContextTraceNoSearchAuthorization(
          () -> ObjectMapperContext.builder().objectMapper(objectMapper).build(),
          () -> traceContext);
    }

    @Bean
    public EntityRegistry entityRegistry(
        @Qualifier("systemOperationContext") OperationContext systemOperationContext) {
      return systemOperationContext.getEntityRegistry();
    }

    @Bean
    @Primary
    public TraceContext traceContext(
        @Qualifier("systemOperationContext") OperationContext systemOperationContext) {
      return systemOperationContext.getTraceContext();
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
