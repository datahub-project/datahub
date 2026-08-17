package io.datahubproject.openapi.openlineage.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthorizerChain;
import com.linkedin.common.FabricType;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.authorization.EntityAuthorizationUtils;
import com.linkedin.metadata.entity.EntityServiceImpl;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.openapi.config.GlobalControllerExceptionHandler;
import io.datahubproject.openapi.openlineage.exception.OpenLineageControllerExceptionHandler;
import io.datahubproject.openapi.openlineage.mapping.RunEventMapper;
import io.datahubproject.openapi.openlineage.validation.JsonSchemaOpenLineageRequestValidator;
import io.datahubproject.openapi.openlineage.validation.OpenLineageRequestValidator;
import io.datahubproject.openapi.openlineage.validation.OpenLineageSchemaCatalog;
import io.datahubproject.openlineage.config.DatahubOpenlineageConfig;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.openlineage.client.OpenLineage;
import jakarta.servlet.http.HttpServletRequest;
import java.util.Collection;
import org.mockito.MockedStatic;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class LineageApiHttpTest {
  private MockMvc mockMvc;
  private LineageApiImpl controller;
  private EntityServiceImpl entityService;
  private RunEventMapper runEventMapper;

  @BeforeMethod
  public void setup() {
    runEventMapper = spy(new RunEventMapper());
    controller =
        new LineageApiImpl(
            new JsonSchemaOpenLineageRequestValidator(new OpenLineageSchemaCatalog()),
            new OpenLineageEventDeserializer(),
            runEventMapper);
    DatahubOpenlineageConfig config =
        DatahubOpenlineageConfig.builder()
            .fabricType(FabricType.PROD)
            .materializeDataset(true)
            .includeSchemaMetadata(true)
            .build();
    ReflectionTestUtils.setField(
        controller,
        "_mappingConfig",
        RunEventMapper.MappingConfig.builder().datahubConfig(config).build());
    entityService = mock(EntityServiceImpl.class);
    ReflectionTestUtils.setField(controller, "_entityService", entityService);
    ReflectionTestUtils.setField(controller, "_authorizerChain", mock(AuthorizerChain.class));
    ReflectionTestUtils.setField(
        controller,
        "systemOperationContext",
        TestOperationContexts.systemContextNoSearchAuthorization());
    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getRemoteAddr()).thenReturn("127.0.0.1");
    ReflectionTestUtils.setField(controller, "request", request);
    mockMvc =
        MockMvcBuilders.standaloneSetup(controller)
            .setControllerAdvice(
                new OpenLineageControllerExceptionHandler(), new GlobalControllerExceptionHandler())
            .build();
    authenticate();
  }

  @Test
  public void testInvalidEventHasStructured400Body() throws Exception {
    mockMvc
        .perform(
            post("/openapi/openlineage/api/v1/lineage")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"producer\":\"https://example.com/tool\"}"))
        .andExpect(status().isBadRequest())
        .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.code").value("INVALID_EVENT"))
        .andExpect(jsonPath("$.details.errors").isArray());
    assertNoValidationSideEffects();
  }

  @Test
  public void testAbsentBodyHasStructured400Body() throws Exception {
    mockMvc
        .perform(
            post("/openapi/openlineage/api/v1/lineage").contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().isBadRequest())
        .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.code").value("INVALID_EVENT"));
    assertNoValidationSideEffects();
  }

  @Test
  public void testStringBodyHasStructured400Body() throws Exception {
    mockMvc
        .perform(
            post("/openapi/openlineage/api/v1/lineage")
                .contentType(MediaType.APPLICATION_JSON)
                .content("\"not-an-event\""))
        .andExpect(status().isBadRequest())
        .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.code").value("INVALID_EVENT"));
    assertNoValidationSideEffects();
  }

  @Test
  public void testMixedRootHasStructured400Body() throws Exception {
    mockMvc
        .perform(
            post("/openapi/openlineage/api/v1/lineage")
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    validJobEvent().substring(0, validJobEvent().length() - 1)
                        + ",\"dataset\":{\"namespace\":\"snowflake\",\"name\":\"db.table\"}}"))
        .andExpect(status().isBadRequest())
        .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.code").value("INVALID_EVENT"));
    assertNoValidationSideEffects();
  }

  @Test
  public void testNullRootWithValidJobHasStructured400Body() throws Exception {
    mockMvc
        .perform(
            post("/openapi/openlineage/api/v1/lineage")
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    validJobEvent().substring(0, validJobEvent().length() - 1) + ",\"run\":null}"))
        .andExpect(status().isBadRequest())
        .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.code").value("INVALID_EVENT"))
        .andExpect(jsonPath("$.details.errors").isArray());
    assertNoValidationSideEffects();
  }

  @Test
  public void testNonObjectRootHasStructured400Body() throws Exception {
    mockMvc
        .perform(
            post("/openapi/openlineage/api/v1/lineage")
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    validJobEvent()
                        .replace(
                            "\"job\":{\"namespace\":\"crm\",\"name\":\"load.customer\"}",
                            "\"job\":\"load.customer\"")))
        .andExpect(status().isBadRequest())
        .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.code").value("INVALID_EVENT"))
        .andExpect(jsonPath("$.details.errors").isArray());
    assertNoValidationSideEffects();
  }

  @Test
  public void testDuplicateAndTrailingJsonHaveStructured400Bodies() throws Exception {
    String duplicateProducer =
        validJobEvent()
            .replace(
                "\"producer\":\"https://example.com/my-pipeline-tool\"",
                "\"producer\":\"https://example.com/my-pipeline-tool\","
                    + "\"producer\":\"https://example.com/duplicate\"");
    mockMvc
        .perform(
            post("/openapi/openlineage/api/v1/lineage")
                .contentType(MediaType.APPLICATION_JSON)
                .content(duplicateProducer))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.details.errors[0].rule").value("duplicateKey"));
    assertNoValidationSideEffects();

    mockMvc
        .perform(
            post("/openapi/openlineage/api/v1/lineage")
                .contentType(MediaType.APPLICATION_JSON)
                .content(validJobEvent() + "{}"))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.details.errors[0].rule").value("trailingContent"));
    assertNoValidationSideEffects();
  }

  @Test
  public void testUnsupportedContentTypeReturnsStructured415WithoutSideEffects() throws Exception {
    for (String contentType :
        new String[] {MediaType.TEXT_PLAIN_VALUE, "*/*", "application/*", "not a media type"}) {
      mockMvc
          .perform(
              post("/openapi/openlineage/api/v1/lineage")
                  .header(HttpHeaders.CONTENT_TYPE, contentType)
                  .content(validJobEvent()))
          .andExpect(status().isUnsupportedMediaType())
          .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
          .andExpect(jsonPath("$.code").value("UNSUPPORTED_MEDIA_TYPE"))
          .andExpect(jsonPath("$.message").isNotEmpty())
          .andExpect(jsonPath("$.details").isMap());
    }
    mockMvc
        .perform(post("/openapi/openlineage/api/v1/lineage").content(validJobEvent()))
        .andExpect(status().isUnsupportedMediaType())
        .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.code").value("UNSUPPORTED_MEDIA_TYPE"));
    assertNoValidationSideEffects();
  }

  @Test
  public void testMissingAuthenticationHasStructured401Body() throws Exception {
    AuthenticationContext.setAuthentication(null);

    mockMvc
        .perform(
            post("/openapi/openlineage/api/v1/lineage")
                .contentType(MediaType.APPLICATION_JSON)
                .content(validJobEvent()))
        .andExpect(status().isUnauthorized())
        .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.code").value("AUTHENTICATION_REQUIRED"));
    verifyNoInteractions(runEventMapper, entityService);
  }

  @Test
  public void testAuthorizedIngestReturns202() throws Exception {
    try (MockedStatic<EntityAuthorizationUtils> authorization =
        mockStatic(EntityAuthorizationUtils.class)) {
      authorization
          .when(() -> EntityAuthorizationUtils.isAPIAuthorizedBatchItems(any(), any()))
          .thenAnswer(
              invocation -> {
                Collection<? extends BatchItem> items = invocation.getArgument(1);
                return items.stream()
                    .map(item -> Pair.of((BatchItem) item, org.apache.http.HttpStatus.SC_OK))
                    .toList();
              });

      mockMvc
          .perform(
              post("/openapi/openlineage/api/v1/lineage")
                  .contentType(MediaType.APPLICATION_JSON)
                  .content(validJobEvent()))
          .andExpect(status().isAccepted());
    }
    verify(entityService)
        .ingestProposal(any(OperationContext.class), any(AspectsBatch.class), eq(true));
  }

  @Test
  public void testDeniedIngestHasStructured403WithoutIngestion() throws Exception {
    try (MockedStatic<EntityAuthorizationUtils> authorization =
        mockStatic(EntityAuthorizationUtils.class)) {
      authorization
          .when(() -> EntityAuthorizationUtils.isAPIAuthorizedBatchItems(any(), any()))
          .thenAnswer(
              invocation -> {
                Collection<? extends BatchItem> items = invocation.getArgument(1);
                return items.stream()
                    .map(item -> Pair.of((BatchItem) item, org.apache.http.HttpStatus.SC_FORBIDDEN))
                    .toList();
              });

      mockMvc
          .perform(
              post("/openapi/openlineage/api/v1/lineage")
                  .contentType(MediaType.APPLICATION_JSON)
                  .content(validJobEvent()))
          .andExpect(status().isForbidden())
          .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
          .andExpect(jsonPath("$.code").value("AUTHORIZATION_DENIED"))
          .andExpect(jsonPath("$.message").isNotEmpty())
          .andExpect(jsonPath("$.details").isMap());
    }
    verifyNoInteractions(entityService);
  }

  @Test
  public void testUnexpectedValidatorFailureHasStructured500BodyWithoutSideEffects()
      throws Exception {
    OpenLineageRequestValidator failingValidator = mock(OpenLineageRequestValidator.class);
    when(failingValidator.validate(any())).thenThrow(new IllegalStateException("validator failed"));
    ReflectionTestUtils.setField(controller, "requestValidator", failingValidator);

    mockMvc
        .perform(
            post("/openapi/openlineage/api/v1/lineage")
                .contentType(MediaType.APPLICATION_JSON)
                .content(validJobEvent()))
        .andExpect(status().isInternalServerError())
        .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.code").value("INGESTION_FAILED"));
    assertNoValidationSideEffects();
  }

  @Test
  public void testMapperIllegalArgumentFailureHasStructured500Body() throws Exception {
    doThrow(new IllegalArgumentException("mapper failed"))
        .when(runEventMapper)
        .map(any(OpenLineage.JobEvent.class), any(RunEventMapper.MappingConfig.class));

    mockMvc
        .perform(
            post("/openapi/openlineage/api/v1/lineage")
                .contentType(MediaType.APPLICATION_JSON)
                .content(validJobEvent()))
        .andExpect(status().isInternalServerError())
        .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.code").value("INGESTION_FAILED"));
    verifyNoInteractions(entityService);
  }

  @Test
  public void testAsyncIngestFailureHasStructured500Body() throws Exception {
    when(entityService.ingestProposal(
            any(OperationContext.class), any(AspectsBatch.class), eq(true)))
        .thenThrow(
            new IllegalStateException("Asynchronous ingestion is disabled in read-only mode"));

    mockMvc
        .perform(
            post("/openapi/openlineage/api/v1/lineage")
                .contentType(MediaType.APPLICATION_JSON)
                .content(validJobEvent()))
        .andExpect(status().isInternalServerError())
        .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.code").value("INGESTION_FAILED"))
        .andExpect(jsonPath("$.details.exception").isNotEmpty());
  }

  private void assertNoValidationSideEffects() {
    verifyNoInteractions(runEventMapper, entityService);
  }

  private static void authenticate() {
    Authentication authentication = mock(Authentication.class);
    when(authentication.getActor()).thenReturn(new Actor(ActorType.USER, "testuser"));
    AuthenticationContext.setAuthentication(authentication);
  }

  private static String validJobEvent() {
    return "{"
        + "\"eventTime\":\"2026-04-14T10:00:00Z\","
        + "\"producer\":\"https://example.com/my-pipeline-tool\","
        + "\"schemaURL\":\"https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/JobEvent\","
        + "\"job\":{\"namespace\":\"crm\",\"name\":\"load.customer\"},"
        + "\"inputs\":[],\"outputs\":[]}";
  }
}
