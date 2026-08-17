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
import io.datahubproject.openapi.openlineage.exception.OpenLineageControllerExceptionHandler;
import io.datahubproject.openapi.openlineage.mapping.RunEventMapper;
import io.datahubproject.openapi.openlineage.validation.JsonSchemaOpenLineageRequestValidator;
import io.datahubproject.openapi.openlineage.validation.OpenLineageSchemaCatalog;
import io.datahubproject.openlineage.config.DatahubOpenlineageConfig;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.openlineage.client.OpenLineage;
import jakarta.servlet.http.HttpServletRequest;
import java.util.Collection;
import org.mockito.MockedStatic;
import org.springframework.http.MediaType;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class LineageApiHttpTest {
  private MockMvc mockMvc;
  private EntityServiceImpl entityService;
  private RunEventMapper runEventMapper;

  @BeforeMethod
  public void setup() {
    runEventMapper = spy(new RunEventMapper());
    LineageApiImpl controller =
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
            .setControllerAdvice(new OpenLineageControllerExceptionHandler())
            .build();
    Authentication authentication = mock(Authentication.class);
    when(authentication.getActor()).thenReturn(new Actor(ActorType.USER, "testuser"));
    AuthenticationContext.setAuthentication(authentication);
  }

  @Test
  public void testInvalidAndEmptyEventsHaveStructured400Bodies() throws Exception {
    mockMvc
        .perform(
            post("/openapi/openlineage/api/v1/lineage")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"producer\":\"https://example.com/tool\"}"))
        .andExpect(status().isBadRequest())
        .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.code").value("INVALID_EVENT"))
        .andExpect(jsonPath("$.details.errors").isArray());

    mockMvc
        .perform(
            post("/openapi/openlineage/api/v1/lineage").contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.code").value("INVALID_EVENT"));
    verifyNoInteractions(runEventMapper, entityService);
  }

  @Test
  public void testDuplicateJsonHasStructured400Body() throws Exception {
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
    verifyNoInteractions(runEventMapper, entityService);
  }

  @Test
  public void testUnsupportedContentTypeReturnsStructured415() throws Exception {
    mockMvc
        .perform(
            post("/openapi/openlineage/api/v1/lineage")
                .contentType(MediaType.TEXT_PLAIN)
                .content(validJobEvent()))
        .andExpect(status().isUnsupportedMediaType())
        .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.code").value("UNSUPPORTED_MEDIA_TYPE"));
    mockMvc
        .perform(
            post("/openapi/openlineage/api/v1/lineage")
                .header("Content-Type", "not a media type")
                .content(validJobEvent()))
        .andExpect(status().isUnsupportedMediaType())
        .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.code").value("UNSUPPORTED_MEDIA_TYPE"));
    mockMvc
        .perform(post("/openapi/openlineage/api/v1/lineage").content(validJobEvent()))
        .andExpect(status().isUnsupportedMediaType())
        .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.code").value("UNSUPPORTED_MEDIA_TYPE"));
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
  public void testDeniedIngestHasStructured403() throws Exception {
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
          .andExpect(jsonPath("$.code").value("AUTHORIZATION_DENIED"));
    }
    verifyNoInteractions(entityService);
  }

  @Test
  public void testMapperFailureHasStructured500Body() throws Exception {
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

  private static String validJobEvent() {
    return "{"
        + "\"eventTime\":\"2026-04-14T10:00:00Z\","
        + "\"producer\":\"https://example.com/my-pipeline-tool\","
        + "\"schemaURL\":\"https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/JobEvent\","
        + "\"job\":{\"namespace\":\"crm\",\"name\":\"load.customer\"},"
        + "\"inputs\":[],\"outputs\":[]}";
  }
}
