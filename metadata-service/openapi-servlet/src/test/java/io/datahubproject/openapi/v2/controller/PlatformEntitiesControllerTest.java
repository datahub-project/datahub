package io.datahubproject.openapi.v2.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthorizerChain;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.IngestResult;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.search.client.CachingEntitySearchService;
import io.datahubproject.openapi.config.GlobalControllerExceptionHandler;
import io.datahubproject.openapi.test.AuthorizerChainTestSupport;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PlatformEntitiesControllerTest {

  private static final String DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:hive,table,PROD)";

  private MockMvc mockMvc;
  private EntityService<ChangeItemImpl> entityService;
  private MockedStatic<AuthenticationContext> authContextMock;

  @BeforeMethod
  @SuppressWarnings("unchecked")
  public void setup() {
    entityService = mock(EntityService.class);
    AuthorizerChain authorizerChain = mock(AuthorizerChain.class);
    AuthorizerChainTestSupport.stubAllowAllPaths(authorizerChain);

    Authentication authentication = mock(Authentication.class);
    when(authentication.getActor()).thenReturn(TestOperationContexts.TEST_USER_AUTH.getActor());
    authContextMock = Mockito.mockStatic(AuthenticationContext.class);
    authContextMock.when(AuthenticationContext::getAuthentication).thenReturn(authentication);

    PlatformEntitiesController controller =
        new PlatformEntitiesController(
            TestOperationContexts.systemContextNoSearchAuthorization(),
            entityService,
            mock(CachingEntitySearchService.class),
            new ObjectMapper(),
            authorizerChain);
    mockMvc =
        MockMvcBuilders.standaloneSetup(controller)
            .setControllerAdvice(new GlobalControllerExceptionHandler())
            .build();
  }

  @AfterMethod
  public void tearDown() {
    authContextMock.close();
  }

  @Test
  public void testValidUpsertReturnsCreated() throws Exception {
    when(entityService.ingestProposal(any(), any(AspectsBatch.class), anyBoolean()))
        .thenReturn(
            List.of(
                IngestResult.builder()
                    .urn(UrnUtils.getUrn(DATASET_URN))
                    .sqlCommitted(true)
                    .build()));

    postProposal("UPSERT", "status").andExpect(status().isCreated());
  }

  @Test
  public void testUnsupportedChangeTypeReturnsBadRequest() throws Exception {
    postProposal("DELETE", "status").andExpect(status().isBadRequest());
    verify(entityService, never()).ingestProposal(any(), any(AspectsBatch.class), anyBoolean());
  }

  @Test
  public void testUnknownAspectReturnsUnprocessableEntity() throws Exception {
    postProposal("UPSERT", "notARealAspect").andExpect(status().isUnprocessableEntity());
    verify(entityService, never()).ingestProposal(any(), any(AspectsBatch.class), anyBoolean());
  }

  private ResultActions postProposal(String changeType, String aspectName) throws Exception {
    String body =
        String.format(
            "[{\"entityType\":\"dataset\",\"entityUrn\":\"%s\",\"changeType\":\"%s\","
                + "\"aspectName\":\"%s\",\"aspect\":{\"value\":{\"__type\":\"Status\",\"removed\":true},"
                + "\"contentType\":\"application/json\"}}]",
            DATASET_URN, changeType, aspectName);
    return mockMvc.perform(
        post("/openapi/v2/platform/entities/v1/")
            .contentType(MediaType.APPLICATION_JSON)
            .content(body));
  }
}
