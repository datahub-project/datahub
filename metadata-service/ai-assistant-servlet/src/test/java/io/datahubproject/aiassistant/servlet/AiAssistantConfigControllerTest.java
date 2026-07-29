package io.datahubproject.aiassistant.servlet;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthorizerChain;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.service.AiAssistantConfigService;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collections;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AiAssistantConfigControllerTest {

  private MockMvc mockMvc;
  private AiAssistantConfigService service;

  @BeforeMethod
  public void setup() {
    service = mock(AiAssistantConfigService.class);
    OperationContext systemOperationContext = TestOperationContexts.systemContextNoSearchAuthorization();
    AuthorizerChain authorizerChain = mock(AuthorizerChain.class);
    AuthenticationContext.setAuthentication(
        new Authentication(
            new Actor(ActorType.USER, "test"), "credentials", Collections.emptyMap()));
    AiAssistantConfigController controller =
        new AiAssistantConfigController(service, systemOperationContext, authorizerChain);
    mockMvc = MockMvcBuilders.standaloneSetup(controller).build();
  }

  @AfterMethod
  public void tearDown() {
    AuthenticationContext.remove();
  }

  @Test
  public void testPutApiKey() throws Exception {
    when(service.upsertProviderKey(
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.eq("claude"),
            org.mockito.ArgumentMatchers.eq("sk-ant-api03-1234")))
        .thenReturn(
            AiAssistantConfigService.ProviderKeyResult.builder()
                .provider("claude")
                .hasKey(true)
                .updated(true)
                .build());

    mockMvc
        .perform(
            put("/api/ai-config/api-key")
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    new ObjectMapper()
                        .writeValueAsString(
                            new AiAssistantConfigController.ProviderApiKeyRequest(
                                "claude", "sk-ant-api03-1234"))))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.provider").value("claude"))
        .andExpect(jsonPath("$.hasKey").value(true))
        .andExpect(jsonPath("$.updated").value(true));
  }

  @Test
  public void testGetApiKey() throws Exception {
    when(service.getProviderKey(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.eq("claude")))
        .thenReturn(
            AiAssistantConfigService.ProviderKeyResult.builder()
                .provider("claude")
                .hasKey(true)
                .updated(false)
                .build());

    mockMvc
        .perform(get("/api/ai-config/api-key").queryParam("provider", "claude"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.provider").value("claude"))
        .andExpect(jsonPath("$.hasKey").value(true))
        .andExpect(jsonPath("$.updated").value(false));
  }

  @Test
  public void testGetProviders() throws Exception {
    when(service.getProviders())
        .thenReturn(
            AiAssistantConfigService.ProvidersResult.builder()
                .providers(
                    java.util.List.of(
                        AiAssistantConfigService.Provider.CLAUDE,
                        AiAssistantConfigService.Provider.OPENAI))
                .build());

    mockMvc
        .perform(get("/api/ai-config/providers").accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.providers[0]").value("CLAUDE"))
        .andExpect(jsonPath("$.providers[1]").value("OPENAI"));
  }

  @Test
  public void testGetModels() throws Exception {
    when(service.getModels())
        .thenReturn(
            AiAssistantConfigService.ModelsResult.builder()
                .models(
                    java.util.List.of(
                        AiAssistantConfigService.Model.SONNET,
                        AiAssistantConfigService.Model.OPUS,
                        AiAssistantConfigService.Model.GPT_5_5))
                .build());

    mockMvc
        .perform(get("/api/ai-config/models").accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.models[0]").value("SONNET"))
        .andExpect(jsonPath("$.models[1]").value("OPUS"))
        .andExpect(jsonPath("$.models[2]").value("GPT_5_5"));
  }

  @Test
  public void testGetPreferredModel() throws Exception {
    when(service.getPreferredModel(org.mockito.ArgumentMatchers.any()))
        .thenReturn(
            AiAssistantConfigService.PreferredModelResult.builder()
                .model("claude-sonnet-5")
                .hasKey(true)
                .build());

    mockMvc
        .perform(get("/api/ai-config/preferred-model").accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.model").value("claude-sonnet-5"))
        .andExpect(jsonPath("$.hasKey").value(true));
  }

  @Test
  public void testPutPreferredModel() throws Exception {
    when(service.updatePreferredModel(
            org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.eq("gpt-5-5")))
        .thenReturn(
            AiAssistantConfigService.UpdatePreferredModelResult.builder()
                .model("gpt-5-5")
                .updated(true)
                .build());

    mockMvc
        .perform(
            put("/api/ai-config/preferred-model")
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    new ObjectMapper()
                        .writeValueAsString(
                            new AiAssistantConfigController.PreferredModelRequest("gpt-5-5"))))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.model").value("gpt-5-5"))
        .andExpect(jsonPath("$.updated").value(true));
  }
}
