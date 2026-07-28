package io.datahubproject.aiassistant.servlet;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.service.AiAssistantConfigService;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AiAssistantConfigControllerTest {

  private MockMvc mockMvc;
  private AiAssistantConfigService service;

  @BeforeMethod
  public void setup() {
    service = mock(AiAssistantConfigService.class);
    AiAssistantConfigController controller = new AiAssistantConfigController(service);
    mockMvc = MockMvcBuilders.standaloneSetup(controller).build();
  }

  @Test
  public void testPutApiKey() throws Exception {
    when(service.upsertProviderKey("claude", "sk-ant-api03-1234"))
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
    when(service.getProviderKey("claude"))
        .thenReturn(
            AiAssistantConfigService.ProviderKeyResult.builder()
                .provider("claude")
                .hasKey(true)
                .updated(false)
                .keyPreview("sk-ant-...1234")
                .build());

    mockMvc
        .perform(get("/api/ai-config/api-key").queryParam("provider", "claude"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.provider").value("claude"))
        .andExpect(jsonPath("$.hasKey").value(true))
        .andExpect(jsonPath("$.updated").value(false))
        .andExpect(jsonPath("$.keyPreview").value("sk-ant-...1234"));
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
    when(service.getPreferredModel())
        .thenReturn(
            AiAssistantConfigService.PreferredModelResult.builder()
                .model("claude-sonnet-5")
                .hasKey(true)
                .keyPreview("sk-ant-...1234")
                .build());

    mockMvc
        .perform(get("/api/ai-config/preferred-model").accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.model").value("claude-sonnet-5"))
        .andExpect(jsonPath("$.hasKey").value(true))
        .andExpect(jsonPath("$.keyPreview").value("sk-ant-...1234"));
  }

  @Test
  public void testPutPreferredModelBadRequest() throws Exception {
    when(service.updatePreferredModel("nope"))
        .thenThrow(new IllegalArgumentException("Unsupported model 'nope'."));

    mockMvc
        .perform(
            put("/api/ai-config/preferred-model")
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    new ObjectMapper()
                        .writeValueAsString(
                            new AiAssistantConfigController.PreferredModelRequest("nope"))))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.error").value("Unsupported model 'nope'."));
  }
}
