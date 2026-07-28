package io.datahubproject.aiassistant.servlet;

import com.linkedin.metadata.service.AiAssistantConfigService;
import jakarta.servlet.http.HttpServletRequest;
import java.util.Map;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RequestParam;

@RestController
@RequestMapping("/api/ai-config")
public class AiAssistantConfigController {

  private final AiAssistantConfigService aiAssistantConfigService;

  public AiAssistantConfigController(AiAssistantConfigService aiAssistantConfigService) {
    this.aiAssistantConfigService = aiAssistantConfigService;
  }

  @PutMapping(path = "/api-key", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<?> upsertProviderApiKey(
      HttpServletRequest request, @RequestBody ProviderApiKeyRequest input) {
    try {
      return ResponseEntity.ok(
          aiAssistantConfigService.upsertProviderKey(input.getProvider(), input.getApiKey()));
    } catch (IllegalArgumentException e) {
      return badRequest(e);
    }
  }

  @GetMapping(path = "/api-key", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<?> getProviderApiKey(
      HttpServletRequest request, @RequestParam("provider") String provider) {
    try {
      return ResponseEntity.ok(aiAssistantConfigService.getProviderKey(provider));
    } catch (IllegalArgumentException e) {
      return badRequest(e);
    }
  }

  @GetMapping(path = "/providers", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<?> getProviders(HttpServletRequest request) {
    return ResponseEntity.ok(aiAssistantConfigService.getProviders());
  }

  @GetMapping(path = "/models", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<?> getModels(HttpServletRequest request) {
    return ResponseEntity.ok(aiAssistantConfigService.getModels());
  }

  @GetMapping(path = "/preferred-model", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<?> getPreferredModel(HttpServletRequest request) {
    return ResponseEntity.ok(aiAssistantConfigService.getPreferredModel());
  }

  @PutMapping(path = "/preferred-model", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<?> updatePreferredModel(
      HttpServletRequest request, @RequestBody PreferredModelRequest input) {
    try {
      return ResponseEntity.ok(aiAssistantConfigService.updatePreferredModel(input.getModel()));
    } catch (IllegalArgumentException e) {
      return badRequest(e);
    }
  }

  private static ResponseEntity<Map<String, String>> badRequest(IllegalArgumentException e) {
    return ResponseEntity.badRequest()
        .body(Map.of("error", Objects.requireNonNullElse(e.getMessage(), "Invalid request.")));
  }

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  public static class ProviderApiKeyRequest {
    private String provider;
    private String apiKey;
  }

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  public static class PreferredModelRequest {
    private String model;
  }
}
