package io.datahubproject.aiassistant.servlet;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthorizerChain;
import com.linkedin.metadata.service.AiAssistantConfigService;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.metadata.context.usage.UsageOperation;
import jakarta.servlet.http.HttpServletRequest;
import java.util.List;
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
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/ai-config")
public class AiAssistantConfigController {

  private final AiAssistantConfigService aiAssistantConfigService;
  private final OperationContext systemOperationContext;
  private final AuthorizerChain authorizerChain;

  public AiAssistantConfigController(
      AiAssistantConfigService aiAssistantConfigService,
      OperationContext systemOperationContext,
      AuthorizerChain authorizerChain) {
    this.aiAssistantConfigService = aiAssistantConfigService;
    this.systemOperationContext = systemOperationContext;
    this.authorizerChain = authorizerChain;
  }

  @PutMapping(path = "/api-key", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<?> upsertProviderApiKey(
      HttpServletRequest request, @RequestBody ProviderApiKeyRequest input) {
    try {
      return ResponseEntity.ok(
          aiAssistantConfigService.upsertProviderKey(
              buildSessionContext(request, "upsertProviderApiKey", UsageOperation.METADATA_WRITE),
              input.getProvider(),
              input.getApiKey()));
    } catch (IllegalArgumentException e) {
      return badRequest(e);
    }
  }

  @GetMapping(path = "/api-key", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<?> getProviderApiKey(
      HttpServletRequest request, @RequestParam("provider") String provider) {
    try {
      return ResponseEntity.ok(
          aiAssistantConfigService.getProviderKey(
              buildSessionContext(request, "getProviderApiKey", UsageOperation.METADATA_READ),
              provider));
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
    return ResponseEntity.ok(
        aiAssistantConfigService.getPreferredModel(
            buildSessionContext(request, "getPreferredModel", UsageOperation.METADATA_READ)));
  }

  @PutMapping(path = "/preferred-model", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<?> updatePreferredModel(
      HttpServletRequest request, @RequestBody PreferredModelRequest input) {
    try {
      return ResponseEntity.ok(
          aiAssistantConfigService.updatePreferredModel(
              buildSessionContext(request, "updatePreferredModel", UsageOperation.METADATA_WRITE),
              input.getModel()));
    } catch (IllegalArgumentException e) {
      return badRequest(e);
    }
  }

  private OperationContext buildSessionContext(
      HttpServletRequest request, String operationName, UsageOperation usageOperation) {
    final Authentication authentication = AuthenticationContext.getAuthentication();
    if (authentication == null) {
      throw new IllegalStateException("Authentication not found in request context.");
    }

    return OperationContext.asSession(
        systemOperationContext,
        RequestContext.builder()
            .buildOpenapi(authentication.getActor().toUrnStr(), request, operationName, List.of())
            .withUsageOperation(usageOperation),
        authorizerChain,
        authentication,
        true);
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
