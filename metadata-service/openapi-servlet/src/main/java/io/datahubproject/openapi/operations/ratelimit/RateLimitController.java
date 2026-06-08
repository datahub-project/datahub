package io.datahubproject.openapi.operations.ratelimit;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizerChain;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.ratelimit.RateLimitEngine;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/openapi/v1/rate-limits")
@Slf4j
@Tag(
    name = "GMS Service Rate Limits",
    description =
        "Read-only GMS HTTP service rate limit config and live status. Not MCP ingestion or Kafka lag throttle.")
public class RateLimitController {

  private final OperationContext systemOperationContext;
  private final AuthorizerChain authorizerChain;
  private final RateLimitEngine rateLimitEngine;

  public RateLimitController(
      @Qualifier("systemOperationContext") OperationContext systemOperationContext,
      AuthorizerChain authorizerChain,
      RateLimitEngine rateLimitEngine) {
    this.systemOperationContext = systemOperationContext;
    this.authorizerChain = authorizerChain;
    this.rateLimitEngine = rateLimitEngine;
  }

  @GetMapping(path = "/config", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Get effective rate limit configuration")
  public ResponseEntity<?> getConfig(HttpServletRequest httpServletRequest) {
    ResponseEntity<?> authFailure = authorize(httpServletRequest, "getRateLimitConfig");
    if (authFailure != null) {
      return authFailure;
    }
    return ResponseEntity.ok(rateLimitEngine.getConfig());
  }

  @GetMapping(path = "/status", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Get live rate limit status for this pod")
  public ResponseEntity<?> getStatus(HttpServletRequest httpServletRequest) {
    ResponseEntity<?> authFailure = authorize(httpServletRequest, "getRateLimitStatus");
    if (authFailure != null) {
      return authFailure;
    }
    return ResponseEntity.ok(rateLimitEngine.statusSnapshot());
  }

  private ResponseEntity<?> authorize(HttpServletRequest httpServletRequest, String operation) {
    Authentication authentication = AuthenticationContext.getAuthentication();
    String actorUrnStr = authentication.getActor().toUrnStr();

    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder()
                .buildOpenapi(actorUrnStr, httpServletRequest, operation, List.of()),
            authorizerChain,
            authentication,
            true);

    if (!AuthUtil.isAPIAuthorized(opContext, PoliciesConfig.MANAGE_SYSTEM_OPERATIONS_PRIVILEGE)) {
      return ResponseEntity.status(HttpStatus.FORBIDDEN)
          .body(
              Map.of(
                  "error",
                  String.format(actorUrnStr + " is not authorized for system operations.")));
    }
    return null;
  }
}
