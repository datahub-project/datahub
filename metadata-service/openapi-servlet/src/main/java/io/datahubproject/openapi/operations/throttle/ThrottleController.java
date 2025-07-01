package io.datahubproject.openapi.operations.throttle;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizerChain;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.gms.factory.entity.throttle.ManualThrottleSensor;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.entity.EntityServiceImpl;
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
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/openapi/operations/throttle")
@Slf4j
@Tag(name = "GMS Throttle Control", description = "An API for GMS throttle control.")
public class ThrottleController {

  private final OperationContext systemOperationContext;
  private final AuthorizerChain authorizerChain;
  private final EntityServiceImpl entityService;
  private final ObjectMapper objectMapper;
  private final ManualThrottleSensor manualThrottleSensor;

  public ThrottleController(
      @Qualifier("systemOperationContext") OperationContext systemOperationContext,
      EntityServiceImpl entityService,
      AuthorizerChain authorizerChain,
      ObjectMapper objectMapper,
      ManualThrottleSensor manualThrottleSensor) {
    this.systemOperationContext = systemOperationContext;
    this.authorizerChain = authorizerChain;
    this.entityService = entityService;
    this.objectMapper = objectMapper;
    this.manualThrottleSensor = manualThrottleSensor;
  }

  @Tag(name = "API Requests")
  @GetMapping(path = "/requests", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Get API Requests Throttle")
  public ResponseEntity<Map<String, Object>> getManualAPIRequestsThrottle(
      HttpServletRequest httpServletRequest) {
    Authentication authentication = AuthenticationContext.getAuthentication();
    String actorUrnStr = authentication.getActor().toUrnStr();

    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder()
                .buildOpenapi(
                    actorUrnStr, httpServletRequest, "getManualAPIRequestsThrottle", List.of()),
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

    return ResponseEntity.ok(
        objectMapper.convertValue(entityService.getThrottleEvents(), new TypeReference<>() {}));
  }

  @Tag(name = "API Requests")
  @PostMapping(path = "/requests/manual", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Set API Requests Manual Throttle")
  public ResponseEntity<Map<String, Object>> setAPIRequestManualThrottle(
      HttpServletRequest httpServletRequest, @RequestParam(name = "enabled") boolean enabled) {

    Authentication authentication = AuthenticationContext.getAuthentication();
    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder()
                .buildOpenapi(
                    authentication.getActor().toUrnStr(),
                    httpServletRequest,
                    "getManualAPIRequestsThrottle",
                    List.of()),
            authorizerChain,
            authentication,
            true);

    if (!AuthUtil.isAPIAuthorized(opContext, PoliciesConfig.MANAGE_SYSTEM_OPERATIONS_PRIVILEGE)) {
      return ResponseEntity.status(HttpStatus.FORBIDDEN).build();
    }

    manualThrottleSensor.setThrottle(enabled);

    return getManualAPIRequestsThrottle(httpServletRequest);
  }
}
