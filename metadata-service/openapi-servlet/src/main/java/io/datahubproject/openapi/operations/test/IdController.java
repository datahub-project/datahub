package io.datahubproject.openapi.operations.test;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthorizerChain;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/openapi/operations/identity")
@Slf4j
@Tag(name = "Identity", description = "An API for checking identity")
public class IdController {
  private final AuthorizerChain authorizerChain;
  private final OperationContext systemOperationContext;

  public IdController(OperationContext systemOperationContext, AuthorizerChain authorizerChain) {
    this.systemOperationContext = systemOperationContext;
    this.authorizerChain = authorizerChain;
  }

  @Tag(name = "User")
  @GetMapping(path = "/user/urn", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "User id")
  public ResponseEntity<Map<String, String>> getUserId(
      HttpServletRequest request,
      @RequestParam(value = "skipCache", required = false, defaultValue = "false")
          Boolean skipCache) {
    Authentication authentication = AuthenticationContext.getAuthentication();
    String actorUrnStr = authentication.getActor().toUrnStr();

    OperationContext.asSession(
        systemOperationContext,
        RequestContext.builder().buildOpenapi(actorUrnStr, request, "getUserIdentity", List.of()),
        authorizerChain,
        authentication,
        true,
        skipCache);

    return ResponseEntity.ok(Map.of("urn", actorUrnStr));
  }
}
