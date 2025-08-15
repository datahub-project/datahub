package io.datahubproject.openapi.operations.v1;

import static com.linkedin.metadata.authorization.ApiOperation.READ;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizerChain;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.systemmetadata.TraceService;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.openapi.exception.UnauthorizedException;
import io.datahubproject.openapi.util.RequestInputUtil;
import io.datahubproject.openapi.v1.models.TraceRequestV1;
import io.datahubproject.openapi.v1.models.TraceResponseV1;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/openapi/v1/trace")
@Slf4j
@Tag(name = "Tracing", description = "An API for tracing async operations.")
public class TraceController {
  private final TraceService traceService;
  private final AuthorizerChain authorizerChain;
  private final OperationContext systemOperationContext;

  public TraceController(
      TraceService traceService,
      OperationContext systemOperationContext,
      AuthorizerChain authorizerChain) {
    this.traceService = traceService;
    this.systemOperationContext = systemOperationContext;
    this.authorizerChain = authorizerChain;
  }

  @Tag(name = "Async Write Tracing")
  @PostMapping(path = "/write/{traceId}", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      summary = "Trace an async write to the underlying storage.",
      requestBody =
          @io.swagger.v3.oas.annotations.parameters.RequestBody(
              required = true,
              content =
                  @Content(
                      mediaType = MediaType.APPLICATION_JSON_VALUE,
                      examples = {
                        @ExampleObject(
                            name = "Default",
                            value =
                                """
                    {
                      "urn:li:dataset:(urn:li:dataPlatform:hdfs,/path/to/data,PROD)": ["status", "datasetProperties"],
                      "urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.adoption.pet_profiles,PROD)": ["datasetProperties"]
                    }
                    """)
                      })))
  public ResponseEntity<TraceResponseV1> getTrace(
      HttpServletRequest request,
      @PathVariable("traceId") String traceId,
      @RequestParam(value = "onlyIncludeErrors", defaultValue = "true") boolean onlyIncludeErrors,
      @RequestParam(value = "detailed", defaultValue = "false") boolean detailed,
      @RequestParam(value = "skipCache", defaultValue = "false") boolean skipCache,
      @RequestBody @Nonnull TraceRequestV1 traceRequestV1) {
    Authentication authentication = AuthenticationContext.getAuthentication();
    String actorUrnStr = authentication.getActor().toUrnStr();

    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder().buildOpenapi(actorUrnStr, request, "getTrace", List.of()),
            authorizerChain,
            authentication,
            true);

    if (!AuthUtil.isAPIAuthorizedEntityUrns(opContext, READ, traceRequestV1.keySet())) {
      throw new UnauthorizedException(
          authentication.getActor().toUrnStr()
              + " is unauthorized to "
              + READ
              + " as least one of the requested URNs.");
    }

    LinkedHashMap<Urn, List<String>> normalizedInput =
        traceRequestV1.entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    e ->
                        RequestInputUtil.resolveAspectNames(
                            opContext.getEntityRegistry(), e.getKey(), e.getValue(), true),
                    (v1, v2) -> v1,
                    LinkedHashMap::new));

    return ResponseEntity.ok(
        new TraceResponseV1(
            traceService.trace(
                opContext,
                extractTraceId(traceId),
                normalizedInput,
                onlyIncludeErrors,
                detailed,
                skipCache)));
  }

  private static String extractTraceId(String input) {
    if (input == null || input.trim().isEmpty()) {
      return null;
    }

    // Clean the input
    input = input.trim();

    // Case 1: If it's a full traceparent header (containing hyphens)
    if (input.contains("-")) {
      String[] parts = input.split("-");
      if (parts.length >= 2) {
        // The trace ID is the second part (index 1)
        return parts[1];
      }
      return null;
    }

    // Case 2: If it's just the trace ID (32 hex characters)
    if (input.length() == 32 && input.matches("[0-9a-fA-F]+")) {
      return input;
    }

    return null;
  }
}
