package io.datahubproject.openapi.operations.analytics;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizerChain;
import com.linkedin.metadata.analytics.compaction.AnalyticsCompactionRequest;
import com.linkedin.metadata.analytics.compaction.AnalyticsCompactionResult;
import com.linkedin.metadata.analytics.compaction.AnalyticsCompactionService;
import com.linkedin.metadata.authorization.PoliciesConfig;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.metadata.context.usage.UsageOperation;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/openapi/operations/analytics")
@Slf4j
@Tag(
    name = "Analytics operations",
    description = "Store-agnostic analytics maintenance. Compaction backend is pgAnalytics today.")
public class AnalyticsCompactionController {

  private final OperationContext systemOperationContext;
  private final AuthorizerChain authorizerChain;

  @Autowired(required = false)
  private AnalyticsCompactionService analyticsCompactionService;

  @Value("${analytics.compact.maxHoursToSeal:6}")
  private int defaultMaxHoursToSeal;

  @Value("${analytics.compact.maxDaysToCompact:2}")
  private int defaultMaxDaysToCompact;

  @Value("${analytics.compact.maxMonthsToCompact:1}")
  private int defaultMaxMonthsToCompact;

  @Value("${analytics.compact.maxWallClockMillis:30000}")
  private long defaultMaxWallClockMillis;

  public AnalyticsCompactionController(
      @Qualifier("systemOperationContext") OperationContext systemOperationContext,
      AuthorizerChain authorizerChain) {
    this.systemOperationContext = systemOperationContext;
    this.authorizerChain = authorizerChain;
  }

  @PostMapping(
      path = "/compact",
      consumes = MediaType.APPLICATION_JSON_VALUE,
      produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      summary =
          "Run progressive analytics compaction (hour→day→month) with work budgets. "
              + "Returns 503 when no compaction backend is registered.")
  public ResponseEntity<?> compact(
      HttpServletRequest httpServletRequest,
      @RequestBody(required = false) @Nullable AnalyticsCompactRequestBody body) {
    if (!authorize(httpServletRequest)) {
      return forbidden();
    }
    if (analyticsCompactionService == null) {
      return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
          .body(Map.of("error", "Analytics compaction backend is not available"));
    }

    AnalyticsCompactionRequest request = toRequest(body);
    AnalyticsCompactionResult result = analyticsCompactionService.compact(request);
    AnalyticsCompactResponseBody response = AnalyticsCompactResponseBody.from(result);
    if (result.isFailed()) {
      return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
    }
    return ResponseEntity.ok(response);
  }

  private AnalyticsCompactionRequest toRequest(@Nullable AnalyticsCompactRequestBody body) {
    AnalyticsCompactionRequest.AnalyticsCompactionRequestBuilder b =
        AnalyticsCompactionRequest.builder()
            .maxHoursToSeal(defaultMaxHoursToSeal)
            .maxDaysToCompact(defaultMaxDaysToCompact)
            .maxMonthsToCompact(defaultMaxMonthsToCompact)
            .maxWallClockMillis(defaultMaxWallClockMillis);
    if (body == null) {
      return b.build();
    }
    if (body.getMaxHoursToSeal() != null) {
      b.maxHoursToSeal(body.getMaxHoursToSeal());
    }
    if (body.getMaxDaysToCompact() != null) {
      b.maxDaysToCompact(body.getMaxDaysToCompact());
    }
    if (body.getMaxMonthsToCompact() != null) {
      b.maxMonthsToCompact(body.getMaxMonthsToCompact());
    }
    if (body.getMaxWallClockMillis() != null) {
      b.maxWallClockMillis(body.getMaxWallClockMillis());
    }
    if (body.getHourLookbackHours() != null) {
      b.hourLookbackHours(body.getHourLookbackHours());
    }
    if (body.getDayLookbackDays() != null) {
      b.dayLookbackDays(body.getDayLookbackDays());
    }
    if (body.getMonthLookbackMonths() != null) {
      b.monthLookbackMonths(body.getMonthLookbackMonths());
    }
    return b.build();
  }

  private boolean authorize(HttpServletRequest httpServletRequest) {
    Authentication authentication = AuthenticationContext.getAuthentication();
    String actorUrnStr = authentication.getActor().toUrnStr();
    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder()
                .buildOpenapi(
                    actorUrnStr, httpServletRequest, "analyticsCompact", java.util.List.of())
                .withUsageOperation(UsageOperation.OTHER_OPERATIONS),
            authorizerChain,
            authentication,
            true);
    return AuthUtil.isAPIAuthorized(opContext, PoliciesConfig.MANAGE_SYSTEM_OPERATIONS_PRIVILEGE);
  }

  private static ResponseEntity<?> forbidden() {
    Authentication authentication = AuthenticationContext.getAuthentication();
    String actorUrnStr = authentication.getActor().toUrnStr();
    return ResponseEntity.status(HttpStatus.FORBIDDEN)
        .body(Map.of("error", actorUrnStr + " is unauthorized to manage system operations."));
  }
}
