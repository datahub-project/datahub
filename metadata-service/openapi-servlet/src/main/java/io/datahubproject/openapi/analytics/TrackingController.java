package io.datahubproject.openapi.analytics;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.telemetry.TrackingService;
import com.fasterxml.jackson.databind.JsonNode;
import io.datahubproject.metadata.context.OperationContext;
import jakarta.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/openapi/v1/tracking")
public class TrackingController {

  private final TrackingService trackingService;
  private final OperationContext systemOperationContext;

  public TrackingController(
      @Qualifier("trackingService") TrackingService trackingService,
      @Qualifier("systemOperationContext") OperationContext systemOperationContext) {
    this.trackingService = trackingService;
    this.systemOperationContext = systemOperationContext;
    log.info(
        "TrackingController initialized with trackingService: {}",
        trackingService != null ? "present" : "null");

    // Log Mixpanel configuration
    if (trackingService != null) {
      log.info("TrackingController is configured to use Mixpanel for analytics");
    }
  }

  @PostMapping("/track")
  public ResponseEntity<Void> trackEvent(HttpServletRequest request, @RequestBody JsonNode event) {
    if (event == null || !event.has("type")) {
      log.warn("Invalid tracking request: missing event type");
      return ResponseEntity.badRequest().build();
    }

    log.debug("Received tracking request with event type: {}", event.get("type"));

    Authentication authentication = AuthenticationContext.getAuthentication();
    if (authentication == null) {
      log.warn("Authentication not found in request");
      return ResponseEntity.status(401).build();
    }
    log.debug("Authentication verified for user: {}", authentication.getActor());

    try {
      log.debug("Forwarding tracking event to TrackingService");
      trackingService.track(event.get("type").asText(), systemOperationContext, null, null, event);
      log.debug("Successfully emitted analytics event");
      return ResponseEntity.ok().build();
    } catch (Exception e) {
      log.error("Failed to emit analytics event: {}", e.getMessage(), e);
      return ResponseEntity.internalServerError().build();
    }
  }
}
