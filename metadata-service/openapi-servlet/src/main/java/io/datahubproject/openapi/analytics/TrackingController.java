package io.datahubproject.openapi.analytics;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthorizerChain;
import com.datahub.telemetry.TrackingService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.metadata.context.usage.UsageOperation;
import jakarta.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Locale;
import javax.annotation.Nullable;
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

  private static final String LANGUAGE_FIELD = "language";
  private static final String ACCEPT_LANGUAGE_HEADER = "Accept-Language";
  private static final String UNDETERMINED_LANGUAGE_TAG = "und";
  private static final String WILDCARD_RANGE = "*";

  /** RFC 5646 tags are short; caps what can reach the `language` keyword field in Elasticsearch. */
  private static final int MAX_LANGUAGE_TAG_LENGTH = 35;

  private final TrackingService trackingService;
  private final OperationContext systemOperationContext;
  private final AuthorizerChain authorizationChain;

  public TrackingController(
      @Qualifier("trackingService") TrackingService trackingService,
      @Qualifier("systemOperationContext") OperationContext systemOperationContext,
      @Qualifier("authorizerChain") AuthorizerChain authorizationChain) {
    this.trackingService = trackingService;
    this.systemOperationContext = systemOperationContext;
    this.authorizationChain = authorizationChain;
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

    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder()
                .buildOpenapi(
                    authentication.getActor().toUrnStr(), request, "trackEvent", List.of())
                .withUsageOperation(UsageOperation.OTHER_WRITE),
            authorizationChain,
            authentication,
            true);

    // Older frontends don't send `language`. Fall back to the browser's Accept-Language header so
    // the field is populated across version skew; a value posted by the client always wins.
    if (event.isObject() && !event.hasNonNull(LANGUAGE_FIELD)) {
      final String language = parsePreferredLanguage(request.getHeader(ACCEPT_LANGUAGE_HEADER));
      if (language != null) {
        ((ObjectNode) event).put(LANGUAGE_FIELD, language);
      }
    }

    try {
      log.debug("Forwarding tracking event to TrackingService");
      trackingService.track(event.get("type").asText(), opContext, null, null, event);
      log.debug("Successfully emitted analytics event");
      return ResponseEntity.ok().build();
    } catch (Exception e) {
      log.error("Failed to emit analytics event: {}", e.getMessage(), e);
      return ResponseEntity.internalServerError().build();
    }
  }

  /**
   * Resolve the client's most-preferred locale from an Accept-Language header, as a canonical BCP
   * 47 tag (e.g. {@code "de-DE,de;q=0.9,en;q=0.8"} -> {@code "de-DE"}).
   *
   * @return the preferred tag, or null when the header is absent, malformed, or names no usable
   *     locale.
   */
  @Nullable
  static String parsePreferredLanguage(@Nullable final String acceptLanguageHeader) {
    if (acceptLanguageHeader == null || acceptLanguageHeader.isBlank()) {
      return null;
    }
    try {
      // parse() orders ranges by descending q-weight, so the first usable one is the top
      // preference. It also lowercases ("de-DE" -> "de-de"), hence the round-trip through Locale to
      // restore canonical casing.
      for (final Locale.LanguageRange range : Locale.LanguageRange.parse(acceptLanguageHeader)) {
        if (WILDCARD_RANGE.equals(range.getRange())) {
          continue;
        }
        final String tag = Locale.forLanguageTag(range.getRange()).toLanguageTag();
        if (!UNDETERMINED_LANGUAGE_TAG.equals(tag) && tag.length() <= MAX_LANGUAGE_TAG_LENGTH) {
          return tag;
        }
      }
      return null;
    } catch (IllegalArgumentException e) {
      log.debug("Ignoring malformed Accept-Language header", e);
      return null;
    }
  }
}
