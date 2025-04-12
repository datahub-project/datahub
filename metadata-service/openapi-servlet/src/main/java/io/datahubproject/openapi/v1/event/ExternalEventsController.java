package io.datahubproject.openapi.v1.event;

import static io.datahubproject.event.ExternalEventsService.PLATFORM_EVENT_TOPIC_NAME;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizerChain;
import com.linkedin.metadata.authorization.PoliciesConfig;
import io.datahubproject.event.ExternalEventsService;
import io.datahubproject.event.models.v1.ExternalEvent;
import io.datahubproject.event.models.v1.ExternalEvents;
import io.datahubproject.event.models.v1.ExternalEventsResponse;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@ConditionalOnProperty(name = "eventsApi.enabled", havingValue = "true")
@RestController
@RequestMapping("/openapi/v1/events")
@Tag(name = "Events", description = "An API for fetching events for a topic.")
public class ExternalEventsController {

  private static final int MAX_POLL_TIMEOUT_SECONDS = 60; // 1 minute
  private static final int MAX_LIMIT = 5000; // Max of 5,000 messages per batch

  @Autowired private ExternalEventsService eventsService;
  @Autowired private AuthorizerChain authorizationChain;

  @Qualifier("systemOperationContext")
  @Autowired
  private OperationContext systemOperationContext;

  @GetMapping(path = "/poll", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      summary = "Scroll events for a specific topic",
      description = "Fetches events for the provided topic")
  public ResponseEntity<ExternalEventsResponse> poll(
      HttpServletRequest request,
      @Parameter(
              name = "topic",
              required = true,
              description =
                  "The topic to read events for. Currently only supports PlatformEvent_v1, which provides Platform Events such as EntityChangeEvent and NotificationRequestEvent.")
          @RequestParam(name = "topic", required = true)
          String topic,
      @Parameter(name = "offsetId", description = "The offset to start reading the topic from")
          @RequestParam(name = "offsetId", required = false)
          String offsetId,
      @Parameter(
              name = "limit",
              description = "The max number of events to read. Defaults to 100 events.")
          @RequestParam(name = "limit", required = false)
          Integer limit,
      @Parameter(
              name = "pollTimeoutSeconds",
              description = "The maximum time to wait for new events. Defaults to 10 seconds.")
          @RequestParam(name = "pollTimeoutSeconds", required = false)
          Integer pollTimeoutSeconds,
      @Parameter(
              name = "lookbackWindowDays",
              description =
                  "The window to lookback in days if there is no offset provided. Defaults to null.")
          @RequestParam(name = "lookbackWindowDays", required = false)
          Integer lookbackWindowDays)
      throws Exception {

    final Authentication authentication = AuthenticationContext.getAuthentication();

    final OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder()
                .buildOpenapi(authentication.getActor().toUrnStr(), request, "poll", List.of()),
            authorizationChain,
            authentication,
            true);

    if (isAuthorizedToGetEvents(opContext, topic)) {
      return toResponse(
          eventsService.poll(
              topic,
              offsetId,
              getLimit(limit),
              getPollTimeout(pollTimeoutSeconds),
              lookbackWindowDays));
    }
    // Else, we're unauthorized.
    return ResponseEntity.status(HttpStatus.FORBIDDEN).body(null);
  }

  private boolean isAuthorizedToGetEvents(
      @Nonnull final OperationContext opContext, @Nonnull final String topic) {
    if (PLATFORM_EVENT_TOPIC_NAME.equals(topic)) {
      return AuthUtil.isAPIAuthorized(opContext, PoliciesConfig.GET_PLATFORM_EVENTS_PRIVILEGE);
    }
    return false;
  }

  @Nullable
  private Integer getLimit(Integer maybeLimit) {
    if (maybeLimit == null) {
      return null;
    }
    if (maybeLimit > MAX_LIMIT) {
      return MAX_LIMIT;
    }
    return maybeLimit;
  }

  @Nullable
  private Integer getPollTimeout(Integer maybeTimeout) {
    if (maybeTimeout == null) {
      return null;
    }
    if (maybeTimeout > MAX_POLL_TIMEOUT_SECONDS) {
      return MAX_POLL_TIMEOUT_SECONDS;
    }
    return maybeTimeout;
  }

  private ResponseEntity<ExternalEventsResponse> toResponse(@Nonnull final ExternalEvents events) {
    final ExternalEventsResponse response = new ExternalEventsResponse();
    response.setCount(events.getCount());
    response.setOffsetId(events.getOffsetId());
    response.setEvents(toEvents(events.getEvents()));
    return ResponseEntity.ok(response);
  }

  private List<ExternalEvent> toEvents(@Nonnull final List<ExternalEvent> events) {
    final List<ExternalEvent> response = new ArrayList<>();
    for (ExternalEvent event : events) {
      ExternalEvent result = new ExternalEvent();
      result.setValue(event.getValue());
      result.setContentType(event.getContentType());
      response.add(result);
    }
    return response;
  }
}
