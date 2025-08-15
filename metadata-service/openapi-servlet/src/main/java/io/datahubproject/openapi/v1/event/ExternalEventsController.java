package io.datahubproject.openapi.v1.event;

import static com.linkedin.metadata.Constants.DATAHUB_USAGE_EVENT_INDEX;
import static com.linkedin.metadata.authorization.ApiOperation.READ;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizerChain;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.authorization.ApiGroup;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.datahubusage.DataHubUsageService;
import com.linkedin.metadata.datahubusage.ExternalAuditEventsSearchRequest;
import com.linkedin.metadata.datahubusage.ExternalAuditEventsSearchResponse;
import com.linkedin.mxe.Topics;
import io.datahubproject.event.ExternalEventsService;
import io.datahubproject.event.models.v1.ExternalEvent;
import io.datahubproject.event.models.v1.ExternalEvents;
import io.datahubproject.event.models.v1.ExternalEventsResponse;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.openapi.exception.UnauthorizedException;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
@RequestMapping("/openapi/v1/events")
@Tag(name = "Events", description = "An API for fetching external facing DataHub events.")
public class ExternalEventsController {

  static final int MAX_POLL_TIMEOUT_SECONDS = 60; // 1 minute
  static final int MAX_LIMIT = 5000; // Max of 5,000 messages per batch

  private ExternalEventsService eventsService;
  private AuthorizerChain authorizationChain;
  private OperationContext systemOperationContext;
  private final DataHubUsageService dataHubUsageService;

  @Autowired ObjectMapper objectMapper;

  public ExternalEventsController(
      @Qualifier("systemOperationContext") OperationContext systemOperationContext,
      AuthorizerChain authorizerChain,
      DataHubUsageService dataHubUsageService,
      ExternalEventsService eventsService) {
    this.systemOperationContext = systemOperationContext;
    this.authorizationChain = authorizerChain;
    this.dataHubUsageService = dataHubUsageService;
    this.eventsService = eventsService;
  }

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
          Integer lookbackWindowDays) {
    try {
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
      // If unauthorized
      return ResponseEntity.status(HttpStatus.FORBIDDEN).body(null);

    } catch (Exception ex) {
      // Log the exception
      log.error("An unexpected error occurred while polling events. Returning 500 to client", ex);

      // Return a generic error response
      String errorMessage = "An unexpected error occurred: " + ex.getMessage();
      ExternalEventsResponse response = new ExternalEventsResponse();
      response.setErrorMessage(errorMessage);
      return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
    }
  }

  private boolean isAuthorizedToGetEvents(
      @Nonnull final OperationContext opContext, @Nonnull final String topic) {
    if (Topics.PLATFORM_EVENT.equals(topic)) {
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

  @PostMapping(value = "/audit/search", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<ExternalAuditEventsSearchResponse> auditEventsSearch(
      HttpServletRequest request,
      @Parameter(
              name = "searchRequest",
              required = true,
              description = "Search Request for trace analytics usage events.")
          @RequestBody
          @Nonnull
          ExternalAuditEventsSearchRequest searchRequest,
      @RequestParam(value = "startTime", required = false) @Nullable Long startTime,
      @RequestParam(value = "endTime", required = false) @Nullable Long endTime,
      @RequestParam(value = "size", required = false) @Nullable Integer size,
      @RequestParam(value = "scrollId", required = false) String scrollId,
      @RequestParam(value = "includeRaw", defaultValue = "true") Boolean includeRaw) {
    Authentication authentication = AuthenticationContext.getAuthentication();
    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder()
                .buildOpenapi(
                    authentication.getActor().toUrnStr(),
                    request,
                    "search",
                    DATAHUB_USAGE_EVENT_INDEX),
            authorizationChain,
            authentication,
            true);

    if (!AuthUtil.isAPIAuthorized(opContext, ApiGroup.ANALYTICS, READ)) {
      throw new UnauthorizedException(
          authentication.getActor().toUrnStr()
              + " is unauthorized to "
              + READ
              + " "
              + ApiGroup.ANALYTICS
              + ".");
    }
    searchRequest.setEndTime(Optional.ofNullable(endTime).orElse(-1L));
    searchRequest.setStartTime(Optional.ofNullable(startTime).orElse(-1L));
    searchRequest.setSize(Optional.ofNullable(size).orElse(10));
    searchRequest.setScrollId(scrollId);
    searchRequest.setIncludeRaw(includeRaw);

    ExternalAuditEventsSearchResponse response =
        dataHubUsageService.externalAuditEventsSearch(opContext, searchRequest);

    return ResponseEntity.ok(response);
  }
}
