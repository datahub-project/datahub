package io.datahubproject.openapi.operations.messaging;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizerChain;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.config.messaging.PgQueueMessagingTransportCondition;
import com.linkedin.metadata.queue.ConsumerOffsetResetDetail;
import com.linkedin.metadata.queue.ConsumerOffsetResetReport;
import com.linkedin.metadata.queue.ConsumerOffsetResetSpec;
import com.linkedin.metadata.queue.MetadataQueueStore;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Conditional;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * pgQueue-only messaging operations (not registered when {@code datahub.messaging.transport} is
 * kafka).
 */
@RestController
@RequestMapping("/openapi/operations/messaging")
@Slf4j
@Conditional(PgQueueMessagingTransportCondition.class)
@Tag(
    name = "Messaging operations (pgQueue)",
    description = "pgQueue-specific messaging operations endpoints")
public class PgQueueMessagingOperationsController {

  private final OperationContext systemOperationContext;
  private final AuthorizerChain authorizerChain;
  private final MetadataQueueStore metadataQueueStore;

  public PgQueueMessagingOperationsController(
      OperationContext systemOperationContext,
      AuthorizerChain authorizerChain,
      MetadataQueueStore metadataQueueStore) {
    this.systemOperationContext = systemOperationContext;
    this.authorizerChain = authorizerChain;
    this.metadataQueueStore = metadataQueueStore;
  }

  @PostMapping(
      path = "/consumer/offsets/reset",
      consumes = MediaType.APPLICATION_JSON_VALUE,
      produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      summary = "Reset pgQueue consumer offsets (STUCK_AHEAD recovery)",
      description =
          "Sets committed offsets to the current message log end per partition. "
              + "By default only updates partitions where the offset is ahead of the log "
              + "(STUCK_AHEAD). Requires manage_system_operations.")
  public ResponseEntity<?> resetConsumerOffsets(
      HttpServletRequest httpServletRequest,
      @RequestBody(required = false) ConsumerOffsetResetRequest request) {
    if (!authorize(httpServletRequest, "resetConsumerOffsets")) {
      return forbidden();
    }
    ConsumerOffsetResetRequest body = request != null ? request : new ConsumerOffsetResetRequest();
    try {
      ConsumerOffsetResetReport report =
          metadataQueueStore.resetConsumerOffsets(
              ConsumerOffsetResetSpec.builder()
                  .consumerGroup(body.getConsumerGroup())
                  .topicName(body.getTopicName())
                  .partitionId(body.getPartitionId())
                  .onlyStuckAhead(body.isOnlyStuckAhead())
                  .build());
      return ResponseEntity.ok(toResetResponse(report));
    } catch (IllegalArgumentException e) {
      return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", e.getMessage()));
    }
  }

  private ConsumerOffsetResetResponse toResetResponse(ConsumerOffsetResetReport report) {
    List<ConsumerOffsetResetDetailResponse> details =
        report.getResets().stream().map(this::toResetDetail).toList();
    return ConsumerOffsetResetResponse.builder()
        .transport("pgqueue")
        .partitionsUpdated(report.getPartitionsUpdated())
        .resets(details)
        .build();
  }

  private ConsumerOffsetResetDetailResponse toResetDetail(ConsumerOffsetResetDetail detail) {
    return ConsumerOffsetResetDetailResponse.builder()
        .consumerGroup(detail.getConsumerGroup())
        .topicName(detail.getTopicName())
        .partitionId(detail.getPartitionId())
        .previousOffset(detail.getPreviousOffset())
        .newOffset(detail.getNewOffset())
        .maxSeq(detail.getMaxSeq())
        .build();
  }

  private boolean authorize(HttpServletRequest request, String operation) {
    Authentication authentication = AuthenticationContext.getAuthentication();
    String actorUrnStr = authentication.getActor().toUrnStr();
    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder().buildOpenapi(actorUrnStr, request, operation, List.of()),
            authorizerChain,
            authentication,
            true);
    return AuthUtil.isAPIAuthorized(opContext, PoliciesConfig.MANAGE_SYSTEM_OPERATIONS_PRIVILEGE);
  }

  private ResponseEntity<?> forbidden() {
    Authentication authentication = AuthenticationContext.getAuthentication();
    String actorUrnStr = authentication.getActor().toUrnStr();
    return ResponseEntity.status(HttpStatus.FORBIDDEN)
        .body(Map.of("error", actorUrnStr + " is not authorized for messaging operations"));
  }
}
