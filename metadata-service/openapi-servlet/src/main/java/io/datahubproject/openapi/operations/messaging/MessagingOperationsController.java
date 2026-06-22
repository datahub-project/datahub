package io.datahubproject.openapi.operations.messaging;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizerChain;
import com.linkedin.gms.factory.kafka.common.TopicConventionFactory;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.config.messaging.KafkaOrPgQueueMessagingTransportCondition;
import com.linkedin.metadata.messaging.ConsumerLagPort;
import com.linkedin.metadata.queue.ConsumerRegistrationRow;
import com.linkedin.metadata.queue.MetadataQueueStore;
import com.linkedin.metadata.queue.QueueTopicMetadata;
import com.linkedin.mxe.TopicConvention;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Conditional;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/openapi/operations/messaging")
@Slf4j
@Conditional(KafkaOrPgQueueMessagingTransportCondition.class)
@Tag(
    name = "Messaging operations",
    description =
        "Transport-neutral consumer lag and topic metadata. Prefer these endpoints over "
            + "/openapi/operations/kafka (deprecated).")
public class MessagingOperationsController {

  private final OperationContext systemOperationContext;
  private final AuthorizerChain authorizerChain;
  private final ConsumerLagPort consumerLagPort;
  private final TopicConvention topicConvention;

  @Autowired(required = false)
  private MetadataQueueStore metadataQueueStore;

  public MessagingOperationsController(
      @Qualifier("systemOperationContext") OperationContext systemOperationContext,
      AuthorizerChain authorizerChain,
      ConsumerLagPort consumerLagPort,
      @Qualifier(TopicConventionFactory.TOPIC_CONVENTION_BEAN) TopicConvention topicConvention) {
    this.systemOperationContext = systemOperationContext;
    this.authorizerChain = authorizerChain;
    this.consumerLagPort = consumerLagPort;
    this.topicConvention = topicConvention;
  }

  @GetMapping(path = "/transport", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Get active messaging transport and primary metadata queue topic names")
  public ResponseEntity<?> getTransport(HttpServletRequest httpServletRequest) {
    if (!authorize(httpServletRequest, "getMessagingTransport")) {
      return forbidden();
    }
    MessagingTransportInfoResponse body =
        new MessagingTransportInfoResponse(
            consumerLagPort.transport(),
            topicConvention.getMetadataChangeProposalTopicName(),
            topicConvention.getMetadataChangeLogVersionedTopicName(),
            topicConvention.getMetadataChangeLogTimeseriesTopicName());
    return ResponseEntity.ok(body);
  }

  @GetMapping(path = "/mcp/consumer/lag", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "MCP consumer lag (Kafka or pgQueue)")
  @Parameter(name = "skipCache")
  @Parameter(name = "detailed")
  public ResponseEntity<?> mcpLag(
      HttpServletRequest httpServletRequest,
      @RequestParam(value = "skipCache", defaultValue = "false") boolean skipCache,
      @RequestParam(value = "detailed", defaultValue = "false") boolean detailed) {
    if (!authorize(httpServletRequest, "getMcpConsumerLag")) {
      return forbidden();
    }
    return ResponseEntity.ok(
        MessagingOpenApiMapper.toEnvelope(
            consumerLagPort.transport(), consumerLagPort.mcpLag(skipCache, detailed)));
  }

  @GetMapping(path = "/mcl/consumer/lag", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "MCL versioned consumer lag (Kafka or pgQueue)")
  public ResponseEntity<?> mclVersionedLag(
      HttpServletRequest httpServletRequest,
      @RequestParam(value = "skipCache", defaultValue = "false") boolean skipCache,
      @RequestParam(value = "detailed", defaultValue = "false") boolean detailed) {
    if (!authorize(httpServletRequest, "getMclVersionedConsumerLag")) {
      return forbidden();
    }
    return ResponseEntity.ok(
        MessagingOpenApiMapper.toEnvelope(
            consumerLagPort.transport(), consumerLagPort.mclVersionedLag(skipCache, detailed)));
  }

  @GetMapping(path = "/usage-events/consumer/lag", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "DataHub usage-event consumer lag (Kafka or pgQueue)")
  public ResponseEntity<?> usageEventsLag(
      HttpServletRequest httpServletRequest,
      @RequestParam(value = "skipCache", defaultValue = "false") boolean skipCache,
      @RequestParam(value = "detailed", defaultValue = "false") boolean detailed) {
    if (!authorize(httpServletRequest, "getUsageEventsConsumerLag")) {
      return forbidden();
    }
    return ResponseEntity.ok(
        MessagingOpenApiMapper.toEnvelope(
            consumerLagPort.transport(), consumerLagPort.usageEventsLag(skipCache, detailed)));
  }

  @GetMapping(path = "/mcl-timeseries/consumer/lag", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "MCL timeseries consumer lag (Kafka or pgQueue)")
  public ResponseEntity<?> mclTimeseriesLag(
      HttpServletRequest httpServletRequest,
      @RequestParam(value = "skipCache", defaultValue = "false") boolean skipCache,
      @RequestParam(value = "detailed", defaultValue = "false") boolean detailed) {
    if (!authorize(httpServletRequest, "getMclTimeseriesConsumerLag")) {
      return forbidden();
    }
    return ResponseEntity.ok(
        MessagingOpenApiMapper.toEnvelope(
            consumerLagPort.transport(), consumerLagPort.mclTimeseriesLag(skipCache, detailed)));
  }

  @GetMapping(path = "/consumers", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "List registered consumers for a topic (aggressive retention)")
  public ResponseEntity<?> listConsumers(
      HttpServletRequest httpServletRequest, @RequestParam("topicName") String topicName) {
    if (!authorize(httpServletRequest, "listConsumers")) {
      return forbidden();
    }
    if (metadataQueueStore == null) {
      return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
          .body(Map.of("error", "pgQueue store is not available"));
    }
    Optional<QueueTopicMetadata> meta = metadataQueueStore.fetchTopic(topicName);
    if (meta.isEmpty()) {
      return ResponseEntity.status(HttpStatus.NOT_FOUND)
          .body(Map.of("error", "Topic not found: " + topicName));
    }
    List<ConsumerRegistrationRow> rows =
        metadataQueueStore.listRegisteredConsumers(meta.get().id());
    List<ConsumerRegistrationResponse> body =
        rows.stream()
            .map(
                r ->
                    ConsumerRegistrationResponse.builder()
                        .consumerGroup(r.consumerGroup())
                        .topicName(topicName)
                        .registeredAt(r.registeredAt().toEpochMilli())
                        .lastHeartbeatAt(r.lastHeartbeatAt().toEpochMilli())
                        .build())
            .toList();
    return ResponseEntity.ok(body);
  }

  @PutMapping(
      path = "/consumers",
      consumes = MediaType.APPLICATION_JSON_VALUE,
      produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Register a consumer group for a topic (aggressive retention)")
  public ResponseEntity<?> registerConsumer(
      HttpServletRequest httpServletRequest, @RequestBody ConsumerRegistrationRequest request) {
    if (!authorize(httpServletRequest, "registerConsumer")) {
      return forbidden();
    }
    if (metadataQueueStore == null) {
      return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
          .body(Map.of("error", "pgQueue store is not available"));
    }
    Optional<QueueTopicMetadata> meta = metadataQueueStore.fetchTopic(request.getTopicName());
    if (meta.isEmpty()) {
      return ResponseEntity.status(HttpStatus.NOT_FOUND)
          .body(Map.of("error", "Topic not found: " + request.getTopicName()));
    }
    metadataQueueStore.registerConsumer(request.getConsumerGroup(), meta.get().id());
    return ResponseEntity.ok(Map.of("registered", true));
  }

  @DeleteMapping(path = "/consumers", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Unregister a consumer group from a topic (aggressive retention)")
  public ResponseEntity<?> unregisterConsumer(
      HttpServletRequest httpServletRequest,
      @RequestParam("consumerGroup") String consumerGroup,
      @RequestParam("topicName") String topicName) {
    if (!authorize(httpServletRequest, "unregisterConsumer")) {
      return forbidden();
    }
    if (metadataQueueStore == null) {
      return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
          .body(Map.of("error", "pgQueue store is not available"));
    }
    Optional<QueueTopicMetadata> meta = metadataQueueStore.fetchTopic(topicName);
    if (meta.isEmpty()) {
      return ResponseEntity.status(HttpStatus.NOT_FOUND)
          .body(Map.of("error", "Topic not found: " + topicName));
    }
    boolean deleted = metadataQueueStore.unregisterConsumer(consumerGroup, meta.get().id());
    return ResponseEntity.ok(Map.of("deleted", deleted));
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
