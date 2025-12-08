package io.datahubproject.openapi.operations.kafka;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizerChain;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.trace.MCLTraceReader;
import com.linkedin.metadata.trace.MCPTraceReader;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.openapi.operations.kafka.models.KafkaClusterResponse;
import io.datahubproject.openapi.operations.kafka.models.KafkaConsumerGroupResponse;
import io.datahubproject.openapi.operations.kafka.models.KafkaLogDirResponse;
import io.datahubproject.openapi.operations.kafka.models.KafkaMessageResponse;
import io.datahubproject.openapi.operations.kafka.models.KafkaOffsetResponse;
import io.datahubproject.openapi.operations.kafka.models.KafkaRequests;
import io.datahubproject.openapi.operations.kafka.models.KafkaTopicResponse;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST API controller for Kafka admin operations.
 *
 * <p><strong>WARNING:</strong> These endpoints are for advanced system operators only. Improper use
 * can break DataHub deployments. All endpoints require MANAGE_SYSTEM_OPERATIONS_PRIVILEGE.
 *
 * <p>Read-only metadata operations (list topics, describe topics, consumer groups, cluster info)
 * are available for any topic in the cluster. Write operations (create/delete topics, alter
 * configs, message retrieval/replay, reset offsets) are restricted to DataHub topics only.
 */
@RestController
@RequestMapping("/openapi/operations/kafka")
@Slf4j
public class KafkaController {

  private final OperationContext systemOperationContext;
  private final AuthorizerChain authorizerChain;
  private final MCPTraceReader mcpTraceReader;
  private final MCLTraceReader mclTraceReader;
  private final MCLTraceReader mclTimeseriesTraceReader;
  private final KafkaAdminService kafkaAdminService;

  public KafkaController(
      @Qualifier("systemOperationContext") OperationContext systemOperationContext,
      AuthorizerChain authorizerChain,
      MCPTraceReader mcpTraceReader,
      @Qualifier("mclVersionedTraceReader") MCLTraceReader mclTraceReader,
      @Qualifier("mclTimeseriesTraceReader") MCLTraceReader mclTimeseriesTraceReader,
      @Qualifier("kafkaAdminService") KafkaAdminService kafkaAdminService) {
    this.systemOperationContext = systemOperationContext;
    this.authorizerChain = authorizerChain;
    this.mcpTraceReader = mcpTraceReader;
    this.mclTraceReader = mclTraceReader;
    this.mclTimeseriesTraceReader = mclTimeseriesTraceReader;
    this.kafkaAdminService = kafkaAdminService;
  }

  /**
   * Checks authorization for Kafka admin operations.
   *
   * @param httpServletRequest the HTTP request
   * @param operationName the operation name for logging
   * @return OperationContext (always non-null)
   */
  private OperationContext checkAuthorization(
      HttpServletRequest httpServletRequest, String operationName) {
    Authentication authentication = AuthenticationContext.getAuthentication();
    String actorUrnStr = authentication.getActor().toUrnStr();

    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder()
                .buildOpenapi(actorUrnStr, httpServletRequest, operationName, List.of()),
            authorizerChain,
            authentication,
            true);

    if (!AuthUtil.isAPIAuthorized(opContext, PoliciesConfig.MANAGE_SYSTEM_OPERATIONS_PRIVILEGE)) {
      throw new UnauthorizedOperationException(actorUrnStr, operationName);
    }

    return opContext;
  }

  /** Exception thrown when a user is not authorized to perform a Kafka admin operation. */
  public static class UnauthorizedOperationException extends RuntimeException {
    private final String actorUrn;
    private final String operation;

    public UnauthorizedOperationException(String actorUrn, String operation) {
      super(actorUrn + " is not authorized to perform " + operation);
      this.actorUrn = actorUrn;
      this.operation = operation;
    }

    public String getActorUrn() {
      return actorUrn;
    }

    public String getOperation() {
      return operation;
    }
  }

  /**
   * Handles UnauthorizedOperationException and returns a 403 response.
   *
   * @param ex the exception
   * @return ResponseEntity with 403 status
   */
  @org.springframework.web.bind.annotation.ExceptionHandler(UnauthorizedOperationException.class)
  public ResponseEntity<?> handleUnauthorizedException(UnauthorizedOperationException ex) {
    return ResponseEntity.status(HttpStatus.FORBIDDEN)
        .body(ErrorResponse.builder().error(ex.getMessage()).build());
  }

  /**
   * Creates an error response for other failures.
   *
   * @param message the error message
   * @return ResponseEntity with 500 status
   */
  private ResponseEntity<?> errorResponse(String message) {
    return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
        .body(ErrorResponse.builder().error(message).build());
  }

  // ============================================================================
  // Existing Offset Endpoints
  // ============================================================================

  @Tag(
      name = "Kafka Admin",
      description =
          "Kafka admin operations. WARNING: Advanced operations - improper use can break deployments.")
  @GetMapping(path = "/mcp/consumer/offsets", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      summary = "Get MetadataChangeProposal consumer kafka offsets with lag metrics",
      description =
          "Retrieves the current offsets and lag information for all partitions of the MCP topic from the consumer group",
      responses = {
        @ApiResponse(
            responseCode = "200",
            description = "Successfully retrieved consumer offsets and lag metrics",
            content =
                @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = KafkaOffsetResponse.class))),
        @ApiResponse(
            responseCode = "403",
            description = "Caller is not authorized to access this endpoint",
            content =
                @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = ErrorResponse.class)))
      })
  public ResponseEntity<?> getMCPOffsets(
      HttpServletRequest httpServletRequest,
      @Parameter(
              description =
                  "Whether to bypass the offset cache and fetch fresh values directly from Kafka")
          @RequestParam(value = "skipCache", defaultValue = "false")
          boolean skipCache,
      @Parameter(description = "Whether to include per-partition offset details in the response")
          @RequestParam(value = "detailed", defaultValue = "false")
          boolean detailed) {
    checkAuthorization(httpServletRequest, "getMCPOffsets");

    // Get consumer offsets
    Map<TopicPartition, OffsetAndMetadata> offsetMap =
        mcpTraceReader.getAllPartitionOffsets(skipCache);

    // Get end offsets for the same partitions to calculate lag
    Map<TopicPartition, Long> endOffsets =
        mcpTraceReader.getEndOffsets(offsetMap.keySet(), skipCache);

    KafkaOffsetResponse response =
        convertToResponse(mcpTraceReader.getConsumerGroupId(), offsetMap, endOffsets, detailed);

    return ResponseEntity.ok(response);
  }

  @Tag(
      name = "Kafka Admin",
      description =
          "Kafka admin operations. WARNING: Advanced operations - improper use can break deployments.")
  @GetMapping(path = "/mcl/consumer/offsets", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      summary = "Get MetadataChangeLog consumer kafka offsets with lag metrics",
      description =
          "Retrieves the current offsets and lag information for all partitions of the MCL topic from the consumer group",
      responses = {
        @ApiResponse(
            responseCode = "200",
            description = "Successfully retrieved consumer offsets and lag metrics",
            content =
                @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = KafkaOffsetResponse.class))),
        @ApiResponse(
            responseCode = "403",
            description = "Caller is not authorized to access this endpoint",
            content =
                @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = ErrorResponse.class)))
      })
  public ResponseEntity<?> getMCLOffsets(
      HttpServletRequest httpServletRequest,
      @Parameter(
              description =
                  "Whether to bypass the offset cache and fetch fresh values directly from Kafka")
          @RequestParam(value = "skipCache", defaultValue = "false")
          boolean skipCache,
      @Parameter(description = "Whether to include per-partition offset details in the response")
          @RequestParam(value = "detailed", defaultValue = "false")
          boolean detailed) {
    checkAuthorization(httpServletRequest, "getMCLOffsets");

    // Get consumer offsets
    Map<TopicPartition, OffsetAndMetadata> offsetMap =
        mclTraceReader.getAllPartitionOffsets(skipCache);

    // Get end offsets for the same partitions to calculate lag
    Map<TopicPartition, Long> endOffsets =
        mclTraceReader.getEndOffsets(offsetMap.keySet(), skipCache);
    KafkaOffsetResponse response =
        convertToResponse(mclTraceReader.getConsumerGroupId(), offsetMap, endOffsets, detailed);

    return ResponseEntity.ok(response);
  }

  @Tag(
      name = "Kafka Admin",
      description =
          "Kafka admin operations. WARNING: Advanced operations - improper use can break deployments.")
  @GetMapping(
      path = "/mcl-timeseries/consumer/offsets",
      produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      summary = "Get MetadataChangeLog timeseries consumer kafka offsets with lag metrics",
      description =
          "Retrieves the current offsets and lag information for all partitions of the MCL timeseries topic from the consumer group",
      responses = {
        @ApiResponse(
            responseCode = "200",
            description = "Successfully retrieved consumer offsets and lag metrics",
            content =
                @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = KafkaOffsetResponse.class))),
        @ApiResponse(
            responseCode = "403",
            description = "Caller is not authorized to access this endpoint",
            content =
                @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = ErrorResponse.class)))
      })
  public ResponseEntity<?> getMCLTimeseriesOffsets(
      HttpServletRequest httpServletRequest,
      @Parameter(
              description =
                  "Whether to bypass the offset cache and fetch fresh values directly from Kafka")
          @RequestParam(value = "skipCache", defaultValue = "false")
          boolean skipCache,
      @Parameter(description = "Whether to include per-partition offset details in the response")
          @RequestParam(value = "detailed", defaultValue = "false")
          boolean detailed) {
    checkAuthorization(httpServletRequest, "getMCLTimeseriesOffsets");

    // Get consumer offsets
    Map<TopicPartition, OffsetAndMetadata> offsetMap =
        mclTimeseriesTraceReader.getAllPartitionOffsets(skipCache);

    // Get end offsets for the same partitions to calculate lag
    Map<TopicPartition, Long> endOffsets =
        mclTimeseriesTraceReader.getEndOffsets(offsetMap.keySet(), skipCache);

    KafkaOffsetResponse response =
        convertToResponse(
            mclTimeseriesTraceReader.getConsumerGroupId(), offsetMap, endOffsets, detailed);

    return ResponseEntity.ok(response);
  }

  // ============================================================================
  // Cluster Info Endpoint
  // ============================================================================

  @Tag(
      name = "Kafka Admin",
      description =
          "Kafka admin operations. WARNING: Advanced operations - improper use can break deployments.")
  @GetMapping(path = "/cluster", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      summary = "Get Kafka cluster metadata",
      description = "Retrieves cluster information including brokers, controller, and cluster ID",
      responses = {
        @ApiResponse(
            responseCode = "200",
            description = "Successfully retrieved cluster information",
            content =
                @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = KafkaClusterResponse.class))),
        @ApiResponse(
            responseCode = "403",
            description = "Caller is not authorized to access this endpoint",
            content =
                @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = ErrorResponse.class)))
      })
  public ResponseEntity<?> getClusterInfo(HttpServletRequest httpServletRequest) {
    checkAuthorization(httpServletRequest, "getClusterInfo");

    try {
      KafkaClusterResponse clusterInfo = kafkaAdminService.getClusterInfo();
      return ResponseEntity.ok(clusterInfo);
    } catch (KafkaAdminException e) {
      log.error("Failed to get cluster information", e);
      return errorResponse(e.getMessage());
    }
  }

  // ============================================================================
  // Topic Management Endpoints
  // ============================================================================

  @Tag(
      name = "Kafka Admin",
      description =
          "Kafka admin operations. WARNING: Advanced operations - improper use can break deployments.")
  @GetMapping(path = "/topics", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      summary = "List all Kafka topics",
      description = "Retrieves a list of all topics in the Kafka cluster",
      responses = {
        @ApiResponse(
            responseCode = "200",
            description = "Successfully retrieved topic list",
            content =
                @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = KafkaTopicResponse.TopicListResponse.class))),
        @ApiResponse(
            responseCode = "403",
            description = "Caller is not authorized to access this endpoint",
            content =
                @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = ErrorResponse.class)))
      })
  public ResponseEntity<?> listTopics(
      HttpServletRequest httpServletRequest,
      @Parameter(description = "Whether to include internal Kafka topics")
          @RequestParam(value = "includeInternal", defaultValue = "false")
          boolean includeInternal) {
    checkAuthorization(httpServletRequest, "listTopics");

    try {
      List<String> topics = kafkaAdminService.listTopics(includeInternal);
      KafkaTopicResponse.TopicListResponse response =
          KafkaTopicResponse.TopicListResponse.builder()
              .topics(topics)
              .count(topics.size())
              .includeInternal(includeInternal)
              .build();
      return ResponseEntity.ok(response);
    } catch (KafkaAdminException e) {
      log.error("Failed to list topics", e);
      return errorResponse(e.getMessage());
    }
  }

  @Tag(
      name = "Kafka Admin",
      description =
          "Kafka admin operations. WARNING: Advanced operations - improper use can break deployments.")
  @GetMapping(path = "/topics/{topicName}", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      summary = "Describe a Kafka topic",
      description =
          "Retrieves detailed information about a specific topic including partitions, configs, and ISR",
      responses = {
        @ApiResponse(
            responseCode = "200",
            description = "Successfully retrieved topic information",
            content =
                @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = KafkaTopicResponse.class))),
        @ApiResponse(
            responseCode = "403",
            description = "Caller is not authorized to access this endpoint",
            content =
                @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = ErrorResponse.class))),
        @ApiResponse(
            responseCode = "404",
            description = "Topic not found",
            content =
                @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = ErrorResponse.class)))
      })
  public ResponseEntity<?> describeTopic(
      HttpServletRequest httpServletRequest,
      @Parameter(
              in = ParameterIn.PATH,
              description = "Topic name",
              required = true,
              example = "MetadataChangeProposal_v1")
          @PathVariable("topicName")
          String topicName) {
    checkAuthorization(httpServletRequest, "describeTopic");

    try {
      KafkaTopicResponse topicInfo = kafkaAdminService.describeTopic(topicName);
      return ResponseEntity.ok(topicInfo);
    } catch (KafkaAdminException.TopicNotFoundException e) {
      return ResponseEntity.status(HttpStatus.NOT_FOUND)
          .body(ErrorResponse.builder().error(e.getMessage()).build());
    } catch (KafkaAdminException e) {
      log.error("Failed to describe topic: " + topicName, e);
      return errorResponse(e.getMessage());
    }
  }

  @Tag(
      name = "Kafka Admin",
      description =
          "Kafka admin operations. WARNING: Advanced operations - improper use can break deployments.")
  @PostMapping(path = "/topics/{topic}", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      summary = "Create a DataHub Kafka topic",
      description =
          "Creates a new Kafka topic. Restricted to DataHub topics only (mcp, mcp-failed, mcl-versioned, mcl-timeseries).",
      responses = {
        @ApiResponse(
            responseCode = "200",
            description = "Successfully created topic",
            content =
                @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = KafkaTopicResponse.class))),
        @ApiResponse(
            responseCode = "400",
            description = "Invalid request or topic already exists",
            content =
                @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = ErrorResponse.class))),
        @ApiResponse(
            responseCode = "403",
            description = "Caller is not authorized or topic is not a DataHub topic",
            content =
                @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = ErrorResponse.class)))
      })
  public ResponseEntity<?> createTopic(
      HttpServletRequest httpServletRequest,
      @Parameter(
              in = ParameterIn.PATH,
              description =
                  "Topic alias (mcp, mcl-versioned, mcl-timeseries, platform-event, etc.)",
              required = true,
              example = "mcp")
          @PathVariable("topic")
          String topic,
      @RequestBody(required = false) KafkaRequests.CreateTopicRequest request) {
    checkAuthorization(httpServletRequest, "createTopic");

    try {
      Integer numPartitions = request != null ? request.getNumPartitions() : null;
      Short replicationFactor = request != null ? request.getReplicationFactor() : null;
      Map<String, String> configs = request != null ? request.getConfigs() : null;

      KafkaTopicResponse topicInfo =
          kafkaAdminService.createDataHubTopic(topic, numPartitions, replicationFactor, configs);
      return ResponseEntity.ok(topicInfo);
    } catch (KafkaAdminException.InvalidAliasException e) {
      return ResponseEntity.status(HttpStatus.BAD_REQUEST)
          .body(ErrorResponse.builder().error(e.getMessage()).build());
    } catch (KafkaAdminException.TopicAlreadyExistsException e) {
      return ResponseEntity.status(HttpStatus.BAD_REQUEST)
          .body(ErrorResponse.builder().error(e.getMessage()).build());
    } catch (KafkaAdminException e) {
      log.error("Failed to create topic: " + topic, e);
      return errorResponse(e.getMessage());
    }
  }

  @Tag(
      name = "Kafka Admin",
      description =
          "Kafka admin operations. WARNING: Advanced operations - improper use can break deployments.")
  @DeleteMapping(path = "/topics/{topic}", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      summary = "Delete a DataHub Kafka topic",
      description =
          "Deletes a Kafka topic. Restricted to DataHub topics only (mcp, mcp-failed, mcl-versioned, mcl-timeseries).",
      responses = {
        @ApiResponse(
            responseCode = "200",
            description = "Successfully deleted topic",
            content =
                @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = ErrorResponse.class))),
        @ApiResponse(
            responseCode = "400",
            description = "Invalid request or topic not found",
            content =
                @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = ErrorResponse.class))),
        @ApiResponse(
            responseCode = "403",
            description = "Caller is not authorized or topic is not a DataHub topic",
            content =
                @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = ErrorResponse.class)))
      })
  public ResponseEntity<?> deleteTopic(
      HttpServletRequest httpServletRequest,
      @Parameter(
              in = ParameterIn.PATH,
              description =
                  "Topic alias (mcp, mcl-versioned, mcl-timeseries, platform-event, etc.)",
              required = true,
              example = "mcp")
          @PathVariable("topic")
          String topic) {
    checkAuthorization(httpServletRequest, "deleteTopic");

    try {
      kafkaAdminService.deleteDataHubTopic(topic);
      return ResponseEntity.ok(
          ErrorResponse.builder().error("Successfully deleted topic: " + topic).build());
    } catch (KafkaAdminException.InvalidAliasException e) {
      return ResponseEntity.status(HttpStatus.BAD_REQUEST)
          .body(ErrorResponse.builder().error(e.getMessage()).build());
    } catch (KafkaAdminException.TopicNotFoundException e) {
      return ResponseEntity.status(HttpStatus.BAD_REQUEST)
          .body(ErrorResponse.builder().error(e.getMessage()).build());
    } catch (KafkaAdminException e) {
      log.error("Failed to delete topic: " + topic, e);
      return errorResponse(e.getMessage());
    }
  }

  @Tag(
      name = "Kafka Admin",
      description =
          "Kafka admin operations. WARNING: Advanced operations - improper use can break deployments.")
  @PutMapping(path = "/topics/{topic}/configs", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      summary = "Alter topic configuration",
      description =
          "Updates topic configuration. Restricted to DataHub topics only (mcp, mcp-failed, mcl-versioned, mcl-timeseries).",
      responses = {
        @ApiResponse(
            responseCode = "200",
            description = "Successfully updated topic configuration",
            content =
                @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = KafkaTopicResponse.class))),
        @ApiResponse(
            responseCode = "400",
            description = "Invalid request",
            content =
                @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = ErrorResponse.class))),
        @ApiResponse(
            responseCode = "403",
            description = "Caller is not authorized or topic is not a DataHub topic",
            content =
                @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = ErrorResponse.class)))
      })
  public ResponseEntity<?> alterTopicConfig(
      HttpServletRequest httpServletRequest,
      @Parameter(
              in = ParameterIn.PATH,
              description =
                  "Topic alias (mcp, mcl-versioned, mcl-timeseries, platform-event, etc.)",
              required = true,
              example = "mcp")
          @PathVariable("topic")
          String topic,
      @RequestBody KafkaRequests.AlterConfigRequest request) {
    checkAuthorization(httpServletRequest, "alterTopicConfig");

    if (request == null || request.getConfigs() == null || request.getConfigs().isEmpty()) {
      return ResponseEntity.status(HttpStatus.BAD_REQUEST)
          .body(ErrorResponse.builder().error("No configuration changes provided").build());
    }

    try {
      KafkaTopicResponse topicInfo =
          kafkaAdminService.alterDataHubTopicConfig(topic, request.getConfigs());
      return ResponseEntity.ok(topicInfo);
    } catch (KafkaAdminException.InvalidAliasException e) {
      return ResponseEntity.status(HttpStatus.BAD_REQUEST)
          .body(ErrorResponse.builder().error(e.getMessage()).build());
    } catch (KafkaAdminException e) {
      log.error("Failed to alter topic config: " + topic, e);
      return errorResponse(e.getMessage());
    }
  }

  // ============================================================================
  // Log Directories Endpoints
  // ============================================================================

  @Tag(
      name = "Kafka Admin",
      description =
          "Kafka admin operations. WARNING: Advanced operations - improper use can break deployments.")
  @GetMapping(path = "/log-dirs", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      summary = "Get log directory information for all brokers",
      description = "Retrieves log directory information including sizes and replica breakdown",
      responses = {
        @ApiResponse(
            responseCode = "200",
            description = "Successfully retrieved log directory information",
            content =
                @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = KafkaLogDirResponse.class))),
        @ApiResponse(
            responseCode = "403",
            description = "Caller is not authorized to access this endpoint",
            content =
                @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = ErrorResponse.class)))
      })
  public ResponseEntity<?> getLogDirs(HttpServletRequest httpServletRequest) {
    checkAuthorization(httpServletRequest, "getLogDirs");

    try {
      List<KafkaLogDirResponse.BrokerLogDirInfo> logDirs = kafkaAdminService.getLogDirs(null);
      return ResponseEntity.ok(KafkaLogDirResponse.builder().brokers(logDirs).build());
    } catch (KafkaAdminException e) {
      log.error("Failed to get log directories", e);
      return errorResponse(e.getMessage());
    }
  }

  @Tag(
      name = "Kafka Admin",
      description =
          "Kafka admin operations. WARNING: Advanced operations - improper use can break deployments.")
  @GetMapping(path = "/log-dirs/{brokerId}", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      summary = "Get log directory information for a specific broker",
      description = "Retrieves log directory information for a specific broker",
      responses = {
        @ApiResponse(
            responseCode = "200",
            description = "Successfully retrieved log directory information",
            content =
                @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = KafkaLogDirResponse.class))),
        @ApiResponse(
            responseCode = "403",
            description = "Caller is not authorized to access this endpoint",
            content =
                @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = ErrorResponse.class)))
      })
  public ResponseEntity<?> getLogDirsForBroker(
      HttpServletRequest httpServletRequest,
      @Parameter(in = ParameterIn.PATH, description = "Broker ID", required = true, example = "0")
          @PathVariable("brokerId")
          Integer brokerId) {
    checkAuthorization(httpServletRequest, "getLogDirsForBroker");

    try {
      List<KafkaLogDirResponse.BrokerLogDirInfo> logDirs =
          kafkaAdminService.getLogDirs(Set.of(brokerId));
      return ResponseEntity.ok(KafkaLogDirResponse.builder().brokers(logDirs).build());
    } catch (KafkaAdminException e) {
      log.error("Failed to get log directories for broker: " + brokerId, e);
      return errorResponse(e.getMessage());
    }
  }

  // ============================================================================
  // Consumer Group Endpoints
  // ============================================================================

  @Tag(
      name = "Kafka Admin",
      description =
          "Kafka admin operations. WARNING: Advanced operations - improper use can break deployments.")
  @GetMapping(path = "/consumer-groups", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      summary = "List all consumer groups",
      description = "Retrieves a list of all consumer groups in the cluster",
      responses = {
        @ApiResponse(
            responseCode = "200",
            description = "Successfully retrieved consumer group list",
            content =
                @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema =
                        @Schema(
                            implementation =
                                KafkaConsumerGroupResponse.ConsumerGroupListResponse.class))),
        @ApiResponse(
            responseCode = "403",
            description = "Caller is not authorized to access this endpoint",
            content =
                @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = ErrorResponse.class)))
      })
  public ResponseEntity<?> listConsumerGroups(HttpServletRequest httpServletRequest) {
    checkAuthorization(httpServletRequest, "listConsumerGroups");

    try {
      List<KafkaConsumerGroupResponse.ConsumerGroupSummary> groups =
          kafkaAdminService.listConsumerGroups();
      KafkaConsumerGroupResponse.ConsumerGroupListResponse response =
          KafkaConsumerGroupResponse.ConsumerGroupListResponse.builder()
              .groups(groups)
              .count(groups.size())
              .build();
      return ResponseEntity.ok(response);
    } catch (KafkaAdminException e) {
      log.error("Failed to list consumer groups", e);
      return errorResponse(e.getMessage());
    }
  }

  @Tag(
      name = "Kafka Admin",
      description =
          "Kafka admin operations. WARNING: Advanced operations - improper use can break deployments.")
  @GetMapping(path = "/consumer-groups/{groupId}", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      summary = "Describe a consumer group",
      description =
          "Retrieves detailed information about a consumer group including state, members, and offsets",
      responses = {
        @ApiResponse(
            responseCode = "200",
            description = "Successfully retrieved consumer group information",
            content =
                @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = KafkaConsumerGroupResponse.class))),
        @ApiResponse(
            responseCode = "403",
            description = "Caller is not authorized to access this endpoint",
            content =
                @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = ErrorResponse.class))),
        @ApiResponse(
            responseCode = "404",
            description = "Consumer group not found",
            content =
                @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = ErrorResponse.class)))
      })
  public ResponseEntity<?> describeConsumerGroup(
      HttpServletRequest httpServletRequest,
      @Parameter(
              in = ParameterIn.PATH,
              description = "Consumer group ID",
              required = true,
              example = "datahub-mcp-consumer")
          @PathVariable("groupId")
          String groupId) {
    checkAuthorization(httpServletRequest, "describeConsumerGroup");

    try {
      KafkaConsumerGroupResponse groupInfo = kafkaAdminService.describeConsumerGroup(groupId);
      return ResponseEntity.ok(groupInfo);
    } catch (KafkaAdminException.ConsumerGroupNotFoundException e) {
      return ResponseEntity.status(HttpStatus.NOT_FOUND)
          .body(ErrorResponse.builder().error(e.getMessage()).build());
    } catch (KafkaAdminException e) {
      log.error("Failed to describe consumer group: " + groupId, e);
      return errorResponse(e.getMessage());
    }
  }

  @Tag(
      name = "Kafka Admin",
      description =
          "Kafka admin operations. WARNING: Advanced operations - improper use can break deployments.")
  @DeleteMapping(path = "/consumer-groups/{groupId}", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      summary = "Delete an inactive consumer group",
      description = "Deletes a consumer group. Only works for inactive groups.",
      responses = {
        @ApiResponse(
            responseCode = "200",
            description = "Successfully deleted consumer group",
            content =
                @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = ErrorResponse.class))),
        @ApiResponse(
            responseCode = "400",
            description = "Consumer group is not empty or active",
            content =
                @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = ErrorResponse.class))),
        @ApiResponse(
            responseCode = "403",
            description = "Caller is not authorized to access this endpoint",
            content =
                @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = ErrorResponse.class)))
      })
  public ResponseEntity<?> deleteConsumerGroup(
      HttpServletRequest httpServletRequest,
      @Parameter(
              in = ParameterIn.PATH,
              description = "Consumer group ID",
              required = true,
              example = "datahub-mcp-consumer")
          @PathVariable("groupId")
          String groupId) {
    checkAuthorization(httpServletRequest, "deleteConsumerGroup");

    try {
      kafkaAdminService.deleteConsumerGroup(groupId);
      return ResponseEntity.ok(
          ErrorResponse.builder().error("Successfully deleted consumer group: " + groupId).build());
    } catch (KafkaAdminException.ConsumerGroupNotEmptyException e) {
      return ResponseEntity.status(HttpStatus.BAD_REQUEST)
          .body(ErrorResponse.builder().error(e.getMessage()).build());
    } catch (KafkaAdminException e) {
      log.error("Failed to delete consumer group: " + groupId, e);
      return errorResponse(e.getMessage());
    }
  }

  @Tag(
      name = "Kafka Admin",
      description =
          "Kafka admin operations. WARNING: Advanced operations - improper use can break deployments.")
  @PostMapping(
      path = "/consumer-groups/{groupId}/offsets/reset",
      produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      summary = "Reset consumer group offsets",
      description =
          "Resets offsets for a consumer group. **Restricted to DataHub topic partitions only.**\n\n"
              + "### Strategies\n"
              + "- **earliest**: Reset to beginning of topic (reprocess all messages)\n"
              + "- **latest**: Reset to end of topic (skip all pending messages)\n"
              + "- **to-offset**: Reset to a specific offset number\n"
              + "- **shift-by**: Move offset forward/backward by N messages\n\n"
              + "### Examples\n"
              + "```json\n"
              + "// Preview reset to beginning (dry run)\n"
              + "{\"strategy\": \"earliest\", \"dryRun\": true}\n\n"
              + "// Reset to specific offset 1000\n"
              + "{\"strategy\": \"to-offset\", \"value\": \"1000\"}\n\n"
              + "// Move backward 100 messages\n"
              + "{\"strategy\": \"shift-by\", \"value\": \"-100\"}\n\n"
              + "// Reset only partitions 0 and 1\n"
              + "{\"strategy\": \"earliest\", \"partitions\": [0, 1]}\n"
              + "```\n\n"
              + "**WARNING**: Stop the consumer group before resetting offsets.",
      responses = {
        @ApiResponse(
            responseCode = "200",
            description = "Successfully reset offsets",
            content =
                @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema =
                        @Schema(
                            implementation =
                                KafkaConsumerGroupResponse.ResetOffsetsResponse.class))),
        @ApiResponse(
            responseCode = "400",
            description = "Invalid request",
            content =
                @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = ErrorResponse.class))),
        @ApiResponse(
            responseCode = "403",
            description = "Caller is not authorized or partitions are not DataHub topics",
            content =
                @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = ErrorResponse.class)))
      })
  public ResponseEntity<?> resetConsumerGroupOffsets(
      HttpServletRequest httpServletRequest,
      @Parameter(
              in = ParameterIn.PATH,
              description = "Consumer group ID",
              required = true,
              example = "datahub-mcp-consumer")
          @PathVariable("groupId")
          String groupId,
      @RequestBody KafkaRequests.ResetOffsetsRequest request) {
    checkAuthorization(httpServletRequest, "resetConsumerGroupOffsets");

    if (request == null || request.getTopic() == null || request.getStrategy() == null) {
      return ResponseEntity.status(HttpStatus.BAD_REQUEST)
          .body(ErrorResponse.builder().error("Topic and strategy are required").build());
    }

    try {
      boolean dryRun = request.getDryRun() != null && request.getDryRun();
      List<KafkaConsumerGroupResponse.PartitionResetInfo> resetInfos =
          kafkaAdminService.resetConsumerGroupOffsets(
              groupId,
              request.getTopic(),
              request.getStrategy(),
              request.getValue(),
              request.getPartitions(),
              dryRun);

      KafkaConsumerGroupResponse.ResetOffsetsResponse response =
          KafkaConsumerGroupResponse.ResetOffsetsResponse.builder()
              .dryRun(dryRun)
              .groupId(groupId)
              .topic(request.getTopic())
              .partitionResets(resetInfos)
              .build();

      return ResponseEntity.ok(response);
    } catch (KafkaAdminException e) {
      log.error("Failed to reset consumer group offsets: " + groupId, e);
      return ResponseEntity.status(HttpStatus.BAD_REQUEST)
          .body(ErrorResponse.builder().error(e.getMessage()).build());
    }
  }

  // ============================================================================
  // Message Retrieval Endpoint
  // ============================================================================

  @Tag(
      name = "Kafka Admin",
      description =
          "Kafka admin operations. WARNING: Advanced operations - improper use can break deployments.")
  @GetMapping(path = "/messages/{topic}", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      summary = "Retrieve messages from a DataHub topic",
      description =
          "Retrieves messages from a Kafka topic. Restricted to DataHub topics only (mcp, mcp-failed, mcl-versioned, mcl-timeseries).",
      responses = {
        @ApiResponse(
            responseCode = "200",
            description = "Successfully retrieved messages",
            content =
                @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = KafkaMessageResponse.class))),
        @ApiResponse(
            responseCode = "400",
            description = "Invalid request",
            content =
                @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = ErrorResponse.class))),
        @ApiResponse(
            responseCode = "403",
            description = "Caller is not authorized or topic is not a DataHub topic",
            content =
                @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = ErrorResponse.class)))
      })
  public ResponseEntity<?> getMessages(
      HttpServletRequest httpServletRequest,
      @Parameter(
              in = ParameterIn.PATH,
              description =
                  "Topic alias (mcp, mcl-versioned, mcl-timeseries, platform-event, etc.)",
              required = true,
              example = "mcp")
          @PathVariable("topic")
          String topic,
      @Parameter(
              in = ParameterIn.QUERY,
              description = "Partition number",
              required = true,
              example = "0")
          @RequestParam(value = "partition")
          Integer partition,
      @Parameter(
              in = ParameterIn.QUERY,
              description = "Starting offset",
              required = true,
              example = "0")
          @RequestParam(value = "offset")
          Long offset,
      @Parameter(
              in = ParameterIn.QUERY,
              description = "Number of messages to retrieve",
              example = "100")
          @RequestParam(value = "count", defaultValue = "100")
          Integer count,
      @Parameter(
              in = ParameterIn.QUERY,
              description =
                  "Output format for message values: "
                      + "'json' (default) - Avro record as JSON string, "
                      + "'avro' - Full Avro representation with schema, "
                      + "'raw' - Base64 encoded binary",
              example = "json",
              schema =
                  @Schema(
                      type = "string",
                      allowableValues = {"json", "avro", "raw"},
                      defaultValue = "json"))
          @RequestParam(value = "format", defaultValue = "json")
          String format,
      @Parameter(
              in = ParameterIn.QUERY,
              description = "Include message headers in response",
              example = "false")
          @RequestParam(value = "includeHeaders", defaultValue = "false")
          Boolean includeHeaders) {
    checkAuthorization(httpServletRequest, "getMessages");

    if (partition == null || offset == null) {
      return ResponseEntity.status(HttpStatus.BAD_REQUEST)
          .body(ErrorResponse.builder().error("Partition and offset are required").build());
    }

    // Validate format parameter
    if (!Set.of("json", "avro", "raw").contains(format.toLowerCase())) {
      return ResponseEntity.status(HttpStatus.BAD_REQUEST)
          .body(
              ErrorResponse.builder()
                  .error("Invalid format. Allowed values: json, avro, raw")
                  .build());
    }

    try {
      KafkaMessageResponse batch =
          kafkaAdminService.getMessages(
              topic, partition, offset, count, format.toLowerCase(), includeHeaders);
      return ResponseEntity.ok(batch);
    } catch (KafkaAdminException.InvalidAliasException e) {
      return ResponseEntity.status(HttpStatus.BAD_REQUEST)
          .body(ErrorResponse.builder().error(e.getMessage()).build());
    } catch (KafkaAdminException e) {
      log.error("Failed to retrieve messages from topic: " + topic, e);
      return errorResponse(e.getMessage());
    }
  }

  // ============================================================================
  // Replay Failed MCPs Endpoint
  // ============================================================================

  @Tag(
      name = "Kafka Admin",
      description =
          "Kafka admin operations. WARNING: Advanced operations - improper use can break deployments.")
  @PostMapping(path = "/messages/mcp-failed/replay", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      summary = "Replay failed MCPs",
      description =
          "Re-publishes failed MCPs from the mcp-failed topic to the mcp topic for reprocessing.",
      responses = {
        @ApiResponse(
            responseCode = "200",
            description = "Successfully replayed messages",
            content =
                @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = KafkaMessageResponse.ReplayResponse.class))),
        @ApiResponse(
            responseCode = "400",
            description = "Invalid request",
            content =
                @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = ErrorResponse.class))),
        @ApiResponse(
            responseCode = "403",
            description = "Caller is not authorized",
            content =
                @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = ErrorResponse.class)))
      })
  public ResponseEntity<?> replayFailedMCPs(
      HttpServletRequest httpServletRequest, @RequestBody KafkaRequests.ReplayRequest request) {
    checkAuthorization(httpServletRequest, "replayFailedMCPs");

    if (request == null || request.getStartOffset() == null || request.getPartition() == null) {
      return ResponseEntity.status(HttpStatus.BAD_REQUEST)
          .body(ErrorResponse.builder().error("Partition and startOffset are required").build());
    }

    try {
      boolean dryRun = request.getDryRun() != null && request.getDryRun();
      KafkaMessageResponse.ReplayResponse result =
          kafkaAdminService.replayFailedMCPs(
              request.getPartition(),
              request.getStartOffset(),
              request.getEndOffset(),
              request.getCount(),
              dryRun);

      return ResponseEntity.ok(result);
    } catch (KafkaAdminException e) {
      log.error("Failed to replay failed MCPs", e);
      return errorResponse(e.getMessage());
    }
  }

  // ============================================================================
  // Helper Methods for Offset Response
  // ============================================================================

  private KafkaOffsetResponse convertToResponse(
      String consumerGroupId,
      Map<TopicPartition, OffsetAndMetadata> offsetMap,
      Map<TopicPartition, Long> endOffsets,
      boolean detailed) {

    // Early return if map is empty
    if (offsetMap == null || offsetMap.isEmpty()) {
      return KafkaOffsetResponse.builder()
          .consumerGroupId(consumerGroupId)
          .topics(new LinkedHashMap<>())
          .build();
    }

    // Group by topic
    Map<String, Map<Integer, KafkaOffsetResponse.PartitionInfo>> topicToPartitions =
        new HashMap<>();
    Map<String, List<Long>> topicToLags = new HashMap<>();

    // Process each entry in the offset map
    for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsetMap.entrySet()) {
      TopicPartition tp = entry.getKey();
      OffsetAndMetadata offset = entry.getValue();

      String topic = tp.topic();
      int partition = tp.partition();

      // Calculate lag if we have end offset information
      long consumerOffset = offset.offset();
      Long endOffset = endOffsets.get(tp);
      Long lag = (endOffset != null) ? Math.max(0, endOffset - consumerOffset) : null;

      // Create partition info
      KafkaOffsetResponse.PartitionInfo partitionInfo =
          KafkaOffsetResponse.PartitionInfo.builder().offset(consumerOffset).lag(lag).build();

      // Add metadata if present
      if (offset.metadata() != null && !offset.metadata().isEmpty()) {
        partitionInfo.setMetadata(offset.metadata());
      }

      // Store partition info by topic and partition ID
      topicToPartitions.computeIfAbsent(topic, k -> new HashMap<>()).put(partition, partitionInfo);

      // Store lag for aggregate calculations
      if (lag != null) {
        topicToLags.computeIfAbsent(topic, k -> new ArrayList<>()).add(lag);
      }
    }

    // Create the response structure with sorted topics and partitions
    Map<String, KafkaOffsetResponse.TopicOffsetInfo> topicMap = new LinkedHashMap<>();

    // Process topics in sorted order
    topicToPartitions.entrySet().stream()
        .sorted(Map.Entry.comparingByKey())
        .forEach(
            topicEntry -> {
              String topic = topicEntry.getKey();
              Map<Integer, KafkaOffsetResponse.PartitionInfo> partitionMap = topicEntry.getValue();

              // Create sorted map of partitions
              Map<String, KafkaOffsetResponse.PartitionInfo> sortedPartitions =
                  new LinkedHashMap<>();
              partitionMap.entrySet().stream()
                  .sorted(Map.Entry.comparingByKey())
                  .forEach(e -> sortedPartitions.put(String.valueOf(e.getKey()), e.getValue()));

              // Calculate metrics if we have lag information
              KafkaOffsetResponse.LagMetrics metrics = null;
              List<Long> lags = topicToLags.get(topic);
              if (lags != null && !lags.isEmpty()) {
                metrics = calculateLagMetrics(lags);
              }

              // Create topic info
              KafkaOffsetResponse.TopicOffsetInfo topicInfo =
                  KafkaOffsetResponse.TopicOffsetInfo.builder()
                      .partitions(detailed ? sortedPartitions : null)
                      .metrics(metrics)
                      .build();

              topicMap.put(topic, topicInfo);
            });

    return KafkaOffsetResponse.builder().consumerGroupId(consumerGroupId).topics(topicMap).build();
  }

  /**
   * Calculates aggregate lag metrics from a list of lag values.
   *
   * @param lags List of lag values
   * @return Structured lag metrics
   */
  private KafkaOffsetResponse.LagMetrics calculateLagMetrics(List<Long> lags) {
    if (lags == null || lags.isEmpty()) {
      return null;
    }

    // Sort the lags for median calculation
    List<Long> sortedLags = new ArrayList<>(lags);
    Collections.sort(sortedLags);

    // Calculate max lag
    long maxLag = sortedLags.get(sortedLags.size() - 1);

    // Calculate median lag
    long medianLag;
    int middle = sortedLags.size() / 2;
    if (sortedLags.size() % 2 == 0) {
      // Even number of elements, average the middle two
      medianLag = (sortedLags.get(middle - 1) + sortedLags.get(middle)) / 2;
    } else {
      // Odd number of elements, take the middle one
      medianLag = sortedLags.get(middle);
    }

    // Calculate total lag
    long totalLag = 0;
    for (Long lag : lags) {
      totalLag += lag;
    }

    // Calculate average lag
    double avgLag = (double) totalLag / lags.size();

    return KafkaOffsetResponse.LagMetrics.builder()
        .maxLag(maxLag)
        .medianLag(medianLag)
        .totalLag(totalLag)
        .avgLag(Math.round(avgLag))
        .build();
  }

  /** Simple error response class for auth failures. */
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  @Schema(description = "Error response")
  public static class ErrorResponse {
    @Schema(description = "Error message")
    private String error;
  }
}
