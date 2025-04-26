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
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
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
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/openapi/operations/kafka")
@Slf4j
public class KafkaController {

  private final OperationContext systemOperationContext;
  private final AuthorizerChain authorizerChain;
  private final MCPTraceReader mcpTraceReader;
  private final MCLTraceReader mclTraceReader;
  private final MCLTraceReader mclTimeseriesTraceReader;

  public KafkaController(
      @Qualifier("systemOperationContext") OperationContext systemOperationContext,
      AuthorizerChain authorizerChain,
      MCPTraceReader mcpTraceReader,
      @Qualifier("mclVersionedTraceReader") MCLTraceReader mclTraceReader,
      @Qualifier("mclTimeseriesTraceReader") MCLTraceReader mclTimeseriesTraceReader) {
    this.systemOperationContext = systemOperationContext;
    this.authorizerChain = authorizerChain;
    this.mcpTraceReader = mcpTraceReader;
    this.mclTraceReader = mclTraceReader;
    this.mclTimeseriesTraceReader = mclTimeseriesTraceReader;
  }

  @Tag(
      name = "Kafka Offsets",
      description = "APIs for retrieving Kafka consumer offset information")
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
  @Parameter(
      name = "skipCache",
      description = "Whether to bypass the offset cache and fetch fresh values directly from Kafka",
      schema = @Schema(type = "boolean", defaultValue = "false"),
      required = false)
  @Parameter(
      name = "detailed",
      description = "Whether to include per-partition offset details in the response",
      schema = @Schema(type = "boolean", defaultValue = "false"),
      required = false)
  public ResponseEntity<?> getMCPOffsets(
      HttpServletRequest httpServletRequest,
      @RequestParam(value = "skipCache", defaultValue = "false") boolean skipCache,
      @RequestParam(value = "detailed", defaultValue = "false") boolean detailed) {
    Authentication authentication = AuthenticationContext.getAuthentication();
    String actorUrnStr = authentication.getActor().toUrnStr();

    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder()
                .buildOpenapi(actorUrnStr, httpServletRequest, "getMCPOffsets", List.of()),
            authorizerChain,
            authentication,
            true);

    if (!AuthUtil.isAPIAuthorized(opContext, PoliciesConfig.MANAGE_SYSTEM_OPERATIONS_PRIVILEGE)) {
      return ResponseEntity.status(HttpStatus.FORBIDDEN)
          .body(
              ErrorResponse.builder()
                  .error(actorUrnStr + " is not authorized to get kafka offsets")
                  .build());
    }

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
      name = "Kafka Offsets",
      description = "APIs for retrieving Kafka consumer offset information")
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
  @Parameter(
      name = "skipCache",
      description = "Whether to bypass the offset cache and fetch fresh values directly from Kafka",
      schema = @Schema(type = "boolean", defaultValue = "false"),
      required = false)
  @Parameter(
      name = "detailed",
      description = "Whether to include per-partition offset details in the response",
      schema = @Schema(type = "boolean", defaultValue = "false"),
      required = false)
  public ResponseEntity<?> getMCLOffsets(
      HttpServletRequest httpServletRequest,
      @RequestParam(value = "skipCache", defaultValue = "false") boolean skipCache,
      @RequestParam(value = "detailed", defaultValue = "false") boolean detailed) {
    Authentication authentication = AuthenticationContext.getAuthentication();
    String actorUrnStr = authentication.getActor().toUrnStr();

    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder()
                .buildOpenapi(actorUrnStr, httpServletRequest, "getMCLOffsets", List.of()),
            authorizerChain,
            authentication,
            true);

    if (!AuthUtil.isAPIAuthorized(opContext, PoliciesConfig.MANAGE_SYSTEM_OPERATIONS_PRIVILEGE)) {
      return ResponseEntity.status(HttpStatus.FORBIDDEN)
          .body(
              ErrorResponse.builder()
                  .error(actorUrnStr + " is not authorized to get kafka offsets")
                  .build());
    }

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
      name = "Kafka Offsets",
      description = "APIs for retrieving Kafka consumer offset information")
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
  @Parameter(
      name = "skipCache",
      description = "Whether to bypass the offset cache and fetch fresh values directly from Kafka",
      schema = @Schema(type = "boolean", defaultValue = "false"),
      required = false)
  @Parameter(
      name = "detailed",
      description = "Whether to include per-partition offset details in the response",
      schema = @Schema(type = "boolean", defaultValue = "false"),
      required = false)
  public ResponseEntity<?> getMCLTimeseriesOffsets(
      HttpServletRequest httpServletRequest,
      @RequestParam(value = "skipCache", defaultValue = "false") boolean skipCache,
      @RequestParam(value = "detailed", defaultValue = "false") boolean detailed) {
    Authentication authentication = AuthenticationContext.getAuthentication();
    String actorUrnStr = authentication.getActor().toUrnStr();

    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder()
                .buildOpenapi(actorUrnStr, httpServletRequest, "getMCLOffsets", List.of()),
            authorizerChain,
            authentication,
            true);

    if (!AuthUtil.isAPIAuthorized(opContext, PoliciesConfig.MANAGE_SYSTEM_OPERATIONS_PRIVILEGE)) {
      return ResponseEntity.status(HttpStatus.FORBIDDEN)
          .body(
              ErrorResponse.builder()
                  .error(actorUrnStr + " is not authorized to get kafka offsets")
                  .build());
    }

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

  /**
   * Converts the Kafka offset data into a strongly-typed response object.
   *
   * @param consumerGroupId The consumer group ID
   * @param offsetMap Map of TopicPartition to OffsetAndMetadata
   * @param endOffsets Map of TopicPartition to end offset
   * @param detailed Whether to include detailed partition information
   * @return A structured KafkaOffsetResponse object
   */
  private KafkaOffsetResponse convertToResponse(
      String consumerGroupId,
      Map<TopicPartition, OffsetAndMetadata> offsetMap,
      Map<TopicPartition, Long> endOffsets,
      boolean detailed) {

    // Early return if map is empty
    if (offsetMap == null || offsetMap.isEmpty()) {
      return new KafkaOffsetResponse();
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

    // Create map of consumer group ID to its topic information
    KafkaOffsetResponse response = new KafkaOffsetResponse();
    response.put(consumerGroupId, topicMap);
    return response;
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
