package io.datahubproject.openapi.operations.kafka.models;

import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Response model for Kafka message retrieval API endpoint. */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Schema(description = "Kafka message retrieval response")
public class KafkaMessageResponse {

  @Schema(description = "The topic name")
  private String topic;

  @Schema(description = "DataHub topic alias if applicable", example = "mcp")
  private String dataHubAlias;

  @Schema(description = "List of retrieved messages")
  private List<MessageInfo> messages;

  @Schema(description = "Number of messages retrieved")
  private Integer count;

  @Schema(description = "Whether there are more messages available after the retrieved batch")
  private Boolean hasMore;

  @Schema(description = "The next offset to continue reading from, if hasMore is true")
  private Long nextOffset;

  /** Information about a single Kafka message. */
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  @Schema(description = "Information about a Kafka message")
  public static class MessageInfo {

    @Schema(description = "Partition the message was retrieved from", example = "0")
    private Integer partition;

    @Schema(description = "Offset of the message within the partition", example = "12345")
    private Long offset;

    @Schema(description = "Timestamp of the message in epoch milliseconds")
    private Long timestamp;

    @Schema(
        description = "Timestamp type",
        example = "CreateTime",
        allowableValues = {"CreateTime", "LogAppendTime", "NoTimestampType"})
    private String timestampType;

    @Schema(description = "Message key (may be null)")
    private String key;

    @Schema(description = "Message value in the requested format")
    private Object value;

    @Schema(
        description = "Format of the value field",
        example = "json",
        allowableValues = {"json", "avro", "base64"})
    private String format;

    @Schema(description = "Message headers as key-value pairs")
    private List<HeaderInfo> headers;

    @Schema(
        description = "For failed MCPs, indicates if the original MCP can be extracted for replay")
    private Boolean replayable;
  }

  /** Message header information. */
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  @Schema(description = "Kafka message header")
  public static class HeaderInfo {

    @Schema(description = "Header key")
    private String key;

    @Schema(description = "Header value (as string)")
    private String value;
  }

  /** Response for replay operations. */
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  @Schema(description = "Result of replay operation")
  public static class ReplayResponse {

    @Schema(description = "Whether this was a dry run")
    private Boolean dryRun;

    @Schema(description = "Source topic (mcp-failed)")
    private String sourceTopic;

    @Schema(description = "Destination topic (mcp)")
    private String destinationTopic;

    @Schema(description = "Partition replayed from")
    private Integer partition;

    @Schema(description = "Starting offset of replay range")
    private Long startOffset;

    @Schema(description = "Ending offset of replay range")
    private Long endOffset;

    @Schema(description = "Number of messages successfully replayed")
    private Integer successCount;

    @Schema(description = "Number of messages that failed to replay")
    private Integer failureCount;

    @Schema(description = "Details of each replayed message")
    private List<ReplayedMessageInfo> messages;

    @Schema(description = "Errors encountered during replay")
    private List<String> errors;
  }

  /** Information about a replayed message. */
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  @Schema(description = "Information about a replayed message")
  public static class ReplayedMessageInfo {

    @Schema(description = "Source offset from mcp-failed topic")
    private Long sourceOffset;

    @Schema(description = "Destination offset in mcp topic (if successful)")
    private Long destinationOffset;

    @Schema(description = "Message key (URN)")
    private String key;

    @Schema(description = "Whether replay was successful")
    private Boolean success;

    @Schema(description = "Error message if replay failed")
    private String error;
  }
}
