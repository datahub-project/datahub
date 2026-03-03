package com.linkedin.metadata.datahubusage.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.LinkedHashMap;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@Data
@NoArgsConstructor(force = true, access = AccessLevel.PROTECTED)
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@SuperBuilder
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.EXISTING_PROPERTY,
    property = "eventType",
    defaultImpl = UsageEventResult.class,
    visible = true)
@JsonSubTypes({
  @JsonSubTypes.Type(value = CreateAccessTokenEvent.class, name = "CreateAccessTokenEvent"),
  @JsonSubTypes.Type(value = CreateIngestionSourceEvent.class, name = "CreateIngestionSourceEvent"),
  @JsonSubTypes.Type(value = CreatePolicyEvent.class, name = "CreatePolicyEvent"),
  @JsonSubTypes.Type(value = CreateUserEvent.class, name = "CreateUserEvent"),
  @JsonSubTypes.Type(value = DeleteEntityEvent.class, name = "DeleteEntityEvent"),
  @JsonSubTypes.Type(value = DeletePolicyEvent.class, name = "DeletePolicyEvent"),
  @JsonSubTypes.Type(value = LogInEvent.class, name = "LogInEvent"),
  @JsonSubTypes.Type(value = RevokeAccessTokenEvent.class, name = "RevokeAccessTokenEvent"),
  @JsonSubTypes.Type(value = EntityEvent.class, name = "EntityEvent"),
  @JsonSubTypes.Type(value = FailedLogInEvent.class, name = "FailedLogInEvent"),
  @JsonSubTypes.Type(value = UpdateAspectEvent.class, name = "UpdateAspectEvent"),
  @JsonSubTypes.Type(value = UpdateIngestionSourceEvent.class, name = "UpdateIngestionSourceEvent"),
  @JsonSubTypes.Type(value = UpdatePolicyEvent.class, name = "UpdatePolicyEvent"),
  @JsonSubTypes.Type(value = UpdateUserEvent.class, name = "UpdateUserEvent")
})
public class UsageEventResult {
  @JsonProperty("eventType")
  @Schema(description = "Type of event.")
  protected String eventType;

  @JsonProperty("timestamp")
  @Schema(description = "Timestamp of the event.")
  protected long timestamp;

  @JsonProperty("actorUrn")
  @Schema(description = "Actor that performed the described action.")
  protected String actorUrn;

  @JsonProperty("sourceIP")
  @Schema(description = "IP of the actor the performed the action.")
  protected String sourceIP;

  @JsonProperty("eventSource")
  @Schema(description = "Source API of the event.")
  protected EventSource eventSource;

  @JsonProperty("userAgent")
  @Schema(
      description =
          "User agent string captured from http request, not present if not coming in from a client request.")
  protected String userAgent;

  @JsonProperty("telemetryTraceId")
  @Schema(description = "Trace Id from system telemetry.")
  protected String telemetryTraceId;

  @JsonProperty("rawUsageEvent")
  @Schema(description = "Full raw usage event contents.")
  protected LinkedHashMap<String, Object> rawUsageEvent;
}
