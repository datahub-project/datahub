package com.linkedin.metadata.datahubusage;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;
import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor(force = true, access = AccessLevel.PRIVATE)
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class ExternalAuditEventsSearchRequest {
  @JsonProperty("eventTypes")
  @Nullable
  @Schema(
      description = "List of event types to filter by, if empty then looks at all event types.",
      requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  List<String> eventTypes;

  @JsonProperty("entityTypes")
  @Nullable
  @Schema(
      description = "List of entity types to filter by, if empty then looks at all entity types.",
      requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  List<String> entityTypes;

  @JsonProperty("aspectTypes")
  @Nullable
  @Schema(
      description = "List of aspect types to filter by, if empty then looks at all aspect types.",
      requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  List<String> aspectTypes;

  @JsonProperty("actorUrns")
  @Nullable
  @Schema(
      description = "List of actor urns to filter by, if empty then looks at all actors.",
      requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  List<String> actorUrns;

  @JsonIgnore
  @Schema(
      description = "Beginning time to start filtering. By default will do a 1 day lookback.",
      defaultValue = "-1")
  long startTime;

  @JsonIgnore
  @Schema(
      description = "End time to filter until. By default will end at the current time.",
      defaultValue = "-1")
  long endTime;

  @JsonIgnore
  @Schema(
      description =
          "Maximum number of events to return that match. If more events exist will "
              + "return a scroll ID to page by. Defaults to 10.",
      defaultValue = "10")
  int size;

  @JsonIgnore
  @Schema(
      description =
          "The next scroll ID to look for based on the current search parameters. "
              + "Should be pulled from response for the previous page.",
      requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  String scrollId;

  @JsonIgnore
  @Schema(description = "Whether to include the full raw event or not.", defaultValue = "true")
  boolean includeRaw;
}
