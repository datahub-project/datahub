package com.linkedin.metadata.datahubusage;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.linkedin.metadata.datahubusage.event.UsageEventResult;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;
import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor(force = true, access = AccessLevel.PRIVATE)
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@Builder
public class ExternalAuditEventsSearchResponse {
  @JsonProperty("nextScrollId")
  @Nullable
  @Schema(
      description = "The next page's scroll ID if there are more results.",
      requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  String nextScrollId;

  @JsonProperty("count")
  @Schema(description = "Count of returned search hits.", defaultValue = "0")
  int count;

  @JsonProperty("total")
  @Schema(description = "Total count of hits. Only calculated up to 10,000.", defaultValue = "0")
  int total;

  @JsonProperty("usageEvents")
  @Schema(
      description = "Usage Event results from the search.",
      requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  List<UsageEventResult> usageEvents;
}
