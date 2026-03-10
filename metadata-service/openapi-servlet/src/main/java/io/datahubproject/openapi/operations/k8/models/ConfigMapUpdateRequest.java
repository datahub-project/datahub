package io.datahubproject.openapi.operations.k8.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Request to update a ConfigMap's data entries. */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
@Schema(description = "Request to update ConfigMap data entries")
public class ConfigMapUpdateRequest {

  @Nullable
  @Schema(
      description = "Data entries to add or update (key -> value)",
      example = "{\"config.yaml\": \"key: value\", \"settings.json\": \"{\\\"enabled\\\": true}\"}")
  private Map<String, String> set;

  @Nullable
  @Schema(
      description = "Data entry keys to remove",
      example = "[\"old-config.yaml\", \"deprecated.properties\"]")
  private List<String> remove;
}
