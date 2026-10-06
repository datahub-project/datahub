package com.linkedin.metadata.timeline.data;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.json.JsonPatch;
import java.util.List;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Getter
@Builder
public class ChangeTransaction {
  long timestamp;
  String actor;
  String proxy;
  String reporter;
  String semVer;
  SemanticChangeType semVerChange;
  @Setter List<ChangeEvent> changeEvents;

  // JsonPatch (parsson JsonPatchImpl) is not a Jackson bean, so serializing it directly throws
  // "No serializer found" and fails the whole /timeline response when raw=true. Emit its RFC-6902
  // JSON array form instead.
  @ArraySchema(schema = @Schema(implementation = PatchOperation.class))
  @JsonSerialize(using = JsonPatchSerializer.class)
  JsonPatch rawDiff;

  @Setter String versionStamp;

  public void setSemanticVersion(String semanticVersion) {
    this.semVer = semanticVersion;
  }

  public void setSemVerChange(SemanticChangeType semVerChange) {
    this.semVerChange = semVerChange;
  }
}
