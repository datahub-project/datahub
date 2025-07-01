package com.linkedin.metadata.timeline.data;

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
  List<ChangeEvent> changeEvents;

  @ArraySchema(schema = @Schema(implementation = PatchOperation.class))
  JsonPatch rawDiff;

  @Setter String versionStamp;

  public void setSemanticVersion(String semanticVersion) {
    this.semVer = semanticVersion;
  }

  public void setSemVerChange(SemanticChangeType semVerChange) {
    this.semVerChange = semVerChange;
  }
}
