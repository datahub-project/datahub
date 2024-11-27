package com.linkedin.metadata.entity.versioning;

import lombok.Data;

@Data
public class VersionPropertiesInput {
  private String comment;
  private String label;
  private Long sourceCreationTimestamp;
  private String sourceCreator;
}
