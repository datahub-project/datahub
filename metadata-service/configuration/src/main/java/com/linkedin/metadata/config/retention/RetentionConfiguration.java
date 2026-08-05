package com.linkedin.metadata.config.retention;

import lombok.Data;

/** POJO representing the "datahub.retention" configuration block in application.yaml. */
@Data
public class RetentionConfiguration {
  private RetentionBufferProperties buffer;
}
