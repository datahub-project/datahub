package com.linkedin.metadata.config;

import java.util.List;
import lombok.Data;

/**
 * POJO representing the "changeEvents" configuration block in application.yml.
 */
@Data
public class ChangeEventConfiguration {
  /**
   * Configuration for individual change event sinks.
   */
  public List<ChangeEventSinkConfiguration> sinks;
}