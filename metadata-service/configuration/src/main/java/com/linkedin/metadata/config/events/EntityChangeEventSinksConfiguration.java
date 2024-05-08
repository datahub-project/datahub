package com.linkedin.metadata.config.events;

import java.util.List;
import lombok.Data;

/** POJO representing the "eventSinks.entityChangeEvent" configuration block in application.yaml. */
@Data
public class EntityChangeEventSinksConfiguration {
  /** Configuration for individual change event sinks. */
  public List<EntityChangeEventSinkConfiguration> sinks;
}
