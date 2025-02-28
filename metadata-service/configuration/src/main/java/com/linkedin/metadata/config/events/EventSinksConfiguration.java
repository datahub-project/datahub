package com.linkedin.metadata.config.events;

import lombok.Data;

/** POJO representing the "eventSinks" configuration block in application.yaml. */
@Data
public class EventSinksConfiguration {
  /** Configuration for entityChangeEvent related sinks. */
  public EntityChangeEventSinksConfiguration entityChangeEvent;
}
