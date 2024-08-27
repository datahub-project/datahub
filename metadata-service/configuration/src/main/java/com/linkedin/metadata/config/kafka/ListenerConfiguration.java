package com.linkedin.metadata.config.kafka;

import lombok.Data;

@Data
public class ListenerConfiguration {
  private int concurrency;
}
