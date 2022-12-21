package com.linkedin.metadata.config;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Setter;


@Data
@AllArgsConstructor
public class ProducerConfiguration {

  @Setter(AccessLevel.NONE)
  final private int retryCount;

  @Setter(AccessLevel.NONE)
  final private int deliveryTimeout;

  @Setter(AccessLevel.NONE)
  final private int requestTimeout;

  @Setter(AccessLevel.NONE)
  final private int backoffTimeout;
}
