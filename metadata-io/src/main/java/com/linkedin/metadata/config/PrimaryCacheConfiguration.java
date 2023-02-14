package com.linkedin.metadata.config;

import lombok.Data;


@Data
public class PrimaryCacheConfiguration {
  long ttlSeconds;
  long maxSize;
}
