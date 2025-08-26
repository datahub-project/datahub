package com.linkedin.metadata.config;

import lombok.Data;

@Data
public class ForwardingActionConfiguration {
  boolean enabled;
  String recipe;
}
