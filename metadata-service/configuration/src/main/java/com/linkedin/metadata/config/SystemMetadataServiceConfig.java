package com.linkedin.metadata.config;

import com.linkedin.metadata.config.shared.LimitConfig;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder(toBuilder = true)
@AllArgsConstructor
@NoArgsConstructor
public class SystemMetadataServiceConfig {
  private LimitConfig limit;
}
