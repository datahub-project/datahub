package com.linkedin.metadata.config;

import lombok.Data;
import lombok.experimental.Accessors;

@Accessors(chain = true)
@Data
public class TestsBootstrapConfiguration {
  private boolean enabled;
  private boolean activeDefaults;
}
