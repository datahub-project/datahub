package com.linkedin.metadata.config.telemetry;

import lombok.Data;

@Data
public class MixpanelConfiguration {
  public boolean enabled; // Whether Mixpanel is enabled
  public String token; // Token for mixpanel
}
