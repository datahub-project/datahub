package com.linkedin.metadata.config.telemetry;

import lombok.Data;

@Data
public class MixpanelConfiguration {

  public boolean enabled;
  public String token;
  public boolean useStandardEndpoints;
  public boolean disableObfuscation;
}
