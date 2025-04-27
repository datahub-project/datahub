package com.linkedin.metadata.config.telemetry;

import lombok.Data;

/** POJO representing the "telemetry" configuration block in application.yaml. */
@Data
public class TelemetryConfiguration {
  /** Whether cli telemetry is enabled */
  public boolean enabledCli;

  /** Whether reporting telemetry is enabled */
  public boolean enabledIngestion;

  /** Whether or not third party logging should be enabled for this instance */
  public boolean enableThirdPartyLogging;

  /** Whether or not server telemetry should be enabled */
  public boolean enabledServer;

  // TODO Chakru: review if needed
  /** Whether or not user tracking is enabled in the product (using external tool like hotjar) */
  public boolean userTrackingEnabled;

  /** Configuration for Mixpanel. This is used to track user interactions with the product. */
  public MixpanelConfiguration mixpanel;
}
