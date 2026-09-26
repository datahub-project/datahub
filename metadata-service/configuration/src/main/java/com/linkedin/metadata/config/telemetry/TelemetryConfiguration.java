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

  /** Per-request actor/operation attribution on OpenTelemetry spans. Off by default. */
  /**
   * Continue an inbound W3C trace ({@code traceparent}/{@code tracestate} headers) in GMS's own
   * OpenTelemetry SDK, so a trace id minted by the ingress or load balancer is the id on GMS spans.
   * Off by default: GMS then starts a fresh trace per request, as it always has. The OpenTelemetry
   * Java agent does this on its own when deployed; this flag is for installs without the agent.
   */
  private boolean traceContinuation = false;

  private RequestAttributionConfiguration requestAttribution =
      new RequestAttributionConfiguration();
}
