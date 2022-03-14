package com.linkedin.gms.factory.config;

import lombok.Data;
/**
 * POJO representing the "telemtry" configuration block in application.yml.
 */
@Data
public class TelemetryConfiguration {
    /**
    * Whether cli telemtry is enabled
    */
    public boolean enabledCli;
    /**
    * Whether reporting telemetry is enabled
    */
    public boolean enabledIngestion;
}