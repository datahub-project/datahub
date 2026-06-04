package com.linkedin.gms.factory.system_telemetry;

import org.springframework.boot.actuate.autoconfigure.web.ManagementContextConfiguration;
import org.springframework.context.annotation.Import;

/**
 * Registers scrape auth on the dedicated management server (default Docker port 4319).
 */
@ManagementContextConfiguration
@Import(PrometheusScrapeAuthFilterConfigurationSupport.class)
public class PrometheusScrapeAuthManagementConfiguration {}
