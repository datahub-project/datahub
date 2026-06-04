package com.linkedin.gms.factory.system_telemetry;

import org.springframework.boot.actuate.autoconfigure.web.server.ConditionalOnManagementPort;
import org.springframework.boot.actuate.autoconfigure.web.server.ManagementPortType;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * Registers scrape auth on the main web server when management shares the application port (e.g.
 * {@code MANAGEMENT_SERVER_PORT=8080}).
 */
@Configuration
@ConditionalOnManagementPort(ManagementPortType.SAME)
@Import(PrometheusScrapeAuthFilterConfigurationSupport.class)
public class PrometheusScrapeAuthFilterFactory {}
