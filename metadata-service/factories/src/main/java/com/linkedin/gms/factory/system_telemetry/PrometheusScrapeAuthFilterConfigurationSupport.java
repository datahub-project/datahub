package com.linkedin.gms.factory.system_telemetry;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.core.Ordered;

/** Shared filter beans for Prometheus scrape auth. */
@EnableConfigurationProperties(PrometheusScrapeAuthProperties.class)
class PrometheusScrapeAuthFilterConfigurationSupport {

  @Bean
  PrometheusScrapeAuthFilter prometheusScrapeAuthFilter(
      PrometheusScrapeAuthProperties authProperties,
      @Value("${management.metrics.export.prometheus.enabled:false}")
          boolean prometheusExportEnabled) {
    PrometheusScrapeAuthSettings settings =
        PrometheusScrapeAuthSettings.resolve(
            prometheusExportEnabled,
            authProperties.getEnabled(),
            authProperties.getUsername(),
            authProperties.getPassword());
    return new PrometheusScrapeAuthFilter(settings);
  }

  @Bean
  FilterRegistrationBean<PrometheusScrapeAuthFilter> prometheusScrapeAuthFilterRegistration(
      PrometheusScrapeAuthFilter filter) {
    FilterRegistrationBean<PrometheusScrapeAuthFilter> registration = new FilterRegistrationBean<>();
    registration.setFilter(filter);
    registration.addUrlPatterns(PrometheusScrapeAuthFilter.PROMETHEUS_ACTUATOR_PATH);
    registration.setOrder(Ordered.HIGHEST_PRECEDENCE);
    registration.setAsyncSupported(true);
    return registration;
  }

  private PrometheusScrapeAuthFilterConfigurationSupport() {}
}
