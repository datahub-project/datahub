package com.linkedin.gms.factory.timeseries;

import com.linkedin.metadata.spring.YamlPropertySourceFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;

/**
 * Loads the optional pgTimeseries multi-store override file (Tier 2). Defaults live in {@code
 * application.yaml}. Declared as its own {@code @Configuration} so GMS and datahub-upgrade both
 * pick it up wherever timeseries factories are scanned.
 *
 * <p>{@code DATAHUB_PGTIMESERIES_CONFIG_FILE} is a Spring resource URI (e.g. {@code
 * file:/etc/datahub/pgtimeseries.yaml}); when unset it resolves to the bundled empty {@code
 * pgtimeseries-config.yaml}. {@code ignoreResourceNotFound} tolerates a set-but-missing path.
 */
@Slf4j
@Configuration
@PropertySource(
    name = "pgTimeseriesConfigOverride",
    value = "${DATAHUB_PGTIMESERIES_CONFIG_FILE:classpath:/pgtimeseries-config.yaml}",
    ignoreResourceNotFound = true,
    factory = YamlPropertySourceFactory.class)
public class PgTimeseriesConfigOverlay {

  public static final String CONFIG_FILE_ENV = "DATAHUB_PGTIMESERIES_CONFIG_FILE";

  /** Warn when the override URI lacks a Spring resource prefix (same guidance as rate limits). */
  public static void warnIfConfigFileMissingResourcePrefix(Environment environment) {
    String configFile = environment.getProperty(CONFIG_FILE_ENV);
    if (configFile == null || configFile.isBlank()) {
      return;
    }
    if (!configFile.startsWith("file:") && !configFile.startsWith("classpath:")) {
      log.warn(
          "{} should be a Spring resource URI (e.g. file:/etc/datahub/pgtimeseries.yaml); got '{}'."
              + " Prepend file: or classpath: so the PropertySource overlay can load it.",
          CONFIG_FILE_ENV,
          configFile);
    }
  }
}
