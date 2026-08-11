package com.linkedin.gms.factory.analytics;

import com.linkedin.metadata.spring.YamlPropertySourceFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;

@Slf4j
@Configuration
@PropertySource(
    name = "pgAnalyticsConfigOverride",
    value = "${DATAHUB_PGANALYTICS_CONFIG_FILE:classpath:/pganalytics-config.yaml}",
    ignoreResourceNotFound = true,
    factory = YamlPropertySourceFactory.class)
public class PgAnalyticsConfigOverlay {

  public static final String CONFIG_FILE_ENV = "DATAHUB_PGANALYTICS_CONFIG_FILE";

  public static void warnIfConfigFileMissingResourcePrefix(Environment environment) {
    String configFile = environment.getProperty(CONFIG_FILE_ENV);
    if (configFile == null || configFile.isBlank()) {
      return;
    }
    if (!configFile.startsWith("file:") && !configFile.startsWith("classpath:")) {
      log.warn(
          "{} should be a Spring resource URI (e.g. file:/etc/datahub/pganalytics.yaml); got '{}'."
              + " Prepend file: or classpath: so the PropertySource overlay can load it.",
          CONFIG_FILE_ENV,
          configFile);
    }
  }
}
