package com.linkedin.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.Properties;
import javax.annotation.Nonnull;

public class Configuration {

  private Configuration() {}

  @Nonnull
  public static Properties loadProperties(@Nonnull String configFile) {
    Properties configuration = new Properties();
    try (InputStream inputStream =
        Configuration.class.getClassLoader().getResourceAsStream(configFile)) {
      configuration.load(inputStream);
    } catch (IOException e) {
      throw new RuntimeException("Can't read file: " + configFile);
    }
    return configuration;
  }

  @Nonnull
  public static String getEnvironmentVariable(@Nonnull String envVar) {
    return System.getenv(envVar);
  }

  @Nonnull
  public static String getEnvironmentVariable(@Nonnull String envVar, @Nonnull String defaultVal) {
    return Optional.ofNullable(System.getenv(envVar)).orElse(defaultVal);
  }
}
