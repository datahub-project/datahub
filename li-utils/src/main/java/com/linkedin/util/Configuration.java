/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
