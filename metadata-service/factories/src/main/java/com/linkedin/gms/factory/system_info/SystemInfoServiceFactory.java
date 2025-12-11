/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.gms.factory.system_info;

import com.linkedin.metadata.system_info.SystemInfoService;
import com.linkedin.metadata.system_info.collectors.PropertiesCollector;
import com.linkedin.metadata.system_info.collectors.SpringComponentsCollector;
import com.linkedin.metadata.version.GitVersion;
import javax.annotation.Nonnull;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.core.env.Environment;

@Configuration
public class SystemInfoServiceFactory {

  @Bean(name = "propertiesCollector")
  @Scope("singleton")
  @Nonnull
  protected PropertiesCollector createPropertiesCollector(final Environment springEnvironment) {
    return new PropertiesCollector(springEnvironment);
  }

  @Bean(name = "springComponentsCollector")
  @Scope("singleton")
  @Nonnull
  protected SpringComponentsCollector createSpringComponentsCollector(
      final GitVersion gitVersion, final PropertiesCollector propertiesCollector) {
    return new SpringComponentsCollector(gitVersion, propertiesCollector);
  }

  @Bean(name = "systemInfoService")
  @Scope("singleton")
  @Nonnull
  protected SystemInfoService createSystemInfoService(
      final SpringComponentsCollector springComponentsCollector,
      final PropertiesCollector propertiesCollector) {
    return new SystemInfoService(springComponentsCollector, propertiesCollector);
  }
}
