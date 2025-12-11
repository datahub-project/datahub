/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.gms.factory.settings;

import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.service.SettingsService;
import javax.annotation.Nonnull;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Configuration
public class SettingsServiceFactory {
  @Bean(name = "settingsService")
  @Scope("singleton")
  @Nonnull
  protected SettingsService getInstance(final SystemEntityClient entityClient) throws Exception {
    return new SettingsService(entityClient);
  }
}
