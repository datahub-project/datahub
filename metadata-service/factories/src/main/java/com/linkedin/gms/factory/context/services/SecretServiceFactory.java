/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.gms.factory.context.services;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import io.datahubproject.metadata.services.SecretService;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
public class SecretServiceFactory {

  @Value("${secretService.encryptionKey}")
  private String encryptionKey;

  @Bean(name = "dataHubSecretService")
  @Primary
  @Nonnull
  protected SecretService getInstance(final ConfigurationProvider configurationProvider) {
    return new SecretService(
        this.encryptionKey, configurationProvider.getSecretService().isV1AlgorithmEnabled());
  }
}
