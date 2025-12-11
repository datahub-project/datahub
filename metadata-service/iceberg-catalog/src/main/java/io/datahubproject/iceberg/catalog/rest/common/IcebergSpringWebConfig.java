/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package io.datahubproject.iceberg.catalog.rest.common;

import io.datahubproject.iceberg.catalog.credentials.CachingCredentialProvider;
import io.datahubproject.iceberg.catalog.credentials.CredentialProvider;
import io.datahubproject.iceberg.catalog.credentials.S3CredentialProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.converter.*;

@Configuration
public class IcebergSpringWebConfig {

  @Bean
  public CredentialProvider credentialProvider() {
    return new S3CredentialProvider();
  }

  @Bean
  public CredentialProvider cachingCredentialProvider(CredentialProvider credentialProvider) {
    return new CachingCredentialProvider(credentialProvider);
  }
}
