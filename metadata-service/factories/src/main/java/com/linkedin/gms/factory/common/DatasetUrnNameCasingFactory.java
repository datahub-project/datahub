/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.gms.factory.common;

import javax.annotation.Nonnull;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DatasetUrnNameCasingFactory {
  @Nonnull
  @Bean(name = "datasetUrnNameCasing")
  protected Boolean getInstance() {
    String datasetUrnNameCasingEnv = System.getenv("DATAHUB_DATASET_URN_TO_LOWER");
    return Boolean.parseBoolean(datasetUrnNameCasingEnv);
  }
}
