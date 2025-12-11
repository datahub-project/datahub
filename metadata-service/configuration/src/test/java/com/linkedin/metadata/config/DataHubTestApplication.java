/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.config;

import com.linkedin.metadata.spring.YamlPropertySourceFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.PropertySource;

@SpringBootApplication
@PropertySource(value = "classpath:/application.yaml", factory = YamlPropertySourceFactory.class)
public class DataHubTestApplication {
  @Autowired private DataHubAppConfiguration dataHubAppConfig;

  public DataHubAppConfiguration getDataHubAppConfig() {
    return dataHubAppConfig;
  }
}
