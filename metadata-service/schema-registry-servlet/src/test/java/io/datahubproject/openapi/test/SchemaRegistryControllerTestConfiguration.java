/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package io.datahubproject.openapi.test;

import com.linkedin.metadata.dao.producer.KafkaHealthChecker;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.TestPropertySource;

@TestConfiguration
@TestPropertySource(value = "classpath:/application.properties")
@ComponentScan(basePackages = {"com.linkedin.gms.factory.kafka", "com.linkedin.gms.factory.config"})
public class SchemaRegistryControllerTestConfiguration {
  @MockBean KafkaHealthChecker kafkaHealthChecker;

  @MockBean EntityRegistry entityRegistry;

  @MockBean MetricUtils metricUtils;

  @Bean(name = "systemOperationContext")
  public OperationContext systemOperationContext() {
    return TestOperationContexts.systemContextNoSearchAuthorization();
  }
}
