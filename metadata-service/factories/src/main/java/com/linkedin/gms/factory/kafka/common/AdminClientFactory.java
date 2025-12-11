/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.gms.factory.kafka.common;

import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;

public class AdminClientFactory {
  public static AdminClient buildKafkaAdminClient(
      KafkaConfiguration kafkaConfiguration,
      final KafkaProperties kafkaProperties,
      String clientId) {
    Map<String, Object> adminProperties = new HashMap<>(kafkaProperties.buildAdminProperties(null));
    adminProperties.put(AdminClientConfig.CLIENT_ID_CONFIG, clientId);

    // KAFKA_BOOTSTRAP_SERVER has precedence over SPRING_KAFKA_BOOTSTRAP_SERVERS
    if (kafkaConfiguration.getBootstrapServers() != null
        && !kafkaConfiguration.getBootstrapServers().isEmpty()) {
      adminProperties.put(
          AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
          Arrays.asList(kafkaConfiguration.getBootstrapServers().split(",")));
    } // else we rely on KafkaProperties which defaults to localhost:9092 or environment variables

    return KafkaAdminClient.create(adminProperties);
  }
}
