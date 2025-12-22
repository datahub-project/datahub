package com.linkedin.gms.factory.kafka.common;

import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.codehaus.plexus.util.StringUtils;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;

public class AdminClientFactory {
  public static AdminClient buildKafkaAdminClient(
      KafkaConfiguration kafkaConfiguration,
      final KafkaProperties kafkaProperties,
      String clientId) {
    Map<String, Object> adminProperties = new HashMap<>(kafkaProperties.buildAdminProperties(null));
    adminProperties.put(AdminClientConfig.CLIENT_ID_CONFIG, clientId);

    // We use the producer configuration because the admin client is used for checking availability
    // of the new system
    // and tracing events through the system. The more important side of this tracking during
    // temporary migration
    // stages is the end target system (producer side). This does temporarily limit some of the
    // capabilities until
    // they are merged back to a single config.
    String bootstrapServers =
        StringUtils.isNotBlank(kafkaConfiguration.getProducer().getBootstrapServers())
            ? kafkaConfiguration.getProducer().getBootstrapServers()
            : kafkaConfiguration.getBootstrapServers();

    // KAFKA_BOOTSTRAP_SERVER has precedence over SPRING_KAFKA_BOOTSTRAP_SERVERS
    if (StringUtils.isNotBlank(bootstrapServers)) {
      adminProperties.put(
          AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, Arrays.asList(bootstrapServers.split(",")));
    } // else we rely on KafkaProperties which defaults to localhost:9092 or environment variables

    return KafkaAdminClient.create(adminProperties);
  }
}
