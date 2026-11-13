package com.linkedin.gms.factory.kafka.common;

import com.linkedin.gms.factory.kafka.KafkaMskIamAuth;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.codehaus.plexus.util.StringUtils;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;

public class AdminClientFactory {
  /**
   * Builds an admin client against the producer-side cluster.
   *
   * <p>Under MSK IAM this needs the shared AWS credentials installed by {@code
   * defaultAwsCredentialsProvider}, so the calling Spring context must load {@code
   * AwsClientFactory} and create that bean first: either build the client after context refresh, or
   * annotate the calling configuration with {@code @Import(AwsClientFactory.class)} and
   * {@code @DependsOn("defaultAwsCredentialsProvider")}.
   */
  public static AdminClient buildKafkaAdminClient(
      KafkaConfiguration kafkaConfiguration,
      final KafkaProperties kafkaProperties,
      String clientId) {
    Map<String, Object> adminProperties = new HashMap<>(kafkaProperties.buildAdminProperties());
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

    // Pair the producer-side bootstrap above with the producer-side security protocol, as the
    // producer itself does; otherwise a split migration dials the producer listener with the base
    // protocol.
    String producerSecurityProtocol = kafkaConfiguration.getProducer().getSecurityProtocol();
    if (StringUtils.isNotBlank(producerSecurityProtocol)) {
      adminProperties.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, producerSecurityProtocol);
    }

    // Same MSK IAM handling as the producer and consumer factories: swap the stock
    // IAMClientCallbackHandler for DataHub's shared-credentials handler.
    KafkaMskIamAuth.configure(adminProperties);

    return KafkaAdminClient.create(adminProperties);
  }
}
