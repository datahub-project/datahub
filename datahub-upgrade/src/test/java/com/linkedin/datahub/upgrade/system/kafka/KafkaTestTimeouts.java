package com.linkedin.datahub.upgrade.system.kafka;

import java.util.Map;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;

/**
 * Applies short Kafka client timeouts for tests.
 *
 * <p>datahub-upgrade tests run without a Kafka broker, so any real client (admin, producer, or
 * consumer) built against {@code localhost:9092} is doomed. With Kafka's production defaults --
 * {@code request.timeout.ms=30000}, {@code socket.connection.setup.timeout.max.ms=30000}, {@code
 * default.api.timeout.ms=60000} -- a doomed client's background network thread retries for tens of
 * seconds. That is fast on a machine that refuses the port (localhost sends RST immediately) but a
 * multi-minute stall on a CI runner where the port black-holes packets, producing hundreds of
 * NetworkClient WARN lines and wasted wall time.
 *
 * <p>These short values live on {@link KafkaProperties#getProperties()}, the common map that {@code
 * buildAdminProperties()}, {@code buildProducerProperties()}, and {@code buildConsumerProperties()}
 * all merge in, so every client type built from these properties inherits them. The outcome is
 * identical to the defaults because there is no broker to reach -- just ~30-60x faster to fail.
 */
public final class KafkaTestTimeouts {
  private KafkaTestTimeouts() {}

  public static void applyShort(KafkaProperties kafkaProperties) {
    Map<String, String> props = kafkaProperties.getProperties();
    props.put("request.timeout.ms", "1000");
    props.put("default.api.timeout.ms", "2000");
    props.put("socket.connection.setup.timeout.ms", "500");
    props.put("socket.connection.setup.timeout.max.ms", "1000");
    props.put("reconnect.backoff.max.ms", "1000");
    props.put("retries", "1");
  }
}
