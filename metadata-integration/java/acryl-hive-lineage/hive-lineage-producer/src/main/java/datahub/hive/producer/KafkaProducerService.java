package datahub.hive.producer;

import datahub.hive.producer.HiveLineageLogger.KafkaProducerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.Future;

public class KafkaProducerService {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaProducerService.class);
    private final KafkaProducer<String, String> producer;
    private final KafkaProducerConfig kafkaProducerConfig;

    public KafkaProducerService(KafkaProducerConfig kafkaProducerConfig) {
        this.kafkaProducerConfig = kafkaProducerConfig;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProducerConfig.bootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        props.put(ProducerConfig.RETRIES_CONFIG, kafkaProducerConfig.retries());
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, kafkaProducerConfig.retryBackoffMs());
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, kafkaProducerConfig.enableIdempotence());

        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, kafkaProducerConfig.maxBlockMs());
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, kafkaProducerConfig.requestTimeoutMs());
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, kafkaProducerConfig.deliveryTimeoutMs());

        props.put("security.protocol", "SSL");
        props.put("ssl.truststore.location", kafkaProducerConfig.truststoreLocation());
        props.put("ssl.truststore.password", kafkaProducerConfig.truststorePassword());
        props.put("ssl.keystore.location", kafkaProducerConfig.keystoreLocation());
        props.put("ssl.keystore.password", kafkaProducerConfig.keystorePassword());

        this.producer = new KafkaProducer<>(props);
    }

    public void sendMessage(String key, String value) {
        long startTime = System.currentTimeMillis();
        ProducerRecord<String, String> record = new ProducerRecord<>(kafkaProducerConfig.kafkaTopic(), key, value);
        try {
            Future<RecordMetadata> future = producer.send(record);
            RecordMetadata metadata = future.get();
            long duration = TimeUtils.calculateDuration(startTime);
            LOG.info("Sent message with key: {}, value: {} to partition: {}, offset: {}. Time taken: {} ms", 
                    key, value, metadata.partition(), metadata.offset(), duration);
        } catch (Exception e) {
            long duration = TimeUtils.calculateDuration(startTime);
            LOG.warn("Failed to send message with key: {}, value: {}. Time taken before failure: {} ms", 
                    key, value, duration, e);
        }
    }

    public void close() {
        if (producer != null) {
            try {
                producer.close(Duration.ofMillis(kafkaProducerConfig.closeTimeoutMs()));
                LOG.info("Kafka producer closed successfully with timeout: {} ms", kafkaProducerConfig.closeTimeoutMs());
            } catch (Exception e) {
                LOG.warn("Error closing Kafka producer", e);
            }
        }
    }
}
