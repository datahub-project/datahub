package datahub.spark.conf;

import datahub.client.kafka.KafkaEmitter;
import datahub.client.kafka.KafkaEmitterConfig;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Setter
@ToString
@Getter
public class KafkaDatahubEmitterConfig implements DatahubEmitterConfig {
  final String type = "kafka";
  KafkaEmitterConfig kafkaEmitterConfig;
  String mcpTopic;

  public KafkaDatahubEmitterConfig(KafkaEmitterConfig kafkaEmitterConfig) {
    this.kafkaEmitterConfig = kafkaEmitterConfig;
    this.mcpTopic = KafkaEmitter.DEFAULT_MCP_KAFKA_TOPIC;
  }

  public KafkaDatahubEmitterConfig(KafkaEmitterConfig kafkaEmitterConfig, String mcpTopic) {
    this.kafkaEmitterConfig = kafkaEmitterConfig;
    this.mcpTopic = mcpTopic;
  }
}
