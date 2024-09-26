package datahub.spark.conf;

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

  public KafkaDatahubEmitterConfig(KafkaEmitterConfig kafkaEmitterConfig) {
    this.kafkaEmitterConfig = kafkaEmitterConfig;
  }
}
