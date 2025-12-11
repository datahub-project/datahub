/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
