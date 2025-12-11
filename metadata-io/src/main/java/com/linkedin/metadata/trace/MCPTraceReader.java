/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.trace;

import com.linkedin.metadata.EventUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.util.Pair;
import java.io.IOException;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.experimental.SuperBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;

@Getter
@SuperBuilder
public class MCPTraceReader extends KafkaTraceReader<MetadataChangeProposal> {
  @Nonnull private final String topicName;
  @Nullable private final String consumerGroupId;

  @Override
  public Optional<MetadataChangeProposal> read(@Nullable GenericRecord genericRecord) {
    try {
      return Optional.ofNullable(
          genericRecord == null ? null : EventUtils.avroToPegasusMCP(genericRecord));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected Optional<Pair<ConsumerRecord<String, GenericRecord>, SystemMetadata>>
      matchConsumerRecord(
          ConsumerRecord<String, GenericRecord> consumerRecord, String traceId, String aspectName) {
    return read(consumerRecord.value())
        .filter(
            event ->
                traceIdMatch(event.getSystemMetadata(), traceId)
                    && aspectName.equals(event.getAspectName()))
        .map(event -> Pair.of(consumerRecord, event.getSystemMetadata()));
  }
}
