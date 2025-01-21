package com.linkedin.metadata.trace;

import com.linkedin.metadata.EventUtils;
import com.linkedin.mxe.MetadataChangeLog;
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
public class MCLTraceReader extends KafkaTraceReader<MetadataChangeLog> {
  @Nonnull private final String topicName;
  @Nullable private final String consumerGroupId;

  @Override
  public Optional<MetadataChangeLog> read(@Nullable GenericRecord genericRecord) {
    try {
      return Optional.ofNullable(
          genericRecord == null ? null : EventUtils.avroToPegasusMCL(genericRecord));
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
