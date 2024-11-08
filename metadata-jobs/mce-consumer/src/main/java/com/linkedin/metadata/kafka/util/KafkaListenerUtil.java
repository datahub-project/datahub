package com.linkedin.metadata.kafka.util;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.EventUtils;
import com.linkedin.metadata.dao.throttle.ThrottleControl;
import com.linkedin.metadata.dao.throttle.ThrottleSensor;
import com.linkedin.mxe.FailedMetadataChangeProposal;
import com.linkedin.mxe.MetadataChangeProposal;
import java.io.IOException;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;

@Slf4j
public class KafkaListenerUtil {

  private KafkaListenerUtil() {}

  public static void registerThrottle(
      ThrottleSensor kafkaThrottle,
      ConfigurationProvider provider,
      KafkaListenerEndpointRegistry registry,
      String mceConsumerGroupId) {
    if (kafkaThrottle != null
        && provider
            .getMetadataChangeProposal()
            .getThrottle()
            .getComponents()
            .getMceConsumer()
            .isEnabled()) {
      log.info("MCE Consumer Throttle Enabled");
      kafkaThrottle.addCallback(
          (throttleEvent) -> {
            Optional<MessageListenerContainer> container =
                Optional.ofNullable(registry.getListenerContainer(mceConsumerGroupId));
            if (container.isEmpty()) {
              log.warn(
                  "Expected container was missing: {} throttle is not possible.",
                  mceConsumerGroupId);
            } else {
              if (throttleEvent.isThrottled()) {
                container.ifPresent(MessageListenerContainer::pause);
                return ThrottleControl.builder()
                    // resume consumer after sleep
                    .callback(
                        (resumeEvent) -> container.ifPresent(MessageListenerContainer::resume))
                    .build();
              }
            }

            return ThrottleControl.NONE;
          });
    } else {
      log.info("MCE Consumer Throttle Disabled");
    }
  }

  public static void sendFailedMCP(
      @Nonnull MetadataChangeProposal event,
      @Nonnull Throwable throwable,
      String fmcpTopicName,
      Producer<String, IndexedRecord> kafkaProducer) {
    final FailedMetadataChangeProposal failedMetadataChangeProposal =
        createFailedMCPEvent(event, throwable);
    try {
      final GenericRecord genericFailedMCERecord =
          EventUtils.pegasusToAvroFailedMCP(failedMetadataChangeProposal);
      log.debug("Sending FailedMessages to topic - {}", fmcpTopicName);
      log.info(
          "Error while processing FMCP: FailedMetadataChangeProposal - {}",
          failedMetadataChangeProposal);
      kafkaProducer.send(new ProducerRecord<>(fmcpTopicName, genericFailedMCERecord));
    } catch (IOException e) {
      log.error(
          "Error while sending FailedMetadataChangeProposal: Exception  - {}, FailedMetadataChangeProposal - {}",
          e.getStackTrace(),
          failedMetadataChangeProposal);
    }
  }

  @Nonnull
  public static FailedMetadataChangeProposal createFailedMCPEvent(
      @Nonnull MetadataChangeProposal event, @Nonnull Throwable throwable) {
    final FailedMetadataChangeProposal fmcp = new FailedMetadataChangeProposal();
    fmcp.setError(ExceptionUtils.getStackTrace(throwable));
    fmcp.setMetadataChangeProposal(event);
    return fmcp;
  }
}
