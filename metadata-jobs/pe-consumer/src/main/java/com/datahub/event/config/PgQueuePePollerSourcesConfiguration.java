package com.datahub.event.config;

import com.datahub.event.PlatformEventProcessor;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.PeConsumerConfiguration;
import com.linkedin.metadata.config.pgqueue.PgQueueConsumerPollSettings;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.kafka.InboundMetadataEnvelope;
import com.linkedin.metadata.pgqueue.PgQueuePollerRegistration;
import com.linkedin.metadata.pgqueue.PgQueuePollerSource;
import com.linkedin.metadata.queue.QueueReceivedMessage;
import com.linkedin.mxe.Topics;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class PgQueuePePollerSourcesConfiguration {

  private final ConfigurationProvider configurationProvider;
  private final PostgresSqlSetupProperties postgresSqlSetupProperties;

  @Bean
  @Conditional(PgQueueMessagingAndPlatformEventProcessorCondition.class)
  public PgQueuePollerSource pgQueuePeSource(
      PlatformEventProcessor processor,
      @Value("${PLATFORM_EVENT_TOPIC_NAME:" + Topics.PLATFORM_EVENT + "}") String topic,
      @Value(PlatformEventProcessor.DATAHUB_PLATFORM_EVENT_CONSUMER_GROUP_VALUE) String groupId) {
    PgQueueConsumerPollSettings.SleepMillis sleep =
        PgQueueConsumerPollSettings.requireSleep(postgresSqlSetupProperties);
    return () ->
        Stream.of(
            new PgQueuePollerRegistration(
                groupId,
                List.of(topic),
                platformEventPollMaxBatch(),
                "pgqueue-pe-" + groupId,
                sleep.emptyPoll(),
                sleep.missingTopic(),
                sleep.errorRecovery(),
                (logicalTopic, batch, ctx) -> {
                  for (QueueReceivedMessage msg : batch) {
                    try {
                      GenericRecord record = ctx.decodeAvro(msg, logicalTopic);
                      InboundMetadataEnvelope<GenericRecord> envelope =
                          InboundMetadataEnvelope.fromPgQueue(msg, logicalTopic, groupId, record);
                      processor.consumeEnvelope(envelope);
                      ctx.commit(List.of(msg.handle()));
                    } catch (Exception e) {
                      log.error(
                          "Platform event pgQueue message failed; lease will expire for retry", e);
                    }
                  }
                }));
  }

  int platformEventPollMaxBatch() {
    return PgQueueConsumerPollSettings.requirePollMaxBatch(
        Optional.ofNullable(configurationProvider.getPeConsumer())
            .map(PeConsumerConfiguration::getPgQueue)
            .map(PeConsumerConfiguration.PgQueuePoll::getPlatformEventMaxBatch)
            .orElse(null),
        "peConsumer.pgQueue.platformEventMaxBatch");
  }
}
