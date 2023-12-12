package com.linkedin.metadata.dao.producer;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@EnableScheduling
@Component
public class KafkaHealthChecker {

  @Value("${kafka.producer.deliveryTimeout}")
  private long kafkaProducerDeliveryTimeout;

  private final Set<MessageLog> messagesInProgress = ConcurrentHashMap.newKeySet();

  public Callback getKafkaCallBack(String eventType, String entityDesc) {
    final MessageLog tracking = MessageLog.track(entityDesc, kafkaProducerDeliveryTimeout);
    sendMessageStarted(tracking);
    return (metadata, e) -> {
      sendMessageEnded(tracking);
      if (e != null) {
        log.error(String.format("Failed to emit %s for entity %s", eventType, entityDesc), e);
        MetricUtils.counter(
                this.getClass(),
                MetricRegistry.name("producer_failed_count", eventType.replaceAll(" ", "_")))
            .inc();
      } else {
        log.debug(
            String.format(
                "Successfully emitted %s for entity %s at offset %s, partition %s, topic %s",
                eventType, entityDesc, metadata.offset(), metadata.partition(), metadata.topic()));
      }
    };
  }

  private void sendMessageStarted(MessageLog messageLog) {
    messagesInProgress.add(messageLog);
  }

  private void sendMessageEnded(MessageLog messageLog) {
    messagesInProgress.remove(messageLog);
  }

  @Scheduled(cron = "0/60 * * * * ?")
  private synchronized void periodicKafkaHealthChecker() {
    long moment = System.currentTimeMillis();
    Set<MessageLog> oldItems =
        messagesInProgress.stream()
            .filter(item -> item.expectedMilli < moment)
            .collect(Collectors.toSet());

    if (oldItems.size() > 0) {
      Map<String, Long> itemCounts =
          oldItems.stream()
              .collect(Collectors.groupingBy(MessageLog::getEntityDesc, Collectors.counting()));
      log.error(
          String.format(
              "Kafka Health Check Failed. Old message(s) were waiting to be sent: %s", itemCounts));
      messagesInProgress.removeAll(oldItems);
    }
  }

  @Getter
  static class MessageLog {
    private final String entityDesc;
    private final long uniqueMessageId;
    private final long expectedMilli;
    private static long lastMoment = 0L;

    public static MessageLog track(String entityDesc, long maxDelayMilli) {
      return new MessageLog(entityDesc, maxDelayMilli);
    }

    private MessageLog(String entityDesc, long maxDelayMilli) {
      this.entityDesc = entityDesc;
      this.uniqueMessageId = getNextUniqueMoment();
      this.expectedMilli = this.uniqueMessageId + maxDelayMilli;
    }

    private synchronized long getNextUniqueMoment() {
      long moment = System.currentTimeMillis();
      lastMoment = moment != lastMoment ? moment : ++lastMoment;
      return lastMoment;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      MessageLog that = (MessageLog) o;

      if (uniqueMessageId != that.uniqueMessageId) {
        return false;
      }
      return entityDesc.equals(that.entityDesc);
    }

    @Override
    public int hashCode() {
      int result = entityDesc.hashCode();
      result = 31 * result + (int) (uniqueMessageId ^ (uniqueMessageId >>> 32));
      return result;
    }
  }
}
