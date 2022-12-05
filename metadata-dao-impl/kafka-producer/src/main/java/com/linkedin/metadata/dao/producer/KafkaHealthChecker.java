package com.linkedin.metadata.dao.producer;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.Set;

@Slf4j
@EnableScheduling
@Component
public class KafkaHealthChecker {

    @Value("${kafka.producer.requestTimeout}")
    private long kafkaProducerRequestTimeout;

    @Value("${kafka.producer.backoffTimeout}")
    private long kafkaProducerBackOffTimeout;

    private static long lastMoment = 0L;
    private Set<Long> messagesInProgress = new HashSet<>();

    private synchronized long getNextUniqueMoment() {
        long moment = System.currentTimeMillis();
        lastMoment = moment != lastMoment ? moment : ++lastMoment;
        return lastMoment;
    }

    public Callback getKafkaCallBack(String eventType, String entityDesc) {
        long moment = getNextUniqueMoment();
        sendMessageStarted(moment);
        return (metadata, e) -> {
            sendMessageEnded(moment);
            if (e != null) {
                log.error(String.format("Failed to emit %s for entity %s", eventType, entityDesc), e);
                MetricUtils.counter(this.getClass(),
                        MetricRegistry.name("producer_failed_count", eventType.replaceAll(" ", "_"))).inc();
            } else {
                log.debug(String.format(
                        "Successfully emitted %s for entity %s at offset %s, partition %s, topic %s",
                        eventType, entityDesc, metadata.offset(), metadata.partition(), metadata.topic()));
            }
        };
    }

    private synchronized void sendMessageStarted(long uniqueMessageId) {
        messagesInProgress.add(uniqueMessageId);
    }

    private synchronized void sendMessageEnded(long uniqueMessageId) {
        messagesInProgress.remove(uniqueMessageId);
    }

    @Scheduled(cron = "0/15 * * * * ?")
    private synchronized void periodicKafkaHealthChecker() {
        long moment = System.currentTimeMillis();
        long count = messagesInProgress.stream()
                .filter(item -> item + kafkaProducerRequestTimeout + kafkaProducerBackOffTimeout < moment)
                .count();
        if (count > 0) {
            log.error("Kafka Health Check Failed. %d message(s) is(are) waiting to be sent.", count);
        }
    }

}
