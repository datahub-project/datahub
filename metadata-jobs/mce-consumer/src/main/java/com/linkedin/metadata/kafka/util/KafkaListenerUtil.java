package com.linkedin.metadata.kafka.util;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.dao.throttle.ThrottleControl;
import com.linkedin.metadata.dao.throttle.ThrottleSensor;
import com.linkedin.metadata.kafka.pause.ConsumerPauseSupport;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KafkaListenerUtil {

  private KafkaListenerUtil() {}

  public static void registerThrottle(
      ThrottleSensor kafkaThrottle,
      ConfigurationProvider provider,
      ConsumerPauseSupport pauseSupport,
      String listenerContainerId) {
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
            if (throttleEvent.isThrottled()) {
              pauseSupport.pause(listenerContainerId);
              return ThrottleControl.builder()
                  .callback((resumeEvent) -> pauseSupport.resume(listenerContainerId))
                  .build();
            }
            return ThrottleControl.NONE;
          });
    } else {
      log.info("MCE Consumer Throttle Disabled");
    }
  }
}
