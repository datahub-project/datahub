/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.kafka.util;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.dao.throttle.ThrottleControl;
import com.linkedin.metadata.dao.throttle.ThrottleSensor;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
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
}
