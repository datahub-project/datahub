package com.linkedin.metadata.config.messaging;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.context.annotation.Conditional;

/**
 * Beans that consume or listen on Kafka metadata topics should be registered only when {@link
 * MessagingTransport#PROPERTY} is {@code kafka}. Non-Kafka transports use {@link
 * KafkaMessagingDisabled} for complementary beans (for example a no-op {@code
 * dataHubUpgradeKafkaListener}).
 */
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Conditional(KafkaMessagingEnabledCondition.class)
public @interface KafkaMessagingEnabled {}
