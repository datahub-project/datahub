package com.linkedin.metadata.config.messaging;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.context.annotation.Conditional;

/**
 * Beans that exist only when metadata messaging is not {@link MessagingTransport#KAFKA} (for
 * example {@link MessagingTransport#PGQUEUE}) should use this annotation alongside beans guarded by
 * {@link KafkaMessagingEnabled}.
 */
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Conditional(KafkaMessagingDisabledCondition.class)
public @interface KafkaMessagingDisabled {}
