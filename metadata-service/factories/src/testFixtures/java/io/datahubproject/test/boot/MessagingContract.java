package io.datahubproject.test.boot;

import io.datahubproject.test.boot.ProcessBoot.Messaging;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import javax.annotation.Nonnull;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.util.ClassUtils;

/**
 * Transport-dependent part of every process contract: which event/usage producers are wired, and,
 * with Kafka disabled, that nothing Kafka is.
 */
public final class MessagingContract {

  private MessagingContract() {}

  /** Kafka clients and DataHub's Kafka producer: never acceptable with Kafka disabled. */
  private static final List<String> KAFKA_CLIENT_TYPES =
      List.of(
          "org.apache.kafka.clients.producer.Producer",
          "org.apache.kafka.clients.consumer.Consumer",
          "org.apache.kafka.clients.admin.Admin",
          "org.springframework.kafka.listener.MessageListenerContainer",
          "com.linkedin.metadata.dao.producer.KafkaEventProducer",
          "com.linkedin.metadata.dao.producer.KafkaUsageEventPublisher",
          "com.linkedin.metadata.dao.producer.GenericProducerImpl");

  /**
   * Kafka client factories / templates. They open no connection until used, but they should not be
   * defined with Kafka disabled either. Existing ones are listed per process as a baseline that may
   * only shrink: a new one fails the test, and removing one fails it until the baseline entry is
   * deleted too.
   */
  private static final List<String> KAFKA_INFRASTRUCTURE_TYPES =
      List.of(
          "org.springframework.kafka.core.ConsumerFactory",
          "org.springframework.kafka.core.ProducerFactory",
          "org.springframework.kafka.config.KafkaListenerContainerFactory",
          "org.springframework.kafka.core.KafkaOperations",
          "org.springframework.kafka.core.KafkaAdmin");

  /**
   * @param kafkaDisabledBaseline bean names of {@link #KAFKA_INFRASTRUCTURE_TYPES} beans that
   *     currently exist in this process with Kafka disabled. Ignored for {@link Messaging#KAFKA}.
   */
  public static BootContract apply(
      @Nonnull BootContract contract,
      @Nonnull Messaging messaging,
      @Nonnull Set<String> kafkaDisabledBaseline) {
    // The stubs must have taken effect, or the boot talked to real infrastructure.
    contract.check(
        (ctx, v) -> {
          if (!ctx.containsBean("searchClientShims")
              || !ExternalBoundaryStubs.isStub(ctx.getBean("searchClientShims"))) {
            v.add("searchClientShims is not the boot-test stub (bean renamed?)");
          }
        });
    if (messaging == Messaging.KAFKA) {
      return contract
          .check(
              (ctx, v) -> {
                for (String name : List.of("kafkaProducer", "dataHubUsageProducer")) {
                  if (ctx.containsBean(name) && !ExternalBoundaryStubs.isStub(ctx.getBean(name))) {
                    v.add(name + " is a real Kafka producer, not the boot-test stub");
                  }
                }
              })
          .requiresBean(
              "kafkaEventProducer", type("com.linkedin.metadata.dao.producer.KafkaEventProducer"))
          .requiresBean(
              "dataHubUsageGenericProducer",
              type("com.linkedin.metadata.dao.producer.GenericProducerImpl"))
          .requiresBean(
              "usageEventPublisher",
              type("com.linkedin.metadata.dao.producer.KafkaUsageEventPublisher"))
          .requiresBeans("kafkaProducer", "dataHubUsageProducer")
          .forbidsTypeNamed(
              "com.linkedin.metadata.queue.MetadataQueueStore",
              "com.linkedin.metadata.event.PgQueueEventProducer",
              "com.linkedin.metadata.event.PgQueueGenericProducer")
          .forbidsBeans("pgQueueEbeanServer");
    }
    return contract
        .requiresBean(
            "kafkaEventProducer", type("com.linkedin.metadata.event.PgQueueEventProducer"))
        // OTEL usage/audit export needs this bean under every transport (it was Kafka-only once).
        .requiresBean(
            "dataHubUsageGenericProducer",
            type("com.linkedin.metadata.event.PgQueueGenericProducer"))
        .requiresBean(
            "usageEventPublisher", type("com.linkedin.metadata.event.PgQueueUsageEventPublisher"))
        .requiresTypeNamed("com.linkedin.metadata.queue.MetadataQueueStore")
        .requiresBeans("pgQueueEbeanServer")
        .forbidsTypeNamed(KAFKA_CLIENT_TYPES.toArray(new String[0]))
        .forbidsBeans("kafkaProducer", "dataHubUsageProducer")
        .check(
            (ctx, v) -> {
              for (KafkaListenerEndpointRegistry registry :
                  ctx.getBeansOfType(KafkaListenerEndpointRegistry.class).values()) {
                if (!registry.getListenerContainerIds().isEmpty()) {
                  v.add(
                      "Kafka listener containers registered with Kafka disabled: "
                          + registry.getListenerContainerIds());
                }
              }
            })
        .check((ctx, v) -> ratchet(ctx, v, kafkaDisabledBaseline));
  }

  private static void ratchet(ApplicationContext ctx, List<String> v, Set<String> baseline) {
    Set<String> present = new TreeSet<>();
    for (String typeName : KAFKA_INFRASTRUCTURE_TYPES) {
      Class<?> type = resolve(ctx, typeName);
      if (type != null) {
        present.addAll(Arrays.asList(ctx.getBeanNamesForType(type, true, false)));
      }
    }
    Set<String> added = new TreeSet<>(present);
    added.removeAll(baseline);
    Set<String> removed = new TreeSet<>(baseline);
    removed.removeAll(present);
    if (!added.isEmpty()) {
      v.add("new Kafka client factories defined with Kafka disabled: " + added);
    }
    if (!removed.isEmpty()) {
      v.add(
          "Kafka-disabled baseline is stale, these beans are gone (good) -- delete them from the"
              + " baseline: "
              + removed);
    }
  }

  private static Class<?> type(String name) {
    try {
      return ClassUtils.forName(name, MessagingContract.class.getClassLoader());
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException(name + " is not on the boot-test classpath", e);
    }
  }

  private static Class<?> resolve(ApplicationContext ctx, String name) {
    try {
      return ClassUtils.forName(name, ctx.getClassLoader());
    } catch (ClassNotFoundException | LinkageError e) {
      return null;
    }
  }
}
