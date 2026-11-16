package com.linkedin.gms.factory.event;

import com.linkedin.metadata.config.messaging.KafkaMessagingDisabledCondition;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties.PgQueue;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

/**
 * Validates {@code postgres.pgQueue.store} before bean creation and selects the Postgres-backed
 * queue's datasource pool and store.
 */
public class PostgresQueueStoreCondition implements Condition {

  public static final String PROPERTY = "postgres.pgQueue.store";
  public static final String POSTGRES = "postgres";
  public static final String INMEMORY = "inmemory";

  @Override
  public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
    return POSTGRES.equals(configuredStore(context, metadata));
  }

  private static String configuredStore(ConditionContext context, AnnotatedTypeMetadata metadata) {
    // Conditions may run in either order; unused queue settings must not affect Kafka startup.
    if (!new KafkaMessagingDisabledCondition().matches(context, metadata)) {
      return null;
    }
    return PgQueue.normalizeStore(context.getEnvironment().getProperty(PROPERTY));
  }

  /** Uses the same early validation even in contexts that wire only the in-memory factory. */
  public static class InMemory implements Condition {
    @Override
    public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
      return INMEMORY.equals(configuredStore(context, metadata));
    }
  }
}
