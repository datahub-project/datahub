package com.linkedin.gms.factory.event;

import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

/**
 * True unless {@code postgres.pgQueue.store} selects a non-Postgres queue store. Gates the beans
 * that only make sense with the Postgres-backed queue (its datasource pool and store).
 */
public class PostgresQueueStoreCondition implements Condition {

  public static final String PROPERTY = "postgres.pgQueue.store";
  public static final String POSTGRES = "postgres";
  public static final String INMEMORY = "inmemory";

  @Override
  public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
    String store = context.getEnvironment().getProperty(PROPERTY, POSTGRES);
    return POSTGRES.equalsIgnoreCase(store);
  }
}
