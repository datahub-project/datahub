package com.linkedin.metadata.config.graphql;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;
import lombok.Data;

/**
 * GraphQL worker-pool sizing. Numeric defaults live in {@code application.yaml} under {@code
 * graphQL.concurrency} (env {@code GRAPHQL_CONCURRENCY_*}).
 *
 * <p>Resolvers submitted via {@code GraphQLConcurrencyUtils.supplyAsync} are typically blocking I/O
 * with wide fan-out, so concurrency is not CPU-linear. A small bounded queue does not substitute
 * for in-flight threads: work sits idle until a worker frees, then {@code CallerRunsPolicy} runs on
 * the Jetty request thread and can starve nested GraphQL. Bounded queues remain available by
 * setting {@code queueSize > 0}.
 *
 * <p>Legacy node-scaled behavior ({@code availableProcessors() * 5 / * 100} and a {@link
 * SynchronousQueue}) is restored with {@code GRAPHQL_CONCURRENCY_SCALE_WITH_PROCESSORS=true}, or by
 * sentinel values: {@code corePoolSize < 0}, {@code maxPoolSize <= 0}, {@code queueSize <= 0}.
 */
@Data
public class GraphQLConcurrencyConfiguration {
  boolean separateThreadPool;
  boolean scaleWithProcessors;
  long stackSize;
  int corePoolSize;
  int maxPoolSize;
  int keepAlive;
  int queueSize;

  public int resolveCorePoolSize() {
    if (scaleWithProcessors || corePoolSize < 0) {
      return Runtime.getRuntime().availableProcessors() * 5;
    }
    return corePoolSize;
  }

  public int resolveMaxPoolSize() {
    if (scaleWithProcessors || maxPoolSize <= 0) {
      return Runtime.getRuntime().availableProcessors() * 100;
    }
    return maxPoolSize;
  }

  public boolean useSynchronousQueue() {
    return scaleWithProcessors || queueSize <= 0;
  }

  public BlockingQueue<Runnable> createWorkQueue() {
    if (useSynchronousQueue()) {
      return new SynchronousQueue<>();
    }
    return new ArrayBlockingQueue<>(queueSize);
  }
}
