package com.linkedin.datahub.graphql.concurrency;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

public class GraphQLWorkerPoolThreadFactory implements ThreadFactory {

  private static final AtomicLong THREAD_INIT_NUMBER = new AtomicLong();
  public static final String GRAPHQL_THREAD_POOL_GROUP_NAME = "graphQLThreadGroup";
  public static final ThreadGroup GRAPHQL_THREAD_POOL_GROUP =
      new ThreadGroup(GRAPHQL_THREAD_POOL_GROUP_NAME);

  private static long nextThreadNum() {
    return THREAD_INIT_NUMBER.getAndIncrement();
  }

  private long stackSize;

  public GraphQLWorkerPoolThreadFactory(long stackSize) {
    this.stackSize = stackSize;
  }

  @Override
  public final Thread newThread(Runnable runnable) {

    return new Thread(
        GRAPHQL_THREAD_POOL_GROUP, runnable, "GraphQLWorkerThread-" + nextThreadNum(), stackSize);
  }
}
