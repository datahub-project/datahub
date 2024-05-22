package com.linkedin.datahub.graphql.concurrency;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

public class GraphQLWorkerPoolThreadFactory implements ThreadFactory {

  private static final AtomicLong THREAD_INIT_NUMBER = new AtomicLong();

  private static long nextThreadNum() {
    return THREAD_INIT_NUMBER.getAndIncrement();
  }

  private long stackSize;

  public GraphQLWorkerPoolThreadFactory(long stackSize) {
    this.stackSize = stackSize;
  }

  @Override
  public final Thread newThread(Runnable runnable) {
    Thread thread = new Thread(null, runnable, "GraphQLWorkerThread-" + nextThreadNum(), stackSize);

    return thread;
  }
}
