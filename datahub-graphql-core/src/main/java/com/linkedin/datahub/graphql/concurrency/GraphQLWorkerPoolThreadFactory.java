package com.linkedin.datahub.graphql.concurrency;

import java.util.concurrent.ThreadFactory;

public class GraphQLWorkerPoolThreadFactory implements ThreadFactory {

  private static int threadInitNumber;

  private static synchronized int nextThreadNum() {
    return threadInitNumber++;
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
