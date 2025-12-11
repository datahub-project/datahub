/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
