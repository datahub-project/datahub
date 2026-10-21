package com.linkedin.metadata.config.search;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;
import lombok.Data;

/** Worker-pool sizing for parallel Search V3 template ladder legs. */
@Data
public class SearchTemplateConcurrencyConfiguration {
  private int corePoolSize = -1;
  private int maxPoolSize = 0;
  private int queueSize = 0;
  private int keepAlive = 60;

  public int resolveCorePoolSize() {
    return corePoolSize < 0 ? Runtime.getRuntime().availableProcessors() : corePoolSize;
  }

  public int resolveMaxPoolSize() {
    return maxPoolSize <= 0 ? Runtime.getRuntime().availableProcessors() * 4 : maxPoolSize;
  }

  public BlockingQueue<Runnable> createWorkQueue() {
    return queueSize <= 0 ? new SynchronousQueue<>() : new ArrayBlockingQueue<>(queueSize);
  }
}
