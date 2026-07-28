package com.linkedin.metadata.config.buffer;

import lombok.Data;

/**
 * POJO representing the "datahub.buffer" configuration block in application.yaml: selects the
 * backend for {@code CoalesceBuffer<K,V>} (e.g. the post-commit retention buffer). Values:
 * "caffeine" (default, local-only) or "hazelcast" (cross-pod coalescing).
 */
@Data
public class BufferConfiguration {
  private String implementation = "caffeine";
}
