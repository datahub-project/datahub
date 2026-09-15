package com.linkedin.metadata.usage;

import io.datahubproject.metadata.context.OperationContext;
import java.util.Map;
import javax.annotation.Nonnull;

/**
 * Extension point for contributing extra usage dimensions resolved from the request {@link
 * OperationContext} at record time. OSS ships no implementations, so usage aggregation stays
 * dimension-neutral; downstream distributions register beans to make usage rows context-aware
 * without re-threading the record path.
 *
 * <p>Only invoked on request-driven record paths that carry a real context — not the scheduled
 * flush, which runs under the system context with no per-request identity.
 */
public interface UsageDimensionResolver {

  /** Extra dimensions to merge into the usage row for this context; empty for none. */
  @Nonnull
  Map<String, String> resolve(@Nonnull OperationContext opContext);
}
