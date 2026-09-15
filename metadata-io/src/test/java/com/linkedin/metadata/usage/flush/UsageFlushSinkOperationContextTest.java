package com.linkedin.metadata.usage.flush;

import com.linkedin.metadata.entity.OperationContextArchTestUtil;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Map;
import org.testng.annotations.Test;

/**
 * Architectural rule: every public method declared directly on {@link UsageFlushSink} must take
 * {@link OperationContext} as the first parameter, unless annotated with
 * {@code @OperationContextExempt}.
 *
 * <p>Guards the flush-context invariant: the context must be passed per call, never held by the
 * sink. Constructor-injecting {@code systemOperationContext} into a flush sink previously formed a
 * Spring bean cycle (systemOperationContext → usage store → flush-sink composer →
 * systemOperationContext) that failed GMS startup.
 *
 * <p>One test class per checked interface — see {@code EventProducerCorrectnessTest} for the
 * convention.
 */
public class UsageFlushSinkOperationContextTest {

  @Test
  public void usageFlushSinkPublicMethodsMustHaveOperationContextAsFirstParam() {
    OperationContextArchTestUtil.checkArch(UsageFlushSink.class, Map.of(0, OperationContext.class));
  }
}
