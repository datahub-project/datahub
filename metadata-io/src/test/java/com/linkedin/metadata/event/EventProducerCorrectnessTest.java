package com.linkedin.metadata.event;

import com.linkedin.metadata.entity.AspectDaoCorrectnessTest;
import com.linkedin.metadata.entity.OperationContextArchTestUtil;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Map;
import org.testng.annotations.Test;

/**
 * Architectural rule: every public method declared directly on {@link EventProducer} must have
 * {@link OperationContext} as the first parameter, unless annotated with
 * {@code @OperationContextExempt}.
 *
 * <p>One test class per checked interface — see {@link AspectDaoCorrectnessTest } for the
 * convention. Add a new test class (e.g. {@code MyInterfaceOperationContextTest}) when a new
 * interface needs the same contract; do not append additional {@code @Test} methods here for
 * unrelated interfaces.
 */
public class EventProducerCorrectnessTest {

  @Test
  public void eventProducerPublicMethodsMustHaveOperationContextAsFirstParam() {
    OperationContextArchTestUtil.checkArch(EventProducer.class, Map.of(0, OperationContext.class));
  }
}
