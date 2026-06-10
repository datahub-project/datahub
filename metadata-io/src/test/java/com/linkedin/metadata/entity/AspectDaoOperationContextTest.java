package com.linkedin.metadata.entity;

import com.linkedin.metadata.event.EventProducer;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Map;
import org.testng.annotations.Test;

/**
 * Architectural rules: every public method declared directly on the listed interfaces / classes
 * must have {@link OperationContext} as the first parameter, unless annotated with {@link
 * OperationContextExempt}.
 *
 * <p>Add a new {@code @Test} per target class via {@link OperationContextArchTestUtil#checkArch}.
 * The {@code requiredAtIndex} map encodes which type is required at which parameter position — e.g.
 * {@code Map.of(0, OperationContext.class)} means the first parameter must be (or extend) {@code
 * OperationContext}.
 */
public class AspectDaoOperationContextTest {

  @Test
  public void aspectDaoPublicMethodsMustHaveOperationContextAsFirstParam() {
    OperationContextArchTestUtil.checkArch(AspectDao.class, Map.of(0, OperationContext.class));
  }

  @Test
  public void eventProducerPublicMethodsMustHaveOperationContextAsFirstParam() {
    OperationContextArchTestUtil.checkArch(EventProducer.class, Map.of(0, OperationContext.class));
  }
}
