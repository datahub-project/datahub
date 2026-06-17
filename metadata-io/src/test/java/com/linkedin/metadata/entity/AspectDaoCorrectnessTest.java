package com.linkedin.metadata.entity;

import io.datahubproject.metadata.context.OperationContext;
import java.util.Map;
import org.testng.annotations.Test;

/**
 * Architectural rule: every public method declared directly on {@link AspectDao} must have {@link
 * OperationContext} as the first parameter, unless annotated with {@link OperationContextExempt}.
 *
 * <p>One test class per checked interface. When a new interface needs the same contract, create a
 * dedicated test class (e.g. {@code MyInterfaceOperationContextTest}) in that interface's package
 * and call {@link OperationContextArchTestUtil#checkArch(Class, Map)} from it. Do not append extra
 * {@code @Test} methods here for unrelated interfaces — keeps class names accurate, failures triage
 * cleanly to one contract, and the convention is grep-able as {@code *OperationContextTest}.
 */
public class AspectDaoCorrectnessTest {

  @Test
  public void aspectDaoPublicMethodsMustHaveOperationContextAsFirstParam() {
    OperationContextArchTestUtil.checkArch(AspectDao.class, Map.of(0, OperationContext.class));
  }
}
