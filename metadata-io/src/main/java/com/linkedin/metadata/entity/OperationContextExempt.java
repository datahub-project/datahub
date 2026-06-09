package com.linkedin.metadata.entity;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks an {@link AspectDao} method as exempt from the OperationContext-first-parameter
 * architectural rule. Every exemption must supply a {@link #reason()} explaining why the method
 * legitimately does not need an {@code OperationContext}.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface OperationContextExempt {
  /** Required explanation for why this method does not accept an OperationContext. */
  String reason();
}
