package com.linkedin.metadata.utils.arch;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks an interface method as exempt from the
 * OperationContext/OperationFingerprint-first-parameter architectural rule. Every exemption must
 * supply a {@link #reason()} explaining why the method legitimately does not need an {@code
 * OperationContext} or {@code OperationFingerprint}.
 *
 * <p>Supported on any interface whose contract is enforced by {@code OperationContextArchTestUtil}.
 * Currently applied to {@code AspectDao} (which requires {@code OperationContext} as first param)
 * and {@code SearchClientShim} (which requires {@code OperationFingerprint} as first param).
 *
 * <p>Currently targets {@link java.lang.annotation.ElementType#METHOD} only. If future
 * architectural rules need class-level exemptions, add {@code ElementType.TYPE} to {@link
 * java.lang.annotation.Target} and extend the ArchUnit condition accordingly.
 */
@Documented
@Retention(RetentionPolicy.CLASS)
@Target(ElementType.METHOD)
public @interface OperationContextExempt {
  /** Required explanation for why this method does not accept an OperationContext. */
  String reason();
}
