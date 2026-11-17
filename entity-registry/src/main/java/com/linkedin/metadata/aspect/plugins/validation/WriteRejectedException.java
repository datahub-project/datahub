package com.linkedin.metadata.aspect.plugins.validation;

/**
 * A write that validation rejected, as opposed to one that failed in storage or at runtime. It lets
 * modules that cannot see the entity service's validation exception tell a rejected write, which a
 * caller may skip and report, from an operational failure, which must propagate.
 */
public abstract class WriteRejectedException extends RuntimeException {

  protected WriteRejectedException(final String message) {
    super(message);
  }
}
