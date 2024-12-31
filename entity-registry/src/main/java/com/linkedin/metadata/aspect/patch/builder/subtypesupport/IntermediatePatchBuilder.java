package com.linkedin.metadata.aspect.patch.builder.subtypesupport;

import com.linkedin.metadata.aspect.patch.builder.AbstractMultiFieldPatchBuilder;

/**
 * Used for supporting intermediate subtypes when constructing a patch for an aspect that includes
 * complex objects.
 *
 * @param <T> The parent patch builder type
 */
public interface IntermediatePatchBuilder<T extends AbstractMultiFieldPatchBuilder<T>> {

  /** Convenience method to return parent patch builder in functional callstack */
  T getParent();
}
