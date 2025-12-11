/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
