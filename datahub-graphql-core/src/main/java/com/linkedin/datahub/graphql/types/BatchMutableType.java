/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.types;

import com.linkedin.datahub.graphql.QueryContext;
import java.util.List;
import javax.annotation.Nonnull;

public interface BatchMutableType<I, B, T> extends MutableType<I, T> {
  default Class<B[]> batchInputClass() throws UnsupportedOperationException {
    throw new UnsupportedOperationException(
        this.getClass().getName() + " does not implement batchInputClass method");
  }

  default List<T> batchUpdate(@Nonnull final B[] updateInput, QueryContext context)
      throws Exception {
    throw new UnsupportedOperationException(
        this.getClass().getName() + " does not implement batchUpdate method");
  }
}
