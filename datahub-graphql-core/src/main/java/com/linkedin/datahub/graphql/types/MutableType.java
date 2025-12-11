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
import javax.annotation.Nonnull;

/**
 * Graph type that can be updated.
 *
 * @param <I>: The input type corresponding to the write.
 */
public interface MutableType<I, T> {
  /** Returns generated GraphQL class associated with the input type */
  Class<I> inputClass();

  /**
   * Update an entity by urn
   *
   * @param urn
   * @param input input type
   * @param context the {@link QueryContext} corresponding to the request.
   */
  T update(@Nonnull final String urn, @Nonnull final I input, @Nonnull final QueryContext context)
      throws Exception;
}
