/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.types.mappers;

import com.linkedin.datahub.graphql.QueryContext;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Simple interface for classes capable of mapping an input of type I to an output of type O. */
public interface ModelMapper<I, O> {
  O apply(@Nullable final QueryContext context, @Nonnull final I input);
}
