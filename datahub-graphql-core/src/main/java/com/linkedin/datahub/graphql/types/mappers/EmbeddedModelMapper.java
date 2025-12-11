/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.types.mappers;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Made for models that are embedded in other models and thus do not encode their own URN. */
public interface EmbeddedModelMapper<I, O> {
  O apply(
      @Nullable final QueryContext context, @Nonnull final I input, @Nonnull final Urn entityUrn);
}
