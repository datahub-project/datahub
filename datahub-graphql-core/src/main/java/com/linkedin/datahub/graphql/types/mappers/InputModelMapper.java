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
import javax.annotation.Nullable;

/** Maps an input of type I to an output of type O with actor context. */
public interface InputModelMapper<I, O, A> {
  O apply(@Nullable final QueryContext context, final I input, final A actor);
}
