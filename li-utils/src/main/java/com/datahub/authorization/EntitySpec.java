/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.datahub.authorization;

import javax.annotation.Nonnull;
import lombok.Value;

/**
 * Details about the entities involved in the authorization process. It models the actor and the
 * resource being acted upon. Resource types currently supported can be found inside of {@link
 * com.linkedin.metadata.authorization.PoliciesConfig}
 */
@Value
public class EntitySpec {
  /** The entity type. (dataset, chart, dashboard, corpGroup, etc). */
  @Nonnull String type;

  /**
   * The entity identity. Most often, this corresponds to the raw entity urn.
   * (urn:li:corpGroup:groupId)
   */
  @Nonnull String entity;
}
