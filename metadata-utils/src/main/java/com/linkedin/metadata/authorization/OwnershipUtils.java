/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.authorization;

import com.linkedin.common.Ownership;
import com.linkedin.common.urn.Urn;
import java.util.List;
import javax.annotation.Nonnull;

public class OwnershipUtils {

  public static boolean isOwnerOfEntity(
      @Nonnull final Ownership entityOwnership,
      @Nonnull final Urn actorUrn,
      @Nonnull final List<Urn> groupsForUser) {
    return entityOwnership.getOwners().stream()
        .anyMatch(
            owner -> owner.getOwner().equals(actorUrn) || groupsForUser.contains(owner.getOwner()));
  }

  private OwnershipUtils() {}
}
