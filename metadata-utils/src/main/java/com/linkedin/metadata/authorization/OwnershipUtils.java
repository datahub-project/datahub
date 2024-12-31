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
