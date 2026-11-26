package com.linkedin.metadata.authorization;

import com.linkedin.common.Owner;
import com.linkedin.common.Ownership;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.Constants;
import java.util.List;
import java.util.Locale;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class OwnershipUtils {

  private static final String SYSTEM_OWNERSHIP_TYPE_PREFIX = "__system__";

  public static boolean isOwnerOfEntity(
      @Nonnull final Ownership entityOwnership,
      @Nonnull final Urn actorUrn,
      @Nonnull final List<Urn> groupsForUser) {
    return entityOwnership.getOwners().stream()
        .anyMatch(
            owner -> owner.getOwner().equals(actorUrn) || groupsForUser.contains(owner.getOwner()));
  }

  /**
   * Like {@link #isOwnerOfEntity(Ownership, Urn, List)} but additionally requires the matched
   * owner's ownership type to be in {@code ownershipTypes}. {@code null}/empty disables the filter.
   *
   * <p>For legacy owners with only the deprecated {@code Owner.type} enum (no {@code typeUrn}) the
   * equivalent {@code __system__<type>} URN is derived so the check stays consistent with {@code
   * OwnerMapper} and {@code OwnerServiceUtils.isOwnerEqual}.
   */
  public static boolean isOwnerOfEntityWithType(
      @Nonnull final Ownership entityOwnership,
      @Nonnull final Urn actorUrn,
      @Nonnull final List<Urn> groupsForUser,
      @Nullable final List<Urn> ownershipTypes) {
    if (ownershipTypes == null || ownershipTypes.isEmpty()) {
      return isOwnerOfEntity(entityOwnership, actorUrn, groupsForUser);
    }

    return entityOwnership.getOwners().stream()
        .anyMatch(
            owner ->
                (owner.getOwner().equals(actorUrn) || groupsForUser.contains(owner.getOwner()))
                    && ownershipTypes.contains(resolveOwnershipTypeUrn(owner)));
  }

  @Nullable
  private static Urn resolveOwnershipTypeUrn(@Nonnull final Owner owner) {
    if (owner.getTypeUrn() != null) {
      return owner.getTypeUrn();
    }
    if (owner.getType() == null) {
      return null;
    }
    final String typeName =
        SYSTEM_OWNERSHIP_TYPE_PREFIX + owner.getType().name().toLowerCase(Locale.ROOT);
    return Urn.createFromTuple(Constants.OWNERSHIP_TYPE_ENTITY_NAME, typeName);
  }

  private OwnershipUtils() {}
}
