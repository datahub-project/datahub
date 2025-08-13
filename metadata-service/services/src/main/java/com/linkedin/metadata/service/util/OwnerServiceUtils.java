package com.linkedin.metadata.service.util;

import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnershipSource;
import com.linkedin.common.OwnershipSourceType;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.Constants;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class OwnerServiceUtils {
  public static final String SYSTEM_ID = "__system__";

  public static void addOwnerToAspect(
      Ownership ownershipAspect, Urn ownerUrn, OwnershipType type, Urn ownershipTypeUrn) {
    addOwnerToAspect(ownershipAspect, ownerUrn, type, ownershipTypeUrn, null);
  }

  public static void addOwnerToAspect(
      Ownership ownershipAspect,
      Urn ownerUrn,
      OwnershipType type,
      Urn ownershipTypeUrn,
      @Nullable OwnershipSourceType sourceType) {
    if (!ownershipAspect.hasOwners()) {
      ownershipAspect.setOwners(new OwnerArray());
    }

    OwnerArray ownerArray = new OwnerArray(ownershipAspect.getOwners());
    removeExistingOwnerIfExists(ownerArray, ownerUrn, ownershipTypeUrn);

    Owner newOwner = new Owner();

    newOwner.setType(type);
    newOwner.setTypeUrn(ownershipTypeUrn);
    if (sourceType != null) {
      newOwner.setSource(new OwnershipSource().setType(sourceType));
    }
    newOwner.setOwner(ownerUrn);
    ownerArray.add(newOwner);
    ownershipAspect.setOwners(ownerArray);
  }

  public static void removeExistingOwnerIfExists(
      OwnerArray ownerArray, Urn ownerUrn, Urn ownershipTypeUrn) {
    ownerArray.removeIf(
        owner -> {
          // Remove old ownership if it exists (check ownerUrn + type (entity & deprecated type))
          return isOwnerEqual(owner, ownerUrn, ownershipTypeUrn);
        });
  }

  public static boolean isOwnerEqual(
      @Nonnull Owner owner, @Nonnull Urn ownerUrn, @Nullable Urn ownershipTypeUrn) {
    if (!owner.getOwner().equals(ownerUrn)) {
      return false;
    }
    if (owner.getTypeUrn() != null && ownershipTypeUrn != null) {
      return owner.getTypeUrn().equals(ownershipTypeUrn);
    }
    if (ownershipTypeUrn == null) {
      return true;
    }
    // Fall back to mapping deprecated type to the new ownership entity
    return mapOwnershipTypeToEntity(OwnershipType.valueOf(owner.getType().toString()).name())
        .equals(ownershipTypeUrn);
  }

  public static Urn mapOwnershipTypeToEntity(String type) {
    final String typeName = SYSTEM_ID + type.toLowerCase();
    return Urn.createFromTuple(Constants.OWNERSHIP_TYPE_ENTITY_NAME, typeName);
  }
}
