package com.linkedin.metadata.testing;

import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnershipSource;
import com.linkedin.common.OwnershipSourceType;
import com.linkedin.common.OwnershipSuggestion;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import java.util.Collections;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


public final class Owners {
  private Owners() {
  }

  @Nonnull
  public static Owner makeOwner(@Nonnull String ldap) {
    return makeOwner(ldap, OwnershipType.DEVELOPER);
  }

  @Nonnull
  public static Owner makeOwner(@Nonnull String ldap, @Nonnull OwnershipType type) {
    return new Owner().setOwner(new CorpuserUrn(ldap)).setType(type);
  }

  @Nonnull
  public static Owner makeOwner(@Nonnull Urn ownerUrn, @Nonnull OwnershipType type,
      @Nullable OwnershipSourceType sourceType, @Nullable String sourceUrl) {
    Owner owner = new Owner().setOwner(ownerUrn).setType(type);

    if (sourceType != null) {
      OwnershipSource source = new OwnershipSource().setType(sourceType);
      if (sourceUrl != null) {
        source.setUrl(sourceUrl);
      }
      owner.setSource(source);
    }

    return owner;
  }

  @Nonnull
  public static Ownership makeOwnership(@Nonnull String ldap) {
    return new Ownership().setOwners(new OwnerArray(Collections.singleton(makeOwner(ldap))));
  }

  @Nonnull
  public static OwnershipSuggestion makeOwnershipSuggestion(@Nonnull String ldap) {
    return new OwnershipSuggestion().setOwners(new OwnerArray(Collections.singleton(makeOwner(ldap))));
  }
}
