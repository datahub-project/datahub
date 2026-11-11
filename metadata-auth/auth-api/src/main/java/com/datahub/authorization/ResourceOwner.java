package com.datahub.authorization;

import com.linkedin.common.urn.Urn;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** An owner of a resource, with the ownership type it was assigned. */
public record ResourceOwner(@Nonnull String ownerUrn, @Nullable Urn ownershipTypeUrn) {}
