package com.datahub.authorization;

import com.linkedin.common.urn.Urn;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Why a policy's actor filter applies to an actor. */
public record ActorPolicyMatchReason(@Nonnull Type type, @Nullable Urn group, @Nullable Urn role) {

  public enum Type {
    DIRECT_USER,
    DIRECT_GROUP,
    GROUP,
    ROLE,
    ROLE_VIA_GROUP,
    ALL_USERS,
    ALL_GROUPS,
    RESOURCE_OWNER
  }
}
