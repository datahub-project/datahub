package com.datahub.authorization;

import com.linkedin.common.urn.Urn;
import com.linkedin.policy.DataHubPolicyInfo;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A policy that applies to an actor, with every reason it applies. Built-in policies have no URN.
 */
public record ActorPolicyMatch(
    @Nullable Urn policyUrn,
    @Nonnull DataHubPolicyInfo info,
    boolean builtIn,
    @Nonnull List<ActorPolicyMatchReason> reasons) {}
