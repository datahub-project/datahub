package com.datahub.authorization;

import java.util.List;
import javax.annotation.Nonnull;

/**
 * The policies that apply to an actor. {@code ownerPolicies} apply only on resources the actor
 * owns, so they are listed apart from the policies that apply everywhere.
 */
public record ActorPolicies(
    @Nonnull List<ActorPolicyMatch> policies, @Nonnull List<ActorPolicyMatch> ownerPolicies) {}
