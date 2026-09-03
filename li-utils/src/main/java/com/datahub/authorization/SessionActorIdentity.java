package com.datahub.authorization;

import com.linkedin.common.urn.Urn;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import javax.annotation.Nonnull;
import lombok.Getter;

/**
 * Request-scoped snapshot of a corp user's group membership and role membership. {@link #groups}
 * includes both corp (SSO) and native groups, deduplicated. {@link #corpGroups} and {@link
 * #nativeGroups} retain the source aspect distinction for relationship-type labeling. Roles
 * inherited via groups are resolved lazily.
 */
@Getter
public final class SessionActorIdentity {

  private final Urn actorUrn;
  private final List<Urn> corpGroups;
  private final List<Urn> nativeGroups;
  private final List<Urn> groups;
  private final Set<Urn> directRoles;
  private volatile Set<Urn> allRoles;

  public SessionActorIdentity(
      @Nonnull final Urn actorUrn,
      @Nonnull final List<Urn> corpGroups,
      @Nonnull final List<Urn> nativeGroups,
      @Nonnull final Set<Urn> directRoles) {
    this.actorUrn = actorUrn;
    this.corpGroups = List.copyOf(corpGroups);
    this.nativeGroups = List.copyOf(nativeGroups);
    this.groups = List.copyOf(mergeGroupMembership(corpGroups, nativeGroups));
    this.directRoles = Collections.unmodifiableSet(new HashSet<>(directRoles));
  }

  /** Treats all groups as corp (SSO) membership for callers without native/corp distinction. */
  public SessionActorIdentity(
      @Nonnull final Urn actorUrn,
      @Nonnull final List<Urn> groups,
      @Nonnull final Set<Urn> directRoles) {
    this(actorUrn, groups, List.of(), directRoles);
  }

  public static SessionActorIdentity empty(@Nonnull final Urn actorUrn) {
    return new SessionActorIdentity(actorUrn, List.of(), List.of(), Set.of());
  }

  /**
   * Returns direct roles plus roles inherited from group membership. The group role lookup runs at
   * most once per identity instance.
   */
  @Nonnull
  public Set<Urn> resolveAllRoles(
      @Nonnull final Function<Collection<Urn>, Set<Urn>> rolesViaGroupsFetcher) {
    if (allRoles != null) {
      return allRoles;
    }
    synchronized (this) {
      if (allRoles == null) {
        final Set<Urn> roles = new HashSet<>(directRoles);
        if (!groups.isEmpty()) {
          roles.addAll(rolesViaGroupsFetcher.apply(groups));
        }
        allRoles = Collections.unmodifiableSet(roles);
      }
      return allRoles;
    }
  }

  /** Builds a deduplicated group list from corp and native membership aspects. */
  @Nonnull
  public static List<Urn> mergeGroupMembership(
      @Nonnull final Collection<Urn> corpGroups, @Nonnull final Collection<Urn> nativeGroups) {
    final Set<Urn> merged = new HashSet<>();
    merged.addAll(corpGroups);
    merged.addAll(nativeGroups);
    return new ArrayList<>(merged);
  }
}
