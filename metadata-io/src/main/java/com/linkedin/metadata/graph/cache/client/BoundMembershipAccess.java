package com.linkedin.metadata.graph.cache.client;

import com.datahub.authorization.SessionActorIdentity;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.graph.cache.MembershipNeighborResult;
import com.linkedin.metadata.graph.cache.TraversalDirection;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.ServicesRegistryContext;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

/** Cache-first membership reads with aspect and graph-scroll fallbacks. */
public final class BoundMembershipAccess {

  private static final int MEMBERSHIP_MAX_DEPTH = 1;

  private BoundMembershipAccess() {}

  @Nonnull
  public static MembershipNeighborResult listRelated(
      @Nonnull OperationContext opContext,
      @Nonnull MembershipReadSpec spec,
      @Nonnull Urn seedUrn,
      @Nonnull TraversalDirection direction,
      @Nonnull Set<String> relationshipTypes,
      int start,
      int count) {
    return listRelated(opContext, spec, seedUrn, direction, relationshipTypes, start, count, false);
  }

  @Nonnull
  public static MembershipNeighborResult listRelated(
      @Nonnull OperationContext opContext,
      @Nonnull MembershipReadSpec spec,
      @Nonnull Urn seedUrn,
      @Nonnull TraversalDirection direction,
      @Nonnull Set<String> relationshipTypes,
      int start,
      int count,
      boolean includeSoftDelete) {
    if (relationshipTypes.isEmpty() || count <= 0) {
      return MembershipNeighborResult.fromNeighbors(List.of(), 0);
    }
    if (shouldUseCache(includeSoftDelete)) {
      MembershipNeighborResult cached =
          EntityGraphCacheClients.listRelated(
              opContext,
              opContext.getEntityGraphCache(),
              spec.getBinding(),
              seedUrn.toString(),
              direction,
              relationshipTypes,
              MEMBERSHIP_MAX_DEPTH,
              start,
              count);
      if (cached.isHit()) {
        return cached;
      }
    }
    MembershipNeighborResult aspectResult =
        aspectFallback(opContext, spec, seedUrn, direction, relationshipTypes, start, count);
    if (aspectResult.isHit()) {
      return aspectResult;
    }
    return MembershipGraphScrollFallback.listRelated(
        opContext, spec, seedUrn.toString(), direction, relationshipTypes, start, count);
  }

  @Nonnull
  public static Set<Urn> groupsForUser(
      @Nonnull OperationContext opContext, @Nonnull MembershipReadSpec spec, @Nonnull Urn userUrn) {
    return groupsForUser(opContext, spec, userUrn, false);
  }

  @Nonnull
  public static Set<Urn> groupsForUser(
      @Nonnull OperationContext opContext,
      @Nonnull MembershipReadSpec spec,
      @Nonnull Urn userUrn,
      boolean includeSoftDelete) {
    MembershipNeighborResult result =
        listRelated(
            opContext,
            spec,
            userUrn,
            TraversalDirection.FORWARD,
            spec.getGroupRelationshipTypes(),
            0,
            Integer.MAX_VALUE,
            includeSoftDelete);
    return toNeighborUrns(result);
  }

  @Nonnull
  public static Set<Urn> directRolesForUser(
      @Nonnull OperationContext opContext, @Nonnull MembershipReadSpec spec, @Nonnull Urn userUrn) {
    return directRolesForUser(opContext, spec, userUrn, false);
  }

  @Nonnull
  public static Set<Urn> directRolesForUser(
      @Nonnull OperationContext opContext,
      @Nonnull MembershipReadSpec spec,
      @Nonnull Urn userUrn,
      boolean includeSoftDelete) {
    MembershipNeighborResult result =
        listRelated(
            opContext,
            spec,
            userUrn,
            TraversalDirection.FORWARD,
            spec.getRoleRelationshipTypes(),
            0,
            Integer.MAX_VALUE,
            includeSoftDelete);
    return toNeighborUrns(result);
  }

  @Nonnull
  public static Set<Urn> effectiveRolesForUser(
      @Nonnull OperationContext opContext, @Nonnull MembershipReadSpec spec, @Nonnull Urn userUrn) {
    return effectiveRolesForUser(opContext, spec, userUrn, false);
  }

  @Nonnull
  public static Set<Urn> effectiveRolesForUser(
      @Nonnull OperationContext opContext,
      @Nonnull MembershipReadSpec spec,
      @Nonnull Urn userUrn,
      boolean includeSoftDelete) {
    Set<Urn> roles =
        new LinkedHashSet<>(directRolesForUser(opContext, spec, userUrn, includeSoftDelete));
    for (Urn groupUrn : groupsForUser(opContext, spec, userUrn, includeSoftDelete)) {
      roles.addAll(rolesForGroup(opContext, spec, groupUrn, includeSoftDelete));
    }
    if (!roles.isEmpty() || !shouldUseCache(includeSoftDelete)) {
      return roles;
    }
    ServicesRegistryContext services = opContext.getServicesRegistryContext();
    if (services != null) {
      SessionActorIdentity identity = services.fetchUserIdentity(opContext, userUrn);
      return identity.resolveAllRoles(groups -> services.fetchRolesViaGroups(opContext, groups));
    }
    return roles;
  }

  @Nonnull
  public static Set<Urn> rolesForGroup(
      @Nonnull OperationContext opContext,
      @Nonnull MembershipReadSpec spec,
      @Nonnull Urn groupUrn) {
    return rolesForGroup(opContext, spec, groupUrn, false);
  }

  @Nonnull
  public static Set<Urn> rolesForGroup(
      @Nonnull OperationContext opContext,
      @Nonnull MembershipReadSpec spec,
      @Nonnull Urn groupUrn,
      boolean includeSoftDelete) {
    MembershipNeighborResult result =
        listRelated(
            opContext,
            spec,
            groupUrn,
            TraversalDirection.FORWARD,
            spec.getRoleRelationshipTypes(),
            0,
            Integer.MAX_VALUE,
            includeSoftDelete);
    return toNeighborUrns(result);
  }

  @Nonnull
  private static MembershipNeighborResult aspectFallback(
      @Nonnull OperationContext opContext,
      @Nonnull MembershipReadSpec spec,
      @Nonnull Urn seedUrn,
      @Nonnull TraversalDirection direction,
      @Nonnull Set<String> relationshipTypes,
      int start,
      int count) {
    ServicesRegistryContext services = opContext.getServicesRegistryContext();
    if (services == null) {
      return MembershipNeighborResult.miss(com.linkedin.metadata.graph.cache.ReadMissReason.ABSENT);
    }

    String entityType = seedUrn.getEntityType();
    List<MembershipNeighborResult.Neighbor> neighbors = new ArrayList<>();

    if ("corpuser".equals(entityType)
        && direction == TraversalDirection.FORWARD
        && relationshipTypes.stream().anyMatch(spec.getGroupRelationshipTypes()::contains)) {
      if (relationshipTypes.size() > 1
          || relationshipTypes.contains(
              com.linkedin.metadata.Constants.IS_MEMBER_OF_NATIVE_GROUP_RELATIONSHIP_NAME)) {
        return MembershipNeighborResult.miss(
            com.linkedin.metadata.graph.cache.ReadMissReason.ABSENT);
      }
      SessionActorIdentity identity = services.fetchUserIdentity(opContext, seedUrn);
      for (Urn groupUrn : identity.getCorpGroups()) {
        neighbors.add(
            new MembershipNeighborResult.Neighbor(
                groupUrn.toString(),
                com.linkedin.metadata.Constants.IS_MEMBER_OF_GROUP_RELATIONSHIP_NAME));
      }
      return paginateNeighbors(neighbors, start, count);
    }

    if ("corpuser".equals(entityType)
        && direction == TraversalDirection.FORWARD
        && relationshipTypes.equals(spec.getRoleRelationshipTypes())) {
      SessionActorIdentity identity = services.fetchUserIdentity(opContext, seedUrn);
      for (Urn roleUrn : identity.getDirectRoles()) {
        neighbors.add(
            new MembershipNeighborResult.Neighbor(
                roleUrn.toString(), spec.getRoleRelationshipTypes().iterator().next()));
      }
      return paginateNeighbors(neighbors, start, count);
    }

    if ("corpGroup".equals(entityType)
        && direction == TraversalDirection.FORWARD
        && relationshipTypes.equals(spec.getRoleRelationshipTypes())) {
      for (Urn roleUrn : services.fetchRolesViaGroups(opContext, List.of(seedUrn))) {
        neighbors.add(
            new MembershipNeighborResult.Neighbor(
                roleUrn.toString(), spec.getRoleRelationshipTypes().iterator().next()));
      }
      return paginateNeighbors(neighbors, start, count);
    }

    return MembershipNeighborResult.miss(com.linkedin.metadata.graph.cache.ReadMissReason.ABSENT);
  }

  @Nonnull
  private static MembershipNeighborResult paginateNeighbors(
      @Nonnull List<MembershipNeighborResult.Neighbor> neighbors, int start, int count) {
    int total = neighbors.size();
    int resolvedStart = Math.max(start, 0);
    if (resolvedStart >= total) {
      return MembershipNeighborResult.fromNeighbors(List.of(), total);
    }
    int end = Math.min(resolvedStart + count, total);
    return MembershipNeighborResult.fromNeighbors(neighbors.subList(resolvedStart, end), total);
  }

  @Nonnull
  private static Set<Urn> toNeighborUrns(@Nonnull MembershipNeighborResult result) {
    return result.neighborsOrEmpty().stream()
        .map(MembershipNeighborResult.Neighbor::neighborUrn)
        .map(UrnUtils::getUrn)
        .collect(Collectors.toCollection(LinkedHashSet::new));
  }

  private static boolean shouldUseCache(boolean includeSoftDelete) {
    return !includeSoftDelete;
  }
}
