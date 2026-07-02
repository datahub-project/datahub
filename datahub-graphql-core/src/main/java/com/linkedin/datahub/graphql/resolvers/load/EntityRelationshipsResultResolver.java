package com.linkedin.datahub.graphql.resolvers.load;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.canView;
import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.getQueryContext;
import static com.linkedin.metadata.Constants.CONTAINER_ENTITY_NAME;
import static com.linkedin.metadata.Constants.CORP_GROUP_ENTITY_NAME;
import static com.linkedin.metadata.Constants.CORP_USER_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATAHUB_ROLE_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DOMAIN_ENTITY_NAME;
import static com.linkedin.metadata.Constants.GLOSSARY_NODE_ENTITY_NAME;
import static com.linkedin.metadata.Constants.IS_MEMBER_OF_GROUP_RELATIONSHIP_NAME;
import static com.linkedin.metadata.Constants.IS_MEMBER_OF_NATIVE_GROUP_RELATIONSHIP_NAME;
import static com.linkedin.metadata.Constants.IS_PART_OF_RELATIONSHIP_NAME;

import com.datahub.authorization.SessionActorIdentity;
import com.linkedin.common.EntityRelationship;
import com.linkedin.common.EntityRelationships;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityRelationshipsResult;
import com.linkedin.datahub.graphql.generated.RelationshipsInput;
import com.linkedin.datahub.graphql.types.common.mappers.AuditStampMapper;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.graph.cache.MembershipNeighborResult;
import com.linkedin.metadata.graph.cache.TraversalDirection;
import com.linkedin.metadata.graph.cache.client.BoundHierarchyAccess;
import com.linkedin.metadata.graph.cache.client.BoundMembershipAccess;
import com.linkedin.metadata.graph.cache.client.HierarchyBindings;
import com.linkedin.metadata.graph.cache.client.MembershipBindings;
import com.linkedin.metadata.graph.cache.client.MembershipReadSpec;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.ActorContext;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * GraphQL Resolver responsible for fetching relationships between entities in the DataHub graph.
 * All GraphQL "relationships" fields are wired to this resolver (see GmsGraphQLEngine and
 * DocumentResolvers), so cycle detection here covers every recursive relationship resolution path.
 */
public class EntityRelationshipsResultResolver
    implements DataFetcher<CompletableFuture<EntityRelationshipsResult>> {

  private static final Set<String> DOMAIN_CHILD_RELATIONSHIP_TYPES =
      Set.of(IS_PART_OF_RELATIONSHIP_NAME);

  private static final Set<String> GLOSSARY_CHILD_RELATIONSHIP_TYPES =
      Set.of(IS_PART_OF_RELATIONSHIP_NAME);

  private static final Set<String> CONTAINER_CHILD_RELATIONSHIP_TYPES =
      Set.of(IS_PART_OF_RELATIONSHIP_NAME);

  private static final Set<String> GROUP_MEMBERSHIP_RELATIONSHIP_TYPES =
      Set.of(IS_MEMBER_OF_GROUP_RELATIONSHIP_NAME, IS_MEMBER_OF_NATIVE_GROUP_RELATIONSHIP_NAME);

  private static final Set<String> ROLE_MEMBERSHIP_RELATIONSHIP_TYPES = Set.of("IsMemberOfRole");

  private final GraphClient _graphClient;

  private final EntityService _entityService;

  public EntityRelationshipsResultResolver(final GraphClient graphClient) {
    this(graphClient, null);
  }

  public EntityRelationshipsResultResolver(
      final GraphClient graphClient, final EntityService entityService) {
    _graphClient = graphClient;
    _entityService = entityService;
  }

  @Override
  public CompletableFuture<EntityRelationshipsResult> get(DataFetchingEnvironment environment) {
    final QueryContext context = getQueryContext(environment);
    final String urn = ((Entity) environment.getSource()).getUrn();
    final RelationshipsInput input =
        bindArgument(environment.getArgument("input"), RelationshipsInput.class);

    if (context == null) {
      return CompletableFuture.completedFuture(emptyEntityRelationshipsResult());
    }
    if (context
        .getRelationshipTraversalContext()
        .filter(traversal -> !traversal.tryVisit(urn))
        .isPresent()) {
      return CompletableFuture.completedFuture(emptyEntityRelationshipsResult());
    }

    final Set<String> relationshipTypes = new HashSet<>(input.getTypes());
    final com.linkedin.datahub.graphql.generated.RelationshipDirection relationshipDirection =
        input.getDirection();
    final Integer start = input.getStart();
    final Integer count = input.getCount();
    final RelationshipDirection resolvedDirection =
        RelationshipDirection.valueOf(relationshipDirection.toString());
    final boolean includeSoftDelete = input.getIncludeSoftDelete();
    final Set<String> relatedEntityTypes =
        input.getRelatedEntityTypes() == null ? null : new HashSet<>(input.getRelatedEntityTypes());

    if (isDomainDirectChildDomainsQuery(urn, relationshipTypes, resolvedDirection)) {
      return GraphQLConcurrencyUtils.supplyAsync(
          () ->
              mapDomainChildRelationships(
                  context, urn, start, count, relationshipDirection, includeSoftDelete),
          this.getClass().getSimpleName(),
          "getDomainChildRelationships");
    }

    if (isGlossaryDirectChildrenQuery(urn, relationshipTypes, resolvedDirection)) {
      return GraphQLConcurrencyUtils.supplyAsync(
          () ->
              mapGlossaryChildRelationships(
                  context, urn, start, count, relationshipDirection, includeSoftDelete),
          this.getClass().getSimpleName(),
          "getGlossaryChildRelationships");
    }

    if (isContainerDirectChildrenQuery(urn, relationshipTypes, resolvedDirection)) {
      return GraphQLConcurrencyUtils.supplyAsync(
          () ->
              mapContainerChildRelationships(
                  context, urn, start, count, relationshipDirection, includeSoftDelete),
          this.getClass().getSimpleName(),
          "getContainerChildRelationships");
    }

    if (isSessionUserOutgoingMembershipQuery(context, urn, relationshipTypes, resolvedDirection)) {
      return GraphQLConcurrencyUtils.supplyAsync(
          () ->
              mapSessionUserMembershipRelationships(
                  context,
                  urn,
                  relationshipTypes,
                  start,
                  count,
                  relationshipDirection,
                  includeSoftDelete),
          this.getClass().getSimpleName(),
          "getSessionUserMembershipRelationships");
    }

    if (isMembershipQuery(urn, relationshipTypes, resolvedDirection)) {
      return GraphQLConcurrencyUtils.supplyAsync(
          () ->
              mapMembershipRelationships(
                  context,
                  urn,
                  relationshipTypes,
                  resolvedDirection,
                  start,
                  count,
                  relationshipDirection,
                  includeSoftDelete),
          this.getClass().getSimpleName(),
          "getMembershipRelationships");
    }

    return GraphQLConcurrencyUtils.supplyAsync(
        () ->
            mapEntityRelationships(
                context,
                fetchEntityRelationships(
                    urn, relationshipTypes, resolvedDirection, start, count, context.getActorUrn()),
                resolvedDirection,
                includeSoftDelete,
                relatedEntityTypes),
        this.getClass().getSimpleName(),
        "get");
  }

  private static boolean isDomainDirectChildDomainsQuery(
      @Nonnull String urn,
      @Nonnull Set<String> relationshipTypes,
      @Nonnull RelationshipDirection direction) {
    return DOMAIN_ENTITY_NAME.equals(UrnUtils.getUrn(urn).getEntityType())
        && relationshipTypes.equals(DOMAIN_CHILD_RELATIONSHIP_TYPES)
        && direction == RelationshipDirection.INCOMING;
  }

  private static boolean isGlossaryDirectChildrenQuery(
      @Nonnull String urn,
      @Nonnull Set<String> relationshipTypes,
      @Nonnull RelationshipDirection direction) {
    return GLOSSARY_NODE_ENTITY_NAME.equals(UrnUtils.getUrn(urn).getEntityType())
        && relationshipTypes.equals(GLOSSARY_CHILD_RELATIONSHIP_TYPES)
        && direction == RelationshipDirection.INCOMING;
  }

  private static boolean isContainerDirectChildrenQuery(
      @Nonnull String urn,
      @Nonnull Set<String> relationshipTypes,
      @Nonnull RelationshipDirection direction) {
    return CONTAINER_ENTITY_NAME.equals(UrnUtils.getUrn(urn).getEntityType())
        && relationshipTypes.equals(CONTAINER_CHILD_RELATIONSHIP_TYPES)
        && direction == RelationshipDirection.INCOMING;
  }

  private static boolean isSessionUserOutgoingMembershipQuery(
      @Nonnull QueryContext context,
      @Nonnull String urn,
      @Nonnull Set<String> relationshipTypes,
      @Nonnull RelationshipDirection direction) {
    if (direction != RelationshipDirection.OUTGOING
        || !urn.equals(context.getActorUrn())
        || !CORP_USER_ENTITY_NAME.equals(UrnUtils.getUrn(urn).getEntityType())) {
      return false;
    }
    if (relationshipTypes.equals(Set.of(IS_MEMBER_OF_NATIVE_GROUP_RELATIONSHIP_NAME))) {
      return false;
    }
    boolean wantsGroups = isSubsetOf(relationshipTypes, GROUP_MEMBERSHIP_RELATIONSHIP_TYPES);
    boolean wantsRoles = relationshipTypes.equals(ROLE_MEMBERSHIP_RELATIONSHIP_TYPES);
    boolean wantsMixed =
        relationshipTypes.stream().anyMatch(GROUP_MEMBERSHIP_RELATIONSHIP_TYPES::contains)
            && relationshipTypes.stream().anyMatch(ROLE_MEMBERSHIP_RELATIONSHIP_TYPES::contains);
    return wantsGroups || wantsRoles || wantsMixed;
  }

  private static boolean isMembershipQuery(
      @Nonnull String urn,
      @Nonnull Set<String> relationshipTypes,
      @Nonnull RelationshipDirection direction) {
    String entityType = UrnUtils.getUrn(urn).getEntityType();
    if (CORP_USER_ENTITY_NAME.equals(entityType)
        && direction == RelationshipDirection.OUTGOING
        && isSubsetOf(relationshipTypes, GROUP_MEMBERSHIP_RELATIONSHIP_TYPES)) {
      return true;
    }
    if (CORP_USER_ENTITY_NAME.equals(entityType)
        && direction == RelationshipDirection.OUTGOING
        && relationshipTypes.equals(ROLE_MEMBERSHIP_RELATIONSHIP_TYPES)) {
      return true;
    }
    if (CORP_GROUP_ENTITY_NAME.equals(entityType)
        && direction == RelationshipDirection.INCOMING
        && isSubsetOf(relationshipTypes, GROUP_MEMBERSHIP_RELATIONSHIP_TYPES)) {
      return true;
    }
    if (CORP_GROUP_ENTITY_NAME.equals(entityType)
        && direction == RelationshipDirection.OUTGOING
        && relationshipTypes.equals(ROLE_MEMBERSHIP_RELATIONSHIP_TYPES)) {
      return true;
    }
    return DATAHUB_ROLE_ENTITY_NAME.equals(entityType)
        && direction == RelationshipDirection.INCOMING
        && relationshipTypes.equals(ROLE_MEMBERSHIP_RELATIONSHIP_TYPES);
  }

  private static boolean isSubsetOf(@Nonnull Set<String> requested, @Nonnull Set<String> allowed) {
    return !requested.isEmpty() && allowed.containsAll(requested);
  }

  private EntityRelationshipsResult mapSessionUserMembershipRelationships(
      @Nonnull final QueryContext context,
      @Nonnull final String urn,
      @Nonnull final Set<String> relationshipTypes,
      @Nullable final Integer start,
      @Nullable final Integer count,
      @Nonnull
          final com.linkedin.datahub.graphql.generated.RelationshipDirection relationshipDirection,
      final boolean includeSoftDelete) {
    ActorContext sessionActor = context.getOperationContext().getSessionActorContext();
    List<EntityRelationship> relationships = new ArrayList<>();

    boolean wantsGroups =
        relationshipTypes.stream().anyMatch(GROUP_MEMBERSHIP_RELATIONSHIP_TYPES::contains);
    boolean wantsRoles =
        relationshipTypes.stream().anyMatch(ROLE_MEMBERSHIP_RELATIONSHIP_TYPES::contains);

    if (wantsGroups) {
      relationships.addAll(sessionUserGroupRelationships(context, sessionActor, relationshipTypes));
    }

    if (wantsRoles) {
      Set<Urn> roleUrns =
          context
              .getOperationContext()
              .getAuthorizationContext()
              .resolveSessionActorRoles(context.getOperationContext(), sessionActor);
      for (Urn roleUrn : roleUrns) {
        relationships.add(new EntityRelationship().setEntity(roleUrn).setType("IsMemberOfRole"));
      }
    }

    List<EntityRelationship> page = paginateRelationships(relationships, start, count);
    EntityRelationshipsResult result =
        mapEntityRelationshipsFromList(
            context, page, relationshipDirection, includeSoftDelete, 0, page.size());
    result.setStart(start != null ? start : 0);
    result.setTotal(relationships.size());
    result.setCount(page.size());
    return result;
  }

  @Nonnull
  private static List<EntityRelationship> sessionUserGroupRelationships(
      @Nonnull final QueryContext context,
      @Nonnull final ActorContext sessionActor,
      @Nonnull final Set<String> relationshipTypes) {
    SessionActorIdentity identity =
        context
            .getOperationContext()
            .getAuthorizationContext()
            .getSessionActorIdentity(sessionActor.getActorUrn());
    List<Urn> corpGroups;
    List<Urn> nativeGroups;
    if (identity != null) {
      corpGroups = identity.getCorpGroups();
      nativeGroups = identity.getNativeGroups();
    } else {
      corpGroups = List.copyOf(sessionActor.getGroupMembership());
      nativeGroups = List.of();
    }

    List<EntityRelationship> relationships = new ArrayList<>();
    if (relationshipTypes.contains(IS_MEMBER_OF_GROUP_RELATIONSHIP_NAME)) {
      for (Urn groupUrn : corpGroups) {
        relationships.add(
            new EntityRelationship()
                .setEntity(groupUrn)
                .setType(IS_MEMBER_OF_GROUP_RELATIONSHIP_NAME));
      }
    }
    if (relationshipTypes.contains(IS_MEMBER_OF_NATIVE_GROUP_RELATIONSHIP_NAME)) {
      for (Urn groupUrn : nativeGroups) {
        relationships.add(
            new EntityRelationship()
                .setEntity(groupUrn)
                .setType(IS_MEMBER_OF_NATIVE_GROUP_RELATIONSHIP_NAME));
      }
    }
    return relationships;
  }

  private EntityRelationshipsResult mapMembershipRelationships(
      @Nonnull final QueryContext context,
      @Nonnull final String urn,
      @Nonnull final Set<String> relationshipTypes,
      @Nonnull final RelationshipDirection direction,
      @Nullable final Integer start,
      @Nullable final Integer count,
      @Nonnull
          final com.linkedin.datahub.graphql.generated.RelationshipDirection relationshipDirection,
      final boolean includeSoftDelete) {
    MembershipReadSpec spec = MembershipBindings.membershipSpec(context.getOperationContext());
    Urn seedUrn = UrnUtils.getUrn(urn);
    TraversalDirection traversalDirection = toTraversalDirection(direction);

    List<EntityRelationship> relationships;
    int total;
    if (CORP_USER_ENTITY_NAME.equals(seedUrn.getEntityType())
        && direction == RelationshipDirection.OUTGOING
        && relationshipTypes.equals(ROLE_MEMBERSHIP_RELATIONSHIP_TYPES)) {
      Set<Urn> roleUrns =
          BoundMembershipAccess.effectiveRolesForUser(
              context.getOperationContext(), spec, seedUrn, includeSoftDelete);
      relationships =
          roleUrns.stream()
              .map(role -> new EntityRelationship().setEntity(role).setType("IsMemberOfRole"))
              .collect(Collectors.toList());
      total = relationships.size();
      relationships = paginateRelationships(relationships, start, count);
    } else {
      MembershipNeighborResult result =
          BoundMembershipAccess.listRelated(
              context.getOperationContext(),
              spec,
              seedUrn,
              traversalDirection,
              relationshipTypes,
              start != null ? start : 0,
              count != null ? count : Integer.MAX_VALUE,
              includeSoftDelete);
      relationships =
          result.neighborsOrEmpty().stream()
              .map(
                  neighbor ->
                      new EntityRelationship()
                          .setEntity(UrnUtils.getUrn(neighbor.neighborUrn()))
                          .setType(neighbor.relationshipType()))
              .collect(Collectors.toList());
      total =
          result instanceof MembershipNeighborResult.Hit hit ? hit.total() : relationships.size();
    }

    EntityRelationshipsResult mapped =
        mapEntityRelationshipsFromList(
            context,
            relationships,
            relationshipDirection,
            includeSoftDelete,
            0,
            relationships.size());
    mapped.setStart(start != null ? start : 0);
    mapped.setTotal(total);
    mapped.setCount(mapped.getRelationships().size());
    return mapped;
  }

  private static TraversalDirection toTraversalDirection(@Nonnull RelationshipDirection direction) {
    return direction == RelationshipDirection.OUTGOING
        ? TraversalDirection.FORWARD
        : TraversalDirection.REVERSE;
  }

  private static List<EntityRelationship> paginateRelationships(
      @Nonnull List<EntityRelationship> relationships,
      @Nullable Integer start,
      @Nullable Integer count) {
    int resolvedStart = start != null ? start : 0;
    int resolvedCount = count != null ? count : relationships.size();
    if (resolvedCount <= 0) {
      return List.of();
    }
    return relationships.stream()
        .skip(resolvedStart)
        .limit(resolvedCount)
        .collect(Collectors.toList());
  }

  private EntityRelationshipsResult mapEntityRelationshipsFromList(
      @Nonnull final QueryContext context,
      @Nonnull final List<EntityRelationship> relationships,
      @Nonnull
          final com.linkedin.datahub.graphql.generated.RelationshipDirection relationshipDirection,
      final boolean includeSoftDelete,
      @Nullable final Integer start,
      @Nullable final Integer count) {
    EntityRelationships entityRelationships =
        new EntityRelationships()
            .setStart(start != null ? start : 0)
            .setCount(relationships.size())
            .setTotal(relationships.size())
            .setRelationships(new com.linkedin.common.EntityRelationshipArray(relationships));
    return mapEntityRelationships(
        context,
        entityRelationships,
        RelationshipDirection.valueOf(relationshipDirection.toString()),
        includeSoftDelete,
        null);
  }

  private EntityRelationshipsResult mapDomainChildRelationships(
      @Nonnull final QueryContext context,
      @Nonnull final String urn,
      @Nullable final Integer start,
      @Nullable final Integer count,
      @Nonnull
          final com.linkedin.datahub.graphql.generated.RelationshipDirection relationshipDirection,
      final boolean includeSoftDelete) {
    return mapDirectChildRelationships(
        context,
        urn,
        start,
        count,
        relationshipDirection,
        includeSoftDelete,
        HierarchyBindings.domainSpec(context.getOperationContext()));
  }

  private EntityRelationshipsResult mapGlossaryChildRelationships(
      @Nonnull final QueryContext context,
      @Nonnull final String urn,
      @Nullable final Integer start,
      @Nullable final Integer count,
      @Nonnull
          final com.linkedin.datahub.graphql.generated.RelationshipDirection relationshipDirection,
      final boolean includeSoftDelete) {
    return mapDirectChildRelationships(
        context,
        urn,
        start,
        count,
        relationshipDirection,
        includeSoftDelete,
        HierarchyBindings.glossarySpec(context.getOperationContext()));
  }

  private EntityRelationshipsResult mapContainerChildRelationships(
      @Nonnull final QueryContext context,
      @Nonnull final String urn,
      @Nullable final Integer start,
      @Nullable final Integer count,
      @Nonnull
          final com.linkedin.datahub.graphql.generated.RelationshipDirection relationshipDirection,
      final boolean includeSoftDelete) {
    return mapDirectChildRelationships(
        context,
        urn,
        start,
        count,
        relationshipDirection,
        includeSoftDelete,
        HierarchyBindings.containerSpec(context.getOperationContext()));
  }

  private EntityRelationshipsResult mapDirectChildRelationships(
      @Nonnull final QueryContext context,
      @Nonnull final String urn,
      @Nullable final Integer start,
      @Nullable final Integer count,
      @Nonnull
          final com.linkedin.datahub.graphql.generated.RelationshipDirection relationshipDirection,
      final boolean includeSoftDelete,
      @Nonnull com.linkedin.metadata.graph.cache.client.HierarchyReadSpec spec) {
    Set<Urn> childUrns =
        BoundHierarchyAccess.directChildUrns(
            context.getOperationContext(), spec, UrnUtils.getUrn(urn), includeSoftDelete);

    Set<Urn> existentChildUrns;
    if (!includeSoftDelete && _entityService != null) {
      existentChildUrns = _entityService.exists(context.getOperationContext(), childUrns, false);
    } else {
      existentChildUrns = null;
    }

    List<Urn> viewable =
        childUrns.stream()
            .filter(childUrn -> existentChildUrns == null || existentChildUrns.contains(childUrn))
            .filter(childUrn -> canView(context.getOperationContext(), childUrn))
            .collect(Collectors.toList());

    int resolvedStart = start != null ? start : 0;
    int resolvedCount = count != null ? count : viewable.size();
    List<Urn> page =
        resolvedCount <= 0
            ? List.of()
            : viewable.stream()
                .skip(resolvedStart)
                .limit(resolvedCount)
                .collect(Collectors.toList());

    EntityRelationshipsResult result = new EntityRelationshipsResult();
    result.setStart(resolvedStart);
    result.setTotal(viewable.size());
    result.setCount(page.size());
    result.setRelationships(
        page.stream()
            .map(
                childUrn ->
                    mapEntityRelationship(
                        context,
                        relationshipDirection,
                        new EntityRelationship()
                            .setEntity(childUrn)
                            .setType(IS_PART_OF_RELATIONSHIP_NAME)))
            .collect(Collectors.toList()));
    return result;
  }

  private static EntityRelationshipsResult emptyEntityRelationshipsResult() {
    EntityRelationshipsResult result = new EntityRelationshipsResult();
    result.setStart(0);
    result.setCount(0);
    result.setTotal(0);
    result.setRelationships(List.of());
    return result;
  }

  private EntityRelationships fetchEntityRelationships(
      final String urn,
      final Set<String> types,
      final RelationshipDirection direction,
      final Integer start,
      final Integer count,
      final String actor) {

    return _graphClient.getRelatedEntities(urn, types, direction, start, count, actor);
  }

  private EntityRelationshipsResult mapEntityRelationships(
      @Nullable final QueryContext context,
      final EntityRelationships entityRelationships,
      final RelationshipDirection relationshipDirection,
      final boolean includeSoftDelete,
      @Nullable final Set<String> relatedEntityTypes) {
    final EntityRelationshipsResult result = new EntityRelationshipsResult();

    final Set<Urn> existentUrns;
    if (context != null && _entityService != null && !includeSoftDelete) {
      Set<Urn> allRelatedUrns =
          entityRelationships.getRelationships().stream()
              .map(EntityRelationship::getEntity)
              .collect(Collectors.toSet());
      existentUrns = _entityService.exists(context.getOperationContext(), allRelatedUrns, false);
    } else {
      existentUrns = null;
    }

    List<EntityRelationship> viewable =
        entityRelationships.getRelationships().stream()
            .filter(
                rel ->
                    (existentUrns == null || existentUrns.contains(rel.getEntity()))
                        && (relatedEntityTypes == null
                            || relatedEntityTypes.contains(rel.getEntity().getEntityType()))
                        && (context == null
                            || canView(context.getOperationContext(), rel.getEntity())))
            .collect(Collectors.toList());

    result.setStart(entityRelationships.getStart());
    result.setCount(viewable.size());
    result.setTotal(
        entityRelationships.getTotal() - (entityRelationships.getCount() - viewable.size()));
    result.setRelationships(
        viewable.stream()
            .map(
                entityRelationship ->
                    mapEntityRelationship(
                        context,
                        com.linkedin.datahub.graphql.generated.RelationshipDirection.valueOf(
                            relationshipDirection.name()),
                        entityRelationship))
            .collect(Collectors.toList()));
    return result;
  }

  private com.linkedin.datahub.graphql.generated.EntityRelationship mapEntityRelationship(
      @Nullable final QueryContext context,
      final com.linkedin.datahub.graphql.generated.RelationshipDirection direction,
      final EntityRelationship entityRelationship) {
    final com.linkedin.datahub.graphql.generated.EntityRelationship result =
        new com.linkedin.datahub.graphql.generated.EntityRelationship();
    final Entity partialEntity = UrnToEntityMapper.map(context, entityRelationship.getEntity());
    if (partialEntity != null) {
      result.setEntity(partialEntity);
    }
    result.setType(entityRelationship.getType());
    result.setDirection(direction);
    if (entityRelationship.hasCreated()) {
      result.setCreated(AuditStampMapper.map(context, entityRelationship.getCreated()));
    }
    return result;
  }
}
