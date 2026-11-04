package com.linkedin.datahub.graphql.resolvers.load;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.datahub.graphql.types.mappers.MapperUtils.*;

import com.datahub.authorization.AuthorizationConfiguration;
import com.linkedin.common.UrnArray;
import com.linkedin.common.UrnArrayArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityLineageResult;
import com.linkedin.datahub.graphql.generated.EntityPath;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.LineageDirection;
import com.linkedin.datahub.graphql.generated.LineageInput;
import com.linkedin.datahub.graphql.generated.LineageRelationship;
import com.linkedin.datahub.graphql.generated.Restricted;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.datahub.graphql.types.mappers.MapperUtils;
import com.linkedin.metadata.graph.SiblingGraphService;
import graphql.execution.MergedField;
import graphql.language.Field;
import graphql.language.FragmentDefinition;
import graphql.language.FragmentSpread;
import graphql.language.InlineFragment;
import graphql.language.Selection;
import graphql.language.SelectionSet;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.services.RestrictedService;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * GraphQL Resolver responsible for fetching lineage relationships between entities in the DataHub
 * graph. Lineage relationship denotes whether an entity is directly upstream or downstream of
 * another entity
 */
@Slf4j
public class EntityLineageResultResolver
    implements DataFetcher<CompletableFuture<EntityLineageResult>> {

  private static final String RELATIONSHIPS_FIELD = "relationships";

  private final SiblingGraphService _siblingGraphService;
  private final RestrictedService _restrictedService;
  private final AuthorizationConfiguration _authorizationConfiguration;
  private final FeatureFlags _featureFlags;

  public EntityLineageResultResolver(
      final SiblingGraphService siblingGraphService,
      final RestrictedService restrictedService,
      final AuthorizationConfiguration authorizationConfiguration,
      final FeatureFlags featureFlags) {
    _siblingGraphService = siblingGraphService;
    _restrictedService = restrictedService;
    _authorizationConfiguration = authorizationConfiguration;
    _featureFlags = featureFlags;
  }

  @Override
  public CompletableFuture<EntityLineageResult> get(DataFetchingEnvironment environment) {
    final QueryContext context = environment.getContext();
    Urn urn = UrnUtils.getUrn(((Entity) environment.getSource()).getUrn());
    final LineageInput input = bindArgument(environment.getArgument("input"), LineageInput.class);

    final LineageDirection lineageDirection = input.getDirection();
    // All inputs are optional
    @Nullable final Integer start = input.getStart();
    @Nullable final Integer count = input.getCount();
    @Nullable final Boolean separateSiblings = input.getSeparateSiblings();
    @Nullable final Long startTimeMillis = input.getStartTimeMillis();
    @Nullable
    final Long endTimeMillis =
        ResolverUtils.getLineageEndTimeMillis(
            context.getOperationContext(), input.getStartTimeMillis(), input.getEndTimeMillis());
    final Boolean includeGhostEntities =
        Optional.ofNullable(input.getIncludeGhostEntities()).orElse(false);

    com.linkedin.metadata.graph.LineageDirection resolvedDirection =
        com.linkedin.metadata.graph.LineageDirection.valueOf(lineageDirection.toString());

    // Search cards and sidebars ask only for totals. Counting without fetching edges is flat in
    // sibling group size, where the walk fetches every sibling's edges.
    final boolean countOnly =
        _featureFlags != null
            && _featureFlags.isLineageCountOnlyEnabled()
            && !selectsRelationships(environment);

    final Urn finalUrn = urn;
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            // A source the actor may not view keeps only urn/type after mapper redaction; its
            // lineage must not disclose neighbors, counts or degrees either.
            if (isViewAuthorizationEnabled()
                && !AuthorizationUtils.canView(context.getOperationContext(), finalUrn)) {
              return emptyLineageResult();
            }

            final OperationContext lineageOpContext =
                context
                    .getOperationContext()
                    .withSearchFlags(
                        searchFlags -> searchFlags.setIncludeSoftDeleted(includeGhostEntities))
                    .withLineageFlags(
                        flags ->
                            flags
                                .setStartTimeMillis(startTimeMillis, SetMode.REMOVE_IF_NULL)
                                .setEndTimeMillis(endTimeMillis, SetMode.REMOVE_IF_NULL));
            final boolean resolvedSeparateSiblings =
                separateSiblings != null ? input.getSeparateSiblings() : false;

            if (countOnly) {
              return mapEntityRelationships(
                  context,
                  _siblingGraphService.getLineageCount(
                      lineageOpContext,
                      finalUrn,
                      resolvedDirection,
                      count != null ? count : 100,
                      resolvedSeparateSiblings,
                      includeGhostEntities),
                  Set.of());
            }

            com.linkedin.metadata.graph.EntityLineageResult entityLineageResult =
                _siblingGraphService.getLineage(
                    lineageOpContext,
                    finalUrn,
                    resolvedDirection,
                    start != null ? start : 0,
                    count != null ? count : 100,
                    1,
                    resolvedSeparateSiblings,
                    includeGhostEntities,
                    new HashSet<>());

            final Set<Urn> restrictedUrns =
                isViewAuthorizationEnabled()
                    ? findRestrictedUrns(
                        context.getOperationContext(), finalUrn, entityLineageResult)
                    : Set.of();

            return mapEntityRelationships(context, entityLineageResult, restrictedUrns);
          } catch (Exception e) {
            log.error("Failed to fetch lineage for {}", finalUrn);
            throw new RuntimeException(
                String.format("Failed to fetch lineage for %s", finalUrn), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  /**
   * Gate on this resolver's injected configuration. {@link AuthorizationUtils#canView} separately
   * reads the operation context's view configuration, so a check runs only when both are on.
   */
  private boolean isViewAuthorizationEnabled() {
    return _authorizationConfiguration != null
        && _authorizationConfiguration.getView() != null
        && _authorizationConfiguration.getView().isEnabled();
  }

  /**
   * Every urn the response would disclose that the actor may not view: each neighbor and every
   * element of its paths, which can carry a via entity (e.g. a data job) or a sibling. The source
   * already passed its own check. Each distinct urn is authorized once.
   *
   * <p>Without separateSiblings, edges merged in from the source's siblings start their paths at
   * the sibling, so an unviewable sibling is restricted here too; that those edges exist is still
   * disclosed, as the sibling group is treated as one logical entity.
   */
  private static Set<Urn> findRestrictedUrns(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn source,
      @Nonnull final com.linkedin.metadata.graph.EntityLineageResult entityLineageResult) {
    final Map<Urn, Boolean> viewable = new HashMap<>();
    viewable.put(source, true);
    final Set<Urn> restrictedUrns = new HashSet<>();
    for (com.linkedin.metadata.graph.LineageRelationship rel :
        entityLineageResult.getRelationships()) {
      final Set<Urn> disclosed = new HashSet<>();
      disclosed.add(rel.getEntity());
      if (rel.getPaths() != null) {
        rel.getPaths().forEach(disclosed::addAll);
      }
      for (Urn disclosedUrn : disclosed) {
        if (!viewable.computeIfAbsent(
            disclosedUrn, u -> AuthorizationUtils.canView(opContext, u))) {
          restrictedUrns.add(disclosedUrn);
        }
      }
    }
    return restrictedUrns;
  }

  private static EntityLineageResult emptyLineageResult() {
    final EntityLineageResult result = new EntityLineageResult();
    result.setStart(0);
    result.setCount(0);
    result.setTotal(0);
    result.setFiltered(0);
    result.setRelationships(Collections.emptyList());
    return result;
  }

  /**
   * Whether the request asked for {@code relationships} on this lineage field. Reads the field's
   * own AST rather than {@link DataFetchingEnvironment#getSelectionSet()}: the selection set is
   * computed from the normalized form of the whole operation, and for a search page (a union over
   * every entity type) that exceeds graphql-java's 100,000 field limit and fails the request.
   * Anything that cannot be resolved here (a missing fragment, no merged field) counts as selected,
   * so the full walk is the fallback.
   */
  static boolean selectsRelationships(@Nonnull final DataFetchingEnvironment environment) {
    final MergedField mergedField = environment.getMergedField();
    if (mergedField == null) {
      return true;
    }
    final Map<String, FragmentDefinition> fragments =
        environment.getFragmentsByName() != null ? environment.getFragmentsByName() : Map.of();
    final Set<String> visitedFragments = new HashSet<>();
    for (Field field : mergedField.getFields()) {
      if (selectsRelationships(field.getSelectionSet(), fragments, visitedFragments)) {
        return true;
      }
    }
    return false;
  }

  private static boolean selectsRelationships(
      @Nullable final SelectionSet selectionSet,
      @Nonnull final Map<String, FragmentDefinition> fragments,
      @Nonnull final Set<String> visitedFragments) {
    if (selectionSet == null) {
      return false;
    }
    for (Selection<?> selection : selectionSet.getSelections()) {
      if (selection instanceof Field) {
        if (RELATIONSHIPS_FIELD.equals(((Field) selection).getName())) {
          return true;
        }
      } else if (selection instanceof InlineFragment) {
        if (selectsRelationships(
            ((InlineFragment) selection).getSelectionSet(), fragments, visitedFragments)) {
          return true;
        }
      } else if (selection instanceof FragmentSpread) {
        final String name = ((FragmentSpread) selection).getName();
        if (visitedFragments.add(name)) {
          final FragmentDefinition definition = fragments.get(name);
          if (definition == null
              || selectsRelationships(definition.getSelectionSet(), fragments, visitedFragments)) {
            return true;
          }
        }
      }
    }
    return false;
  }

  private EntityLineageResult mapEntityRelationships(
      @Nullable final QueryContext context,
      final com.linkedin.metadata.graph.EntityLineageResult entityLineageResult,
      final Set<Urn> restrictedUrns) {
    final EntityLineageResult result = new EntityLineageResult();
    result.setStart(entityLineageResult.getStart());
    result.setCount(entityLineageResult.getCount());
    result.setTotal(entityLineageResult.getTotal());
    result.setFiltered(entityLineageResult.getFiltered());
    result.setRelationships(
        entityLineageResult.getRelationships().stream()
            .map(r -> mapEntityRelationship(context, r, restrictedUrns))
            .collect(Collectors.toList()));
    return result;
  }

  private LineageRelationship mapEntityRelationship(
      @Nullable final QueryContext context,
      final com.linkedin.metadata.graph.LineageRelationship lineageRelationship,
      final Set<Urn> restrictedUrns) {
    final LineageRelationship result = new LineageRelationship();
    final Entity entity = mapEntity(context, lineageRelationship.getEntity(), restrictedUrns);
    if (entity != null) {
      result.setEntity(entity);
    }
    result.setType(lineageRelationship.getType());
    result.setDegree(lineageRelationship.getDegree());
    if (lineageRelationship.hasCreatedOn()) {
      result.setCreatedOn(lineageRelationship.getCreatedOn());
    }
    if (lineageRelationship.hasCreatedActor()) {
      final Urn createdActor = lineageRelationship.getCreatedActor();
      result.setCreatedActor(UrnToEntityMapper.map(context, createdActor));
    }
    if (lineageRelationship.hasUpdatedOn()) {
      result.setUpdatedOn(lineageRelationship.getUpdatedOn());
    }
    if (lineageRelationship.hasUpdatedActor()) {
      final Urn updatedActor = lineageRelationship.getUpdatedActor();
      result.setUpdatedActor(UrnToEntityMapper.map(context, updatedActor));
    }
    result.setIsManual(lineageRelationship.hasIsManual() && lineageRelationship.isIsManual());
    if (lineageRelationship.getPaths() != null) {
      UrnArrayArray paths = lineageRelationship.getPaths();
      result.setPaths(
          paths.stream()
              .map(path -> mapPath(context, path, restrictedUrns))
              .collect(Collectors.toList()));
    }

    return result;
  }

  private EntityPath mapPath(
      @Nullable final QueryContext context,
      @Nonnull final UrnArray path,
      @Nonnull final Set<Urn> restrictedUrns) {
    if (restrictedUrns.isEmpty()) {
      return MapperUtils.mapPath(context, path);
    }
    final EntityPath entityPath = new EntityPath();
    entityPath.setPath(
        path.stream().map(p -> mapEntity(context, p, restrictedUrns)).collect(Collectors.toList()));
    return entityPath;
  }

  /** The typed entity for {@code urn}, or an encrypted {@link Restricted} placeholder. */
  @Nullable
  private Entity mapEntity(
      @Nullable final QueryContext context,
      @Nonnull final Urn urn,
      @Nonnull final Set<Urn> restrictedUrns) {
    if (restrictedUrns.contains(urn)) {
      final Restricted restrictedEntity = new Restricted();
      restrictedEntity.setType(EntityType.RESTRICTED);
      restrictedEntity.setUrn(_restrictedService.encryptRestrictedUrn(urn).toString());
      return restrictedEntity;
    }
    return UrnToEntityMapper.map(context, urn);
  }
}
