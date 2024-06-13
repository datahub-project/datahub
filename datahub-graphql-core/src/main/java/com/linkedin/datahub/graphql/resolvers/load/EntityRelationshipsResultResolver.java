package com.linkedin.datahub.graphql.resolvers.load;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.canView;
import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.common.EntityRelationship;
import com.linkedin.common.EntityRelationships;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityRelationshipsResult;
import com.linkedin.datahub.graphql.generated.RelationshipsInput;
import com.linkedin.datahub.graphql.types.common.mappers.AuditStampMapper;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * GraphQL Resolver responsible for fetching relationships between entities in the DataHub graph.
 */
public class EntityRelationshipsResultResolver
    implements DataFetcher<CompletableFuture<EntityRelationshipsResult>> {

  private final GraphClient _graphClient;

  public EntityRelationshipsResultResolver(final GraphClient graphClient) {
    _graphClient = graphClient;
  }

  @Override
  public CompletableFuture<EntityRelationshipsResult> get(DataFetchingEnvironment environment) {
    final QueryContext context = environment.getContext();
    final String urn = ((Entity) environment.getSource()).getUrn();
    final RelationshipsInput input =
        bindArgument(environment.getArgument("input"), RelationshipsInput.class);

    final List<String> relationshipTypes = input.getTypes();
    final com.linkedin.datahub.graphql.generated.RelationshipDirection relationshipDirection =
        input.getDirection();
    final Integer start = input.getStart(); // Optional!
    final Integer count = input.getCount(); // Optional!
    final RelationshipDirection resolvedDirection =
        RelationshipDirection.valueOf(relationshipDirection.toString());
    return GraphQLConcurrencyUtils.supplyAsync(
        () ->
            mapEntityRelationships(
                context,
                fetchEntityRelationships(
                    urn, relationshipTypes, resolvedDirection, start, count, context.getActorUrn()),
                resolvedDirection),
        this.getClass().getSimpleName(),
        "get");
  }

  private EntityRelationships fetchEntityRelationships(
      final String urn,
      final List<String> types,
      final RelationshipDirection direction,
      final Integer start,
      final Integer count,
      final String actor) {

    return _graphClient.getRelatedEntities(urn, types, direction, start, count, actor);
  }

  private EntityRelationshipsResult mapEntityRelationships(
      @Nullable final QueryContext context,
      final EntityRelationships entityRelationships,
      final RelationshipDirection relationshipDirection) {
    final EntityRelationshipsResult result = new EntityRelationshipsResult();

    List<EntityRelationship> viewable =
        entityRelationships.getRelationships().stream()
            .filter(
                rel -> context == null || canView(context.getOperationContext(), rel.getEntity()))
            .collect(Collectors.toList());

    result.setStart(entityRelationships.getStart());
    result.setCount(viewable.size());
    // TODO  fix the calculation at the graph call
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
