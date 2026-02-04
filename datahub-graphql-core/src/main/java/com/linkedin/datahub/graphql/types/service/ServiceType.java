package com.linkedin.datahub.graphql.types.service;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.Service;
import com.linkedin.datahub.graphql.types.service.mappers.ServiceMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import graphql.execution.DataFetcherResult;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

/**
 * GraphQL type for the Service entity.
 *
 * <p>Services represent external services that can be integrated with DataHub's AI features.
 * Current supported types:
 *
 * <ul>
 *   <li>MCP_SERVER - Model Context Protocol servers for AI agent tool integration
 * </ul>
 *
 * <p>Future service types may include:
 *
 * <ul>
 *   <li>REST_API - Generic REST API endpoints
 *   <li>OPEN_API - Services with OpenAPI/Swagger specifications
 *   <li>GRPC - gRPC service endpoints
 * </ul>
 */
public class ServiceType implements com.linkedin.datahub.graphql.types.EntityType<Service, String> {

  /** Aspects to fetch when loading Service entities. Immutable and safe to share. */
  public static final Set<String> ASPECTS_TO_FETCH =
      ImmutableSet.of(
          Constants.SERVICE_PROPERTIES_ASPECT_NAME,
          Constants.MCP_SERVER_PROPERTIES_ASPECT_NAME,
          Constants.SUB_TYPES_ASPECT_NAME,
          Constants.OWNERSHIP_ASPECT_NAME,
          Constants.STATUS_ASPECT_NAME,
          Constants.GLOBAL_TAGS_ASPECT_NAME,
          Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME);

  private final EntityClient entityClient;

  public ServiceType(@Nonnull final EntityClient entityClient) {
    this.entityClient = Objects.requireNonNull(entityClient, "entityClient must not be null");
  }

  @Override
  public EntityType type() {
    return EntityType.SERVICE;
  }

  @Override
  public Function<Entity, String> getKeyProvider() {
    return Entity::getUrn;
  }

  @Override
  public Class<Service> objectClass() {
    return Service.class;
  }

  @Override
  public List<DataFetcherResult<Service>> batchLoadWithoutAuthorization(
      @Nonnull List<String> urns, @Nonnull QueryContext context) throws Exception {
    final List<Urn> serviceUrns = urns.stream().map(UrnUtils::getUrn).collect(Collectors.toList());
    try {
      final Map<Urn, EntityResponse> entities =
          entityClient.batchGetV2(
              context.getOperationContext(),
              Constants.SERVICE_ENTITY_NAME,
              new HashSet<>(serviceUrns),
              ASPECTS_TO_FETCH);

      return mapResponsesToBatchResults(urns, entities, ServiceMapper::map, context);
    } catch (Exception e) {
      throw new RuntimeException("Failed to batch load Services", e);
    }
  }
}
