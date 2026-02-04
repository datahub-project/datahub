package com.linkedin.datahub.graphql.types.auth;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.OAuthAuthorizationServer;
import com.linkedin.datahub.graphql.types.auth.mappers.OAuthAuthorizationServerMapper;
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
 * GraphQL type for the OAuthAuthorizationServer entity.
 *
 * <p>OAuth Authorization Servers represent external OAuth providers that DataHub can use to obtain
 * tokens for calling external APIs (OUTBOUND OAuth).
 */
public class OAuthAuthorizationServerType
    implements com.linkedin.datahub.graphql.types.EntityType<OAuthAuthorizationServer, String> {

  static final Set<String> ASPECTS_TO_FETCH =
      ImmutableSet.of(
          Constants.OAUTH_AUTHORIZATION_SERVER_PROPERTIES_ASPECT_NAME,
          Constants.OWNERSHIP_ASPECT_NAME,
          Constants.STATUS_ASPECT_NAME);

  private final EntityClient entityClient;

  public OAuthAuthorizationServerType(@Nonnull final EntityClient entityClient) {
    this.entityClient = Objects.requireNonNull(entityClient, "entityClient must not be null");
  }

  @Override
  public EntityType type() {
    return EntityType.OAUTH_AUTHORIZATION_SERVER;
  }

  @Override
  public Function<Entity, String> getKeyProvider() {
    return Entity::getUrn;
  }

  @Override
  public Class<OAuthAuthorizationServer> objectClass() {
    return OAuthAuthorizationServer.class;
  }

  @Override
  public List<DataFetcherResult<OAuthAuthorizationServer>> batchLoadWithoutAuthorization(
      @Nonnull List<String> urns, @Nonnull QueryContext context) throws Exception {
    final List<Urn> serverUrns = urns.stream().map(UrnUtils::getUrn).collect(Collectors.toList());
    try {
      final Map<Urn, EntityResponse> entities =
          entityClient.batchGetV2(
              context.getOperationContext(),
              Constants.OAUTH_AUTHORIZATION_SERVER_ENTITY_NAME,
              new HashSet<>(serverUrns),
              ASPECTS_TO_FETCH);

      return mapResponsesToBatchResults(
          urns, entities, OAuthAuthorizationServerMapper::map, context);
    } catch (Exception e) {
      throw new RuntimeException("Failed to batch load OAuthAuthorizationServers", e);
    }
  }
}
