package com.datahub.authorization;

import com.datahub.authentication.Authentication;
import com.datahub.authorization.fieldresolverprovider.DataPlatformInstanceFieldResolverProvider;
import com.datahub.authorization.fieldresolverprovider.DomainFieldResolverProvider;
import com.datahub.authorization.fieldresolverprovider.EntityFieldResolverProvider;
import com.datahub.authorization.fieldresolverprovider.EntityTypeFieldResolverProvider;
import com.datahub.authorization.fieldresolverprovider.EntityUrnFieldResolverProvider;
import com.datahub.authorization.fieldresolverprovider.GroupMembershipFieldResolverProvider;
import com.datahub.authorization.fieldresolverprovider.OwnerFieldResolverProvider;
import com.google.common.collect.ImmutableList;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.util.Pair;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DefaultEntitySpecResolver implements EntitySpecResolver {
  private final List<EntityFieldResolverProvider> _entityFieldResolverProviders;

  public DefaultEntitySpecResolver(Authentication systemAuthentication, EntityClient entityClient) {
    _entityFieldResolverProviders =
        ImmutableList.of(
            new EntityTypeFieldResolverProvider(),
            new EntityUrnFieldResolverProvider(),
            new DomainFieldResolverProvider(entityClient, systemAuthentication),
            new OwnerFieldResolverProvider(entityClient, systemAuthentication),
            new DataPlatformInstanceFieldResolverProvider(entityClient, systemAuthentication),
            new GroupMembershipFieldResolverProvider(entityClient, systemAuthentication));
  }

  @Override
  public ResolvedEntitySpec resolve(EntitySpec entitySpec) {
    return new ResolvedEntitySpec(entitySpec, getFieldResolvers(entitySpec));
  }

  private Map<EntityFieldType, FieldResolver> getFieldResolvers(EntitySpec entitySpec) {
    return _entityFieldResolverProviders.stream()
        .flatMap(
            resolver ->
                resolver.getFieldTypes().stream().map(fieldType -> Pair.of(fieldType, resolver)))
        .collect(
            Collectors.toMap(Pair::getKey, pair -> pair.getValue().getFieldResolver(entitySpec)));
  }
}
