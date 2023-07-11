package com.datahub.authorization;

import com.datahub.authorization.fieldresolverprovider.*;
import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.linkedin.entity.client.EntityClient;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class DefaultResourceSpecResolver implements ResourceSpecResolver {
  private final List<ResourceFieldResolverProvider> _resourceFieldResolverProviders;

  public DefaultResourceSpecResolver(Authentication systemAuthentication, EntityClient entityClient) {
    _resourceFieldResolverProviders =
        ImmutableList.of(new EntityTypeFieldResolverProvider(), new EntityUrnFieldResolverProvider(),
            new DomainFieldResolverProvider(entityClient, systemAuthentication),
            new OwnerFieldResolverProvider(entityClient, systemAuthentication),
            new PlatformInstanceFieldResolverProvider(entityClient, systemAuthentication));
  }

  @Override
  public ResolvedResourceSpec resolve(ResourceSpec resourceSpec) {
    return new ResolvedResourceSpec(resourceSpec, getFieldResolvers(resourceSpec));
  }

  private Map<ResourceFieldType, FieldResolver> getFieldResolvers(ResourceSpec resourceSpec) {
    return _resourceFieldResolverProviders.stream()
        .collect(Collectors.toMap(ResourceFieldResolverProvider::getFieldType,
            hydrator -> hydrator.getFieldResolver(resourceSpec)));
  }
}
