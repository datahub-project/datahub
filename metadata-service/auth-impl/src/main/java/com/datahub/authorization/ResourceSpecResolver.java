package com.datahub.authorization;

import com.datahub.authentication.Authentication;
import com.datahub.authorization.fieldresolverprovider.DomainFieldResolverProvider;
import com.datahub.authorization.fieldresolverprovider.EntityTypeFieldResolverProvider;
import com.datahub.authorization.fieldresolverprovider.EntityUrnFieldResolverProvider;
import com.datahub.authorization.fieldresolverprovider.ResourceFieldType;
import com.datahub.authorization.fieldresolverprovider.OwnerFieldResolverProvider;
import com.datahub.authorization.fieldresolverprovider.ResourceFieldResolverProvider;
import com.google.common.collect.ImmutableList;
import com.linkedin.entity.client.EntityClient;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class ResourceSpecResolver {
  private final List<ResourceFieldResolverProvider> _resourceFieldResolverProviders;

  public ResourceSpecResolver(Authentication systemAuthentication, EntityClient entityClient) {
    _resourceFieldResolverProviders =
        ImmutableList.of(new EntityTypeFieldResolverProvider(), new EntityUrnFieldResolverProvider(),
            new DomainFieldResolverProvider(entityClient, systemAuthentication),
            new OwnerFieldResolverProvider(entityClient, systemAuthentication));
  }

  public ResolvedResourceSpec resolve(ResourceSpec resourceSpec) {
    return new ResolvedResourceSpec(resourceSpec, getFieldResolvers(resourceSpec));
  }

  public Map<ResourceFieldType, FieldResolver> getFieldResolvers(ResourceSpec resourceSpec) {
    return _resourceFieldResolverProviders.stream()
        .collect(Collectors.toMap(ResourceFieldResolverProvider::getFieldType,
            hydrator -> hydrator.getFieldResolver(resourceSpec)));
  }
}
