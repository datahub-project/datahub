package com.datahub.authorization;

import com.datahub.authorization.fieldresolverprovider.DataPlatformInstanceFieldResolverProvider;
import com.datahub.authorization.fieldresolverprovider.DomainFieldResolverProvider;
import com.datahub.authorization.fieldresolverprovider.EntityFieldResolverProvider;
import com.datahub.authorization.fieldresolverprovider.EntityTypeFieldResolverProvider;
import com.datahub.authorization.fieldresolverprovider.EntityUrnFieldResolverProvider;
import com.datahub.authorization.fieldresolverprovider.GroupMembershipFieldResolverProvider;
import com.datahub.authorization.fieldresolverprovider.OwnerFieldResolverProvider;
import com.datahub.authorization.fieldresolverprovider.TagFieldResolverProvider;
import com.google.common.collect.ImmutableList;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class DefaultEntitySpecResolver implements EntitySpecResolver {
  private final List<EntityFieldResolverProvider> _entityFieldResolverProviders;
  private final OperationContext systemOperationContext;

  public DefaultEntitySpecResolver(
      @Nonnull OperationContext systemOperationContext, SystemEntityClient entityClient) {
    _entityFieldResolverProviders =
        ImmutableList.of(
            new EntityTypeFieldResolverProvider(),
            new EntityUrnFieldResolverProvider(),
            new DomainFieldResolverProvider(entityClient),
            new OwnerFieldResolverProvider(entityClient),
            new DataPlatformInstanceFieldResolverProvider(entityClient),
            new GroupMembershipFieldResolverProvider(entityClient),
            new TagFieldResolverProvider(entityClient));
    this.systemOperationContext = systemOperationContext;
  }

  @Override
  public ResolvedEntitySpec resolve(EntitySpec entitySpec) {
    return new ResolvedEntitySpec(
        entitySpec, getFieldResolvers(systemOperationContext, entitySpec));
  }

  private Map<EntityFieldType, FieldResolver> getFieldResolvers(
      @Nonnull OperationContext opContext, EntitySpec entitySpec) {
    return _entityFieldResolverProviders.stream()
        .flatMap(
            resolver ->
                resolver.getFieldTypes().stream().map(fieldType -> Pair.of(fieldType, resolver)))
        .collect(
            Collectors.toMap(
                Pair::getKey, pair -> pair.getValue().getFieldResolver(opContext, entitySpec)));
  }
}
