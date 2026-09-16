package com.datahub.authorization;

import com.datahub.authentication.group.GroupService;
import com.datahub.authorization.fieldresolverprovider.*;
import com.google.common.collect.ImmutableList;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class DefaultEntitySpecResolver implements ContextualEntitySpecResolver {
  private final List<EntityFieldResolverProvider> _entityFieldResolverProviders;
  private final OperationContext systemOperationContext;

  public DefaultEntitySpecResolver(
      @Nonnull OperationContext systemOperationContext,
      SystemEntityClient entityClient,
      GroupService groupService) {
    _entityFieldResolverProviders =
        ImmutableList.of(
            new EntityTypeFieldResolverProvider(),
            new EntityUrnFieldResolverProvider(),
            new DomainFieldResolverProvider(entityClient),
            new OwnerFieldResolverProvider(entityClient),
            new DataPlatformInstanceFieldResolverProvider(entityClient),
            new GroupMembershipFieldResolverProvider(groupService),
            new TagFieldResolverProvider(entityClient),
            new ContainerFieldResolverProvider(entityClient),
            new GlossaryFieldResolverProvider(entityClient));
    this.systemOperationContext = systemOperationContext;
  }

  @Override
  public ResolvedEntitySpec resolve(EntitySpec entitySpec) {
    return resolve(entitySpec, systemOperationContext);
  }

  @Override
  @Nonnull
  public ResolvedEntitySpec resolve(
      @Nonnull EntitySpec entitySpec, @Nonnull OperationContext opContext) {
    return new ResolvedEntitySpec(entitySpec, getFieldResolvers(opContext, entitySpec));
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
