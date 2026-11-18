package com.datahub.authorization;

import com.datahub.authentication.group.GroupService;
import com.datahub.authorization.fieldresolverprovider.*;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DefaultEntitySpecResolver implements ContextualEntitySpecResolver {
  private final List<EntityFieldResolverProvider> _entityFieldResolverProviders;
  private final OperationContext systemOperationContext;
  private final SystemEntityClient entityClient;

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
            new GlossaryFieldResolverProvider(entityClient),
            new StructuredPropertyFieldResolverProvider(entityClient));
    this.systemOperationContext = systemOperationContext;
    this.entityClient = entityClient;
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

  /**
   * Reads every aspect the field providers need with one batch read per entity type, instead of one
   * read per provider and entity.
   */
  @Override
  @Nonnull
  public Map<EntitySpec, ResolvedEntitySpec> resolveAll(
      @Nonnull Collection<EntitySpec> entitySpecs, @Nonnull OperationContext opContext) {
    final Set<String> aspectNames =
        _entityFieldResolverProviders.stream()
            .flatMap(provider -> provider.getAspectNames().stream())
            .collect(Collectors.toSet());
    final Map<EntitySpec, Urn> urns = new HashMap<>();
    final Map<EntitySpec, ResolvedEntitySpec> resolved = new HashMap<>();
    for (EntitySpec spec : entitySpecs) {
      final Urn urn = spec.getEntity().isEmpty() ? null : UrnUtils.getUrn(spec.getEntity());
      if (urn == null) {
        resolved.put(spec, resolve(spec, opContext));
      } else {
        urns.put(spec, urn);
      }
    }

    final Map<Urn, EntityResponse> responses = new HashMap<>();
    final Set<String> fetchedTypes = new HashSet<>();
    urns.values().stream()
        .collect(Collectors.groupingBy(Urn::getEntityType, Collectors.toSet()))
        .forEach(
            (entityType, entityUrns) -> {
              try {
                final Set<String> entityAspectNames =
                    aspectNames.stream()
                        .filter(
                            opContext
                                    .getEntityRegistry()
                                    .getEntitySpec(entityType)
                                    .getAspectSpecMap()
                                ::containsKey)
                        .collect(Collectors.toSet());
                if (!entityAspectNames.isEmpty()) {
                  responses.putAll(
                      entityClient.batchGetV2(
                          opContext, entityType, entityUrns, entityAspectNames));
                }
                fetchedTypes.add(entityType);
              } catch (Exception e) {
                log.warn("Failed to prefetch policy fields of {} entities", entityType, e);
              }
            });

    urns.forEach(
        (spec, urn) ->
            resolved.put(
                spec,
                fetchedTypes.contains(urn.getEntityType())
                    ? new ResolvedEntitySpec(
                        spec,
                        getFieldResolvers(
                            opContext, spec, responses.getOrDefault(urn, emptyResponse(urn))))
                    : resolve(spec, opContext)));
    return resolved;
  }

  private static EntityResponse emptyResponse(@Nonnull Urn urn) {
    return new EntityResponse()
        .setEntityName(urn.getEntityType())
        .setUrn(urn)
        .setAspects(new EnvelopedAspectMap());
  }

  private Map<EntityFieldType, FieldResolver> getFieldResolvers(
      @Nonnull OperationContext opContext,
      EntitySpec entitySpec,
      @Nonnull EntityResponse prefetched) {
    return _entityFieldResolverProviders.stream()
        .flatMap(
            resolver ->
                resolver.getFieldTypes().stream().map(fieldType -> Pair.of(fieldType, resolver)))
        .collect(
            Collectors.toMap(
                Pair::getKey,
                pair -> pair.getValue().getFieldResolver(opContext, entitySpec, prefetched)));
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
