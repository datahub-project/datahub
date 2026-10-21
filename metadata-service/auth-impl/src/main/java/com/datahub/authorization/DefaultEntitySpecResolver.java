package com.datahub.authorization;

import static com.linkedin.metadata.Constants.CONTAINER_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATA_PLATFORM_INSTANCE_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DOMAIN_ENTITY_NAME;
import static com.linkedin.metadata.Constants.GLOSSARY_NODE_ENTITY_NAME;
import static com.linkedin.metadata.Constants.GLOSSARY_TERM_ENTITY_NAME;

import com.datahub.authentication.group.GroupService;
import com.datahub.authorization.fieldresolverprovider.*;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class DefaultEntitySpecResolver implements ContextualEntitySpecResolver {
  private static final Map<String, EntityFieldType> SELF_REFERENTIAL_FIELDS =
      Map.of(
          DOMAIN_ENTITY_NAME, EntityFieldType.DOMAIN,
          CONTAINER_ENTITY_NAME, EntityFieldType.CONTAINER,
          GLOSSARY_TERM_ENTITY_NAME, EntityFieldType.GLOSSARY,
          GLOSSARY_NODE_ENTITY_NAME, EntityFieldType.GLOSSARY,
          DATA_PLATFORM_INSTANCE_ENTITY_NAME, EntityFieldType.DATA_PLATFORM_INSTANCE);

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
            new GlossaryFieldResolverProvider(entityClient),
            new StructuredPropertyFieldResolverProvider(entityClient));
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

  /**
   * Resolves a resource that does not exist: every field holds what the providers resolve for an
   * urn with nothing stored, which is the urn and type, plus the urn itself for an entity that is
   * its own domain, container, glossary node or term, or platform instance. Reads nothing.
   */
  @Nonnull
  public static ResolvedEntitySpec resolveMissing(@Nonnull EntitySpec entitySpec) {
    final Map<EntityFieldType, FieldResolver> fieldResolvers = new HashMap<>();
    final Set<String> type = Collections.singleton(entitySpec.getType());
    fieldResolvers.put(EntityFieldType.TYPE, FieldResolver.getResolverFromValues(type));
    fieldResolvers.put(EntityFieldType.RESOURCE_TYPE, FieldResolver.getResolverFromValues(type));
    if (!entitySpec.getEntity().isEmpty()) {
      final Set<String> urn = Collections.singleton(entitySpec.getEntity());
      fieldResolvers.put(EntityFieldType.URN, FieldResolver.getResolverFromValues(urn));
      fieldResolvers.put(EntityFieldType.RESOURCE_URN, FieldResolver.getResolverFromValues(urn));
      final EntityFieldType selfField = selfReferentialField(entitySpec.getEntity());
      if (selfField != null) {
        fieldResolvers.put(selfField, FieldResolver.getResolverFromValues(urn));
      }
    }
    return new ResolvedEntitySpec(entitySpec, fieldResolvers);
  }

  @Nullable
  private static EntityFieldType selfReferentialField(@Nonnull String entityUrn) {
    try {
      return SELF_REFERENTIAL_FIELDS.get(Urn.createFromString(entityUrn).getEntityType());
    } catch (URISyntaxException e) {
      return null;
    }
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
