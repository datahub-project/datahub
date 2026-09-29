package com.datahub.authorization.fieldresolverprovider;

import static com.linkedin.metadata.Constants.*;

import com.datahub.authorization.EntityFieldType;
import com.datahub.authorization.EntitySpec;
import com.datahub.authorization.FieldResolver;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.domain.Domains;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.graph.cache.client.BoundHierarchyAccess;
import com.linkedin.metadata.graph.cache.client.HierarchyBindings;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/** Provides field resolver for domain given entitySpec */
@Slf4j
public class DomainFieldResolverProvider implements EntityFieldResolverProvider {

  private final SystemEntityClient _entityClient;

  public DomainFieldResolverProvider(SystemEntityClient entityClient) {
    this._entityClient = entityClient;
  }

  @Override
  public List<EntityFieldType> getFieldTypes() {
    return Collections.singletonList(EntityFieldType.DOMAIN);
  }

  @Override
  public FieldResolver getFieldResolver(
      @Nonnull OperationContext opContext, EntitySpec entitySpec) {
    return FieldResolver.getResolverFromFunction(entitySpec, spec -> getDomains(opContext, spec));
  }

  private FieldResolver.FieldValue getDomains(
      @Nonnull OperationContext opContext, EntitySpec entitySpec) {

    try {
      if (entitySpec.getEntity().isEmpty()) {
        return FieldResolver.emptyFieldValue();
      }

      final Urn entityUrn = UrnUtils.getUrn(entitySpec.getEntity());

      if (entityUrn.getEntityType().equals(DOMAIN_ENTITY_NAME)) {
        final Set<Urn> domainsWithParents =
            BoundHierarchyAccess.expandAncestors(
                opContext,
                HierarchyBindings.domainSpec(opContext),
                Collections.singleton(entityUrn));
        return FieldResolver.FieldValue.builder()
            .values(domainsWithParents.stream().map(Object::toString).collect(Collectors.toSet()))
            .build();
      }

      EntityResponse response =
          _entityClient.getV2(
              opContext,
              entityUrn.getEntityType(),
              entityUrn,
              Collections.singleton(DOMAINS_ASPECT_NAME));
      if (response == null || !response.getAspects().containsKey(DOMAINS_ASPECT_NAME)) {
        return FieldResolver.emptyFieldValue();
      }

      final EnvelopedAspect domainsAspect = response.getAspects().get(DOMAINS_ASPECT_NAME);
      final Set<Urn> initialDomains =
          new HashSet<>(new Domains(domainsAspect.getValue().data()).getDomains());

      final Set<Urn> domainsWithParents =
          BoundHierarchyAccess.expandAncestors(
              opContext, HierarchyBindings.domainSpec(opContext), initialDomains);

      return FieldResolver.FieldValue.builder()
          .values(domainsWithParents.stream().map(Object::toString).collect(Collectors.toSet()))
          .build();
    } catch (Exception e) {
      log.error("Error while retrieving domains aspect for entitySpec {}", entitySpec, e);
      return FieldResolver.emptyFieldValue();
    }
  }
}
