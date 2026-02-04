package com.datahub.authorization.fieldresolverprovider;

import static com.linkedin.metadata.Constants.*;

import com.datahub.authorization.EntityFieldType;
import com.datahub.authorization.EntitySpec;
import com.datahub.authorization.FieldResolver;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.domain.DomainProperties;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.SystemEntityClient;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Provides field resolver for parent domain hierarchy given a domain entitySpec. This resolver
 * returns all parent domains up the hierarchy chain.
 */
@Slf4j
@RequiredArgsConstructor
public class ParentDomainFieldResolverProvider implements EntityFieldResolverProvider {

  private final SystemEntityClient _entityClient;

  @Override
  public List<EntityFieldType> getFieldTypes() {
    return Collections.singletonList(EntityFieldType.PARENT_DOMAIN);
  }

  @Override
  public FieldResolver getFieldResolver(
      @Nonnull OperationContext opContext, EntitySpec entitySpec) {
    return FieldResolver.getResolverFromFunction(
        entitySpec, spec -> getParentDomains(opContext, spec));
  }

  private FieldResolver.FieldValue getParentDomains(
      @Nonnull OperationContext opContext, EntitySpec entitySpec) {

    try {
      if (entitySpec.getEntity().isEmpty()) {
        return FieldResolver.emptyFieldValue();
      }

      final Urn entityUrn = UrnUtils.getUrn(entitySpec.getEntity());

      if (!entityUrn.getEntityType().equals(DOMAIN_ENTITY_NAME)) {
        return FieldResolver.emptyFieldValue();
      }

      return buildFieldValue(collectParentDomainUrns(opContext, entityUrn));

    } catch (Exception e) {
      log.error("Error while retrieving parent domains for entitySpec {}", entitySpec, e);
      return FieldResolver.emptyFieldValue();
    }
  }

  private Set<Urn> collectParentDomainUrns(
      @Nonnull OperationContext opContext, @Nonnull Urn startUrn) {
    Set<Urn> parentDomainUrns = new LinkedHashSet<>();
    Urn currentUrn = startUrn;

    while (currentUrn != null) {
      Urn parentUrn = getParentDomainUrn(opContext, currentUrn);

      if (parentUrn == null) {
        break;
      }

      if (parentDomainUrns.contains(parentUrn)) {
        log.warn("Cycle detected in domain hierarchy at {}", parentUrn);
        break;
      }

      parentDomainUrns.add(parentUrn);
      currentUrn = parentUrn;
    }

    return parentDomainUrns;
  }

  private Urn getParentDomainUrn(@Nonnull OperationContext opContext, @Nonnull Urn domainUrn) {
    try {
      EntityResponse response =
          _entityClient.getV2(
              opContext,
              DOMAIN_ENTITY_NAME,
              domainUrn,
              Collections.singleton(DOMAIN_PROPERTIES_ASPECT_NAME));

      if (response == null || !response.getAspects().containsKey(DOMAIN_PROPERTIES_ASPECT_NAME)) {
        return null;
      }

      DomainProperties properties =
          new DomainProperties(
              response.getAspects().get(DOMAIN_PROPERTIES_ASPECT_NAME).getValue().data());

      return properties.hasParentDomain() ? properties.getParentDomain() : null;
    } catch (Exception e) {
      log.error("Error retrieving parent domain for {}", domainUrn, e);
      return null;
    }
  }

  private FieldResolver.FieldValue buildFieldValue(Set<Urn> urns) {
    return FieldResolver.FieldValue.builder()
        .values(
            urns.stream()
                .map(Object::toString)
                .collect(Collectors.toCollection(LinkedHashSet::new)))
        .build();
  }
}
