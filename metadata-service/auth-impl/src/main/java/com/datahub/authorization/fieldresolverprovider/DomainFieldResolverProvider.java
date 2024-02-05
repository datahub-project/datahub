package com.datahub.authorization.fieldresolverprovider;

import static com.linkedin.metadata.Constants.*;

import com.datahub.authentication.Authentication;
import com.datahub.authorization.EntityFieldType;
import com.datahub.authorization.EntitySpec;
import com.datahub.authorization.FieldResolver;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.domain.DomainProperties;
import com.linkedin.domain.Domains;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.client.EntityClient;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/** Provides field resolver for domain given entitySpec */
@Slf4j
@RequiredArgsConstructor
public class DomainFieldResolverProvider implements EntityFieldResolverProvider {

  private final EntityClient _entityClient;
  private final Authentication _systemAuthentication;

  @Override
  public List<EntityFieldType> getFieldTypes() {
    return Collections.singletonList(EntityFieldType.DOMAIN);
  }

  @Override
  public FieldResolver getFieldResolver(EntitySpec entitySpec) {
    return FieldResolver.getResolverFromFunction(entitySpec, this::getDomains);
  }

  private Set<Urn> getBatchedParentDomains(@Nonnull final Set<Urn> urns) {
    final Set<Urn> parentUrns = new HashSet<>();

    try {
      final Map<Urn, EntityResponse> batchResponse =
          _entityClient.batchGetV2(
              DOMAIN_ENTITY_NAME,
              urns,
              Collections.singleton(DOMAIN_PROPERTIES_ASPECT_NAME),
              _systemAuthentication);

      batchResponse.forEach(
          (urn, entityResponse) -> {
            if (entityResponse.getAspects().containsKey(DOMAIN_PROPERTIES_ASPECT_NAME)) {
              final DomainProperties properties =
                  new DomainProperties(
                      entityResponse
                          .getAspects()
                          .get(DOMAIN_PROPERTIES_ASPECT_NAME)
                          .getValue()
                          .data());
              if (properties.hasParentDomain()) {
                parentUrns.add(properties.getParentDomain());
              }
            }
          });

    } catch (Exception e) {
      log.error(
          "Error while retrieving parent domains for {} urns including \"{}\"",
          urns.size(),
          urns.stream().findFirst().map(Urn::toString).orElse(""),
          e);
    }

    return parentUrns;
  }

  private FieldResolver.FieldValue getDomains(EntitySpec entitySpec) {
    final Urn entityUrn = UrnUtils.getUrn(entitySpec.getEntity());
    // In the case that the entity is a domain, the associated domain is the domain itself
    if (entityUrn.getEntityType().equals(DOMAIN_ENTITY_NAME)) {
      return FieldResolver.FieldValue.builder()
          .values(Collections.singleton(entityUrn.toString()))
          .build();
    }

    final EnvelopedAspect domainsAspect;
    try {
      EntityResponse response =
          _entityClient.getV2(
              entityUrn.getEntityType(),
              entityUrn,
              Collections.singleton(DOMAINS_ASPECT_NAME),
              _systemAuthentication);
      if (response == null || !response.getAspects().containsKey(DOMAINS_ASPECT_NAME)) {
        return FieldResolver.emptyFieldValue();
      }
      domainsAspect = response.getAspects().get(DOMAINS_ASPECT_NAME);
    } catch (Exception e) {
      log.error("Error while retrieving domains aspect for urn {}", entityUrn, e);
      return FieldResolver.emptyFieldValue();
    }

    /*
     * Build up a set of all directly referenced domains and any of the domains' parent domains.
     * To avoid cycles we remove any parents we've already visited to prevent an infinite loop cycle.
     */

    final Set<Urn> domainUrns =
        new HashSet<>(new Domains(domainsAspect.getValue().data()).getDomains());
    Set<Urn> batchedParentUrns = getBatchedParentDomains(domainUrns);
    batchedParentUrns.removeAll(domainUrns);

    while (!batchedParentUrns.isEmpty()) {
      domainUrns.addAll(batchedParentUrns);
      batchedParentUrns = getBatchedParentDomains(batchedParentUrns);
      batchedParentUrns.removeAll(domainUrns);
    }

    return FieldResolver.FieldValue.builder()
        .values(domainUrns.stream().map(Object::toString).collect(Collectors.toSet()))
        .build();
  }
}
