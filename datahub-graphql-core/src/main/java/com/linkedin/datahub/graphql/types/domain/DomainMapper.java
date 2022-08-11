package com.linkedin.datahub.graphql.types.domain;

import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.Ownership;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.Domain;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.common.mappers.InstitutionalMemoryMapper;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.domain.DomainProperties;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.DomainKey;


public class DomainMapper {

  public static Domain map(final EntityResponse entityResponse) {
    final Domain result = new Domain();
    final Urn entityUrn = entityResponse.getUrn();
    final EnvelopedAspectMap aspects = entityResponse.getAspects();

    result.setUrn(entityUrn.toString());
    result.setType(EntityType.DOMAIN);

    // Domains MUST have key aspect to be rendered.
    final EnvelopedAspect envelopedDomainKey = aspects.get(Constants.DOMAIN_KEY_ASPECT_NAME);
    if (envelopedDomainKey != null) {
      result.setId(new DomainKey(envelopedDomainKey.getValue().data()).getId());
    } else {
      return null;
    }

    final EnvelopedAspect envelopedDomainProperties = aspects.get(Constants.DOMAIN_PROPERTIES_ASPECT_NAME);
    if (envelopedDomainProperties != null) {
      result.setProperties(mapDomainProperties(new DomainProperties(envelopedDomainProperties.getValue().data())));
    }

    final EnvelopedAspect envelopedOwnership = aspects.get(Constants.OWNERSHIP_ASPECT_NAME);
    if (envelopedOwnership != null) {
      result.setOwnership(OwnershipMapper.map(new Ownership(envelopedOwnership.getValue().data()), entityUrn));
    }

    final EnvelopedAspect envelopedInstitutionalMemory = aspects.get(Constants.INSTITUTIONAL_MEMORY_ASPECT_NAME);
    if (envelopedInstitutionalMemory != null) {
      result.setInstitutionalMemory(InstitutionalMemoryMapper.map(new InstitutionalMemory(envelopedInstitutionalMemory.getValue().data())));
    }

    return result;
  }

  private static com.linkedin.datahub.graphql.generated.DomainProperties mapDomainProperties(final DomainProperties gmsProperties) {
    final com.linkedin.datahub.graphql.generated.DomainProperties propertiesResult = new com.linkedin.datahub.graphql.generated.DomainProperties();
    propertiesResult.setName(gmsProperties.getName());
    propertiesResult.setDescription(gmsProperties.getDescription());
    return propertiesResult;
  }

  private DomainMapper() { }
}
