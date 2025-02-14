package com.linkedin.datahub.graphql.types.domain;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.canView;
import static com.linkedin.metadata.Constants.FORMS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTIES_ASPECT_NAME;

import com.linkedin.common.DisplayProperties;
import com.linkedin.common.Forms;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.Ownership;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.generated.Domain;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.common.mappers.DisplayPropertiesMapper;
import com.linkedin.datahub.graphql.types.common.mappers.InstitutionalMemoryMapper;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.datahub.graphql.types.form.FormsMapper;
import com.linkedin.datahub.graphql.types.structuredproperty.StructuredPropertiesMapper;
import com.linkedin.domain.DomainProperties;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.DomainKey;
import com.linkedin.structured.StructuredProperties;
import javax.annotation.Nullable;

public class DomainMapper {

  public static Domain map(@Nullable QueryContext context, final EntityResponse entityResponse) {
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

    final EnvelopedAspect envelopedDomainProperties =
        aspects.get(Constants.DOMAIN_PROPERTIES_ASPECT_NAME);
    if (envelopedDomainProperties != null) {
      result.setProperties(
          mapDomainProperties(new DomainProperties(envelopedDomainProperties.getValue().data())));
    }

    final EnvelopedAspect envelopedOwnership = aspects.get(Constants.OWNERSHIP_ASPECT_NAME);
    if (envelopedOwnership != null) {
      result.setOwnership(
          OwnershipMapper.map(
              context, new Ownership(envelopedOwnership.getValue().data()), entityUrn));
    }

    final EnvelopedAspect envelopedInstitutionalMemory =
        aspects.get(Constants.INSTITUTIONAL_MEMORY_ASPECT_NAME);
    if (envelopedInstitutionalMemory != null) {
      result.setInstitutionalMemory(
          InstitutionalMemoryMapper.map(
              context,
              new InstitutionalMemory(envelopedInstitutionalMemory.getValue().data()),
              entityUrn));
    }

    final EnvelopedAspect envelopedStructuredProps = aspects.get(STRUCTURED_PROPERTIES_ASPECT_NAME);
    if (envelopedStructuredProps != null) {
      result.setStructuredProperties(
          StructuredPropertiesMapper.map(
              context,
              new StructuredProperties(envelopedStructuredProps.getValue().data()),
              entityUrn));
    }

    final EnvelopedAspect envelopedForms = aspects.get(FORMS_ASPECT_NAME);
    if (envelopedForms != null) {
      result.setForms(
          FormsMapper.map(new Forms(envelopedForms.getValue().data()), entityUrn.toString()));
    }

    final EnvelopedAspect envelopedDisplayProperties =
        aspects.get(Constants.DISPLAY_PROPERTIES_ASPECT_NAME);
    if (envelopedDisplayProperties != null) {
      result.setDisplayProperties(
          DisplayPropertiesMapper.map(
              context, new DisplayProperties(envelopedDisplayProperties.getValue().data())));
    }

    if (context != null && !canView(context.getOperationContext(), entityUrn)) {
      return AuthorizationUtils.restrictEntity(result, Domain.class);
    } else {
      return result;
    }
  }

  private static com.linkedin.datahub.graphql.generated.DomainProperties mapDomainProperties(
      final DomainProperties gmsProperties) {
    final com.linkedin.datahub.graphql.generated.DomainProperties propertiesResult =
        new com.linkedin.datahub.graphql.generated.DomainProperties();
    propertiesResult.setName(gmsProperties.getName());
    propertiesResult.setDescription(gmsProperties.getDescription());
    return propertiesResult;
  }

  private DomainMapper() {}
}
