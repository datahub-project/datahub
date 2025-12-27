package com.linkedin.datahub.graphql.types.organization.mappers;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.canView;
import static com.linkedin.metadata.Constants.FORMS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTIES_ASPECT_NAME;

import com.linkedin.common.Forms;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.Ownership;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.Organization;
import com.linkedin.datahub.graphql.types.common.mappers.InstitutionalMemoryMapper;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.datahub.graphql.types.form.FormsMapper;
import com.linkedin.datahub.graphql.types.structuredproperty.StructuredPropertiesMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.OrganizationKey;
import com.linkedin.structured.StructuredProperties;
import javax.annotation.Nullable;

public class OrganizationMapper {

  public static Organization map(
      @Nullable QueryContext context, final EntityResponse entityResponse) {
    final Organization result = new Organization();
    final Urn entityUrn = entityResponse.getUrn();
    final EnvelopedAspectMap aspects = entityResponse.getAspects();

    result.setUrn(entityUrn.toString());
    result.setType(EntityType.ORGANIZATION);

    // Organizations MUST have key aspect to be rendered.
    final EnvelopedAspect envelopedOrganizationKey =
        aspects.get(Constants.ORGANIZATION_KEY_ASPECT_NAME);
    if (envelopedOrganizationKey != null) {
      result.setId(new OrganizationKey(envelopedOrganizationKey.getValue().data()).getId());
    } else {
      return null;
    }

    final EnvelopedAspect envelopedOrganizationProperties =
        aspects.get(Constants.ORGANIZATION_PROPERTIES_ASPECT_NAME);
    if (envelopedOrganizationProperties != null) {
      result.setProperties(
          OrganizationPropertiesMapper.map(
              context,
              new com.linkedin.organization.OrganizationProperties(
                  envelopedOrganizationProperties.getValue().data())));
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

    if (context != null && !canView(context.getOperationContext(), entityUrn)) {
      return AuthorizationUtils.restrictEntity(result, Organization.class);
    } else {
      return result;
    }
  }

  private OrganizationMapper() {}
}
