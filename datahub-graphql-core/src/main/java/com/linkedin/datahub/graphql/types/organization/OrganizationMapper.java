package com.linkedin.datahub.graphql.types.organization;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.GlobalTags;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.Ownership;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.Organization;
import com.linkedin.datahub.graphql.types.common.mappers.InstitutionalMemoryMapper;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.datahub.graphql.types.glossary.mappers.GlossaryTermsMapper;
import com.linkedin.datahub.graphql.types.organization.mappers.OrganizationPropertiesMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.GlobalTagsMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.key.OrganizationKey;
import com.linkedin.organization.OrganizationProperties;
import javax.annotation.Nonnull;

/** Maps Pegasus {@link EntityResponse} objects to GraphQL {@link Organization} objects. */
public class OrganizationMapper {

  public static Organization map(
      @Nonnull QueryContext context, @Nonnull final EntityResponse entityResponse) {
    final Organization result = new Organization();
    final Urn entityUrn = entityResponse.getUrn();
    result.setUrn(entityUrn.toString());
    result.setType(EntityType.ORGANIZATION);

    final EnvelopedAspectMap aspects = entityResponse.getAspects();

    // Map key aspect
    final EnvelopedAspect envelopedOrganizationKey = aspects.get(ORGANIZATION_KEY_ASPECT_NAME);
    if (envelopedOrganizationKey != null) {
      result.setId(new OrganizationKey(envelopedOrganizationKey.getValue().data()).getId());
    }

    // Map properties aspect
    final EnvelopedAspect envelopedOrganizationProperties =
        aspects.get(ORGANIZATION_PROPERTIES_ASPECT_NAME);
    if (envelopedOrganizationProperties != null) {
      result.setProperties(
          OrganizationPropertiesMapper.map(
              context,
              new OrganizationProperties(envelopedOrganizationProperties.getValue().data())));
    }

    // Map ownership aspect
    final EnvelopedAspect envelopedOwnership = aspects.get(OWNERSHIP_ASPECT_NAME);
    if (envelopedOwnership != null) {
      result.setOwnership(
          OwnershipMapper.map(
              context, new Ownership(envelopedOwnership.getValue().data()), entityUrn));
    }

    // Map tags aspect
    final EnvelopedAspect envelopedGlobalTags = aspects.get(GLOBAL_TAGS_ASPECT_NAME);
    if (envelopedGlobalTags != null) {
      result.setTags(
          GlobalTagsMapper.map(
              context, new GlobalTags(envelopedGlobalTags.getValue().data()), entityUrn));
    }

    // Map glossary terms aspect
    final EnvelopedAspect envelopedGlossaryTerms = aspects.get(GLOSSARY_TERMS_ASPECT_NAME);
    if (envelopedGlossaryTerms != null) {
      result.setGlossaryTerms(
          GlossaryTermsMapper.map(
              context, new GlossaryTerms(envelopedGlossaryTerms.getValue().data()), entityUrn));
    }

    // Map institutional memory aspect
    final EnvelopedAspect envelopedInstitutionalMemory =
        aspects.get(INSTITUTIONAL_MEMORY_ASPECT_NAME);
    if (envelopedInstitutionalMemory != null) {
      result.setInstitutionalMemory(
          InstitutionalMemoryMapper.map(
              context,
              new InstitutionalMemory(envelopedInstitutionalMemory.getValue().data()),
              entityUrn));
    }

    return result;
  }

  private OrganizationMapper() {}
}
