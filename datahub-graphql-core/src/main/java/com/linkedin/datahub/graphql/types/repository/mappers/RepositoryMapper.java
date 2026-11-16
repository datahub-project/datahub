package com.linkedin.datahub.graphql.types.repository.mappers;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.canView;

import com.linkedin.common.BrowsePathsV2;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.Ownership;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.generated.DataPlatform;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.Repository;
import com.linkedin.datahub.graphql.generated.RepositoryProperties;
import com.linkedin.datahub.graphql.generated.RepositorySource;
import com.linkedin.datahub.graphql.types.common.mappers.AuditStampMapper;
import com.linkedin.datahub.graphql.types.common.mappers.BrowsePathsV2Mapper;
import com.linkedin.datahub.graphql.types.common.mappers.DataPlatformInstanceAspectMapper;
import com.linkedin.datahub.graphql.types.common.mappers.InstitutionalMemoryMapper;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.datahub.graphql.types.common.mappers.StatusMapper;
import com.linkedin.datahub.graphql.types.common.mappers.SubTypesMapper;
import com.linkedin.datahub.graphql.types.domain.DomainAssociationMapper;
import com.linkedin.datahub.graphql.types.glossary.mappers.GlossaryTermsMapper;
import com.linkedin.datahub.graphql.types.structuredproperty.StructuredPropertiesMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.GlobalTagsMapper;
import com.linkedin.domain.Domains;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Maps a GMS Repository entity response to the GraphQL Repository type. */
public class RepositoryMapper {

  /**
   * Maps a GMS entity response to a GraphQL Repository.
   *
   * @param context The query context
   * @param entityResponse The GMS entity response
   * @return The mapped Repository
   */
  @Nullable
  public static Repository map(
      @Nonnull final QueryContext context, @Nonnull final EntityResponse entityResponse) {
    final Repository result = new Repository();
    final Urn entityUrn = entityResponse.getUrn();
    final EnvelopedAspectMap aspects = entityResponse.getAspects();

    result.setUrn(entityUrn.toString());
    result.setType(EntityType.REPOSITORY);

    final EnvelopedAspect envelopedProperties =
        aspects.get(Constants.REPOSITORY_PROPERTIES_ASPECT_NAME);
    if (envelopedProperties != null) {
      result.setProperties(
          mapProperties(
              context,
              entityUrn,
              new com.linkedin.repository.RepositoryProperties(
                  envelopedProperties.getValue().data())));
    }

    final EnvelopedAspect envelopedSource = aspects.get(Constants.REPOSITORY_SOURCE_ASPECT_NAME);
    if (envelopedSource != null) {
      result.setSource(
          mapSource(
              new com.linkedin.repository.RepositorySource(envelopedSource.getValue().data())));
    }

    // forkOf is exposed as a Repository stub, hydrated by the GraphQL DataLoader.
    final EnvelopedAspect envelopedLineage = aspects.get(Constants.REPOSITORY_LINEAGE_ASPECT_NAME);
    if (envelopedLineage != null) {
      final com.linkedin.repository.RepositoryLineage lineage =
          new com.linkedin.repository.RepositoryLineage(envelopedLineage.getValue().data());
      if (lineage.hasForkOf()) {
        final Repository stub = new Repository();
        stub.setUrn(lineage.getForkOf().toString());
        stub.setType(EntityType.REPOSITORY);
        result.setForkOf(stub);
      }
    }

    final EnvelopedAspect envelopedSubTypes = aspects.get(Constants.SUB_TYPES_ASPECT_NAME);
    if (envelopedSubTypes != null) {
      result.setSubTypes(
          SubTypesMapper.map(
              context, new com.linkedin.common.SubTypes(envelopedSubTypes.getValue().data())));
    }

    final EnvelopedAspect envelopedPlatformInstance =
        aspects.get(Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME);
    if (envelopedPlatformInstance != null) {
      final DataMap data = envelopedPlatformInstance.getValue().data();
      final com.linkedin.common.DataPlatformInstance platformInstance =
          new com.linkedin.common.DataPlatformInstance(data);
      result.setPlatform(mapPlatform(platformInstance));
      result.setDataPlatformInstance(
          DataPlatformInstanceAspectMapper.map(context, platformInstance));
    }

    final EnvelopedAspect envelopedOwnership = aspects.get(Constants.OWNERSHIP_ASPECT_NAME);
    if (envelopedOwnership != null) {
      result.setOwnership(
          OwnershipMapper.map(
              context, new Ownership(envelopedOwnership.getValue().data()), entityUrn));
    }

    final EnvelopedAspect envelopedStatus = aspects.get(Constants.STATUS_ASPECT_NAME);
    if (envelopedStatus != null) {
      final Status status = new Status(envelopedStatus.getValue().data());
      result.setStatus(StatusMapper.map(context, status));
    }

    final EnvelopedAspect envelopedTags = aspects.get(Constants.GLOBAL_TAGS_ASPECT_NAME);
    if (envelopedTags != null) {
      result.setTags(
          GlobalTagsMapper.map(
              context, new GlobalTags(envelopedTags.getValue().data()), entityUrn));
    }

    final EnvelopedAspect envelopedGlossaryTerms =
        aspects.get(Constants.GLOSSARY_TERMS_ASPECT_NAME);
    if (envelopedGlossaryTerms != null) {
      result.setGlossaryTerms(
          GlossaryTermsMapper.map(
              context, new GlossaryTerms(envelopedGlossaryTerms.getValue().data()), entityUrn));
    }

    final EnvelopedAspect envelopedDomains = aspects.get(Constants.DOMAINS_ASPECT_NAME);
    if (envelopedDomains != null) {
      result.setDomain(
          DomainAssociationMapper.map(
              context, new Domains(envelopedDomains.getValue().data()), entityUrn.toString()));
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

    final EnvelopedAspect envelopedStructuredProps =
        aspects.get(Constants.STRUCTURED_PROPERTIES_ASPECT_NAME);
    if (envelopedStructuredProps != null) {
      result.setStructuredProperties(
          StructuredPropertiesMapper.map(
              context,
              new com.linkedin.structured.StructuredProperties(
                  envelopedStructuredProps.getValue().data()),
              entityUrn));
    }

    final EnvelopedAspect envelopedBrowsePathsV2 =
        aspects.get(Constants.BROWSE_PATHS_V2_ASPECT_NAME);
    if (envelopedBrowsePathsV2 != null) {
      result.setBrowsePathV2(
          BrowsePathsV2Mapper.map(
              context, new BrowsePathsV2(envelopedBrowsePathsV2.getValue().data())));
    }

    if (context != null && !canView(context.getOperationContext(), entityUrn)) {
      return AuthorizationUtils.restrictEntity(result, Repository.class);
    }
    return result;
  }

  private static RepositoryProperties mapProperties(
      @Nullable final QueryContext context,
      @Nonnull final Urn entityUrn,
      @Nonnull final com.linkedin.repository.RepositoryProperties gms) {
    final RepositoryProperties result = new RepositoryProperties();
    result.setName(gms.getName());
    if (gms.hasDescription()) {
      result.setDescription(gms.getDescription());
    }
    if (gms.hasDefaultBranch()) {
      result.setDefaultBranch(gms.getDefaultBranch());
    }
    if (gms.hasLanguages()) {
      result.setLanguages(new java.util.ArrayList<>(gms.getLanguages()));
    }
    if (gms.hasLicense()) {
      result.setLicense(gms.getLicense());
    }
    if (gms.hasHomepageUrl()) {
      result.setHomepageUrl(gms.getHomepageUrl());
    }
    if (gms.hasArchived()) {
      result.setArchived(gms.isArchived());
    }
    if (gms.hasCreated()) {
      result.setCreated(AuditStampMapper.map(context, gms.getCreated()));
    }
    if (gms.hasLastModified()) {
      result.setLastModified(AuditStampMapper.map(context, gms.getLastModified()));
    }
    return result;
  }

  private static RepositorySource mapSource(
      @Nonnull final com.linkedin.repository.RepositorySource gms) {
    final RepositorySource result = new RepositorySource();
    if (gms.hasExternalUrl()) {
      result.setExternalUrl(gms.getExternalUrl());
    }
    if (gms.hasExternalId()) {
      result.setExternalId(gms.getExternalId());
    }
    return result;
  }

  private static DataPlatform mapPlatform(
      @Nonnull final com.linkedin.common.DataPlatformInstance platformInstance) {
    final DataPlatform partialPlatform = new DataPlatform();
    partialPlatform.setUrn(platformInstance.getPlatform().toString());
    return partialPlatform;
  }

  private RepositoryMapper() {}
}
