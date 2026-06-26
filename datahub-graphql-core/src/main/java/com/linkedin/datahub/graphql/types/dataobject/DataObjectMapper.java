package com.linkedin.datahub.graphql.types.dataobject;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.canView;

import com.linkedin.application.Applications;
import com.linkedin.common.BrowsePathsV2;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.Deprecation;
import com.linkedin.common.Documentation;
import com.linkedin.common.Forms;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.Ownership;
import com.linkedin.common.Status;
import com.linkedin.common.SubTypes;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AuditStamp;
import com.linkedin.datahub.graphql.generated.Container;
import com.linkedin.datahub.graphql.generated.DataObject;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.application.ApplicationAssociationMapper;
import com.linkedin.datahub.graphql.types.common.mappers.BrowsePathsV2Mapper;
import com.linkedin.datahub.graphql.types.common.mappers.CustomPropertiesMapper;
import com.linkedin.datahub.graphql.types.common.mappers.DataPlatformInstanceAspectMapper;
import com.linkedin.datahub.graphql.types.common.mappers.DeprecationMapper;
import com.linkedin.datahub.graphql.types.common.mappers.DocumentationMapper;
import com.linkedin.datahub.graphql.types.common.mappers.InstitutionalMemoryMapper;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.datahub.graphql.types.common.mappers.SubTypesMapper;
import com.linkedin.datahub.graphql.types.domain.DomainAssociationMapper;
import com.linkedin.datahub.graphql.types.form.FormsMapper;
import com.linkedin.datahub.graphql.types.glossary.mappers.GlossaryTermsMapper;
import com.linkedin.datahub.graphql.types.structuredproperty.StructuredPropertiesMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.GlobalTagsMapper;
import com.linkedin.dataobject.DataObjectProperties;
import com.linkedin.dataobject.ObjectStorageProperties;
import com.linkedin.dataobject.ParentDataObject;
import com.linkedin.domain.Domains;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.DataObjectKey;
import com.linkedin.structured.StructuredProperties;
import javax.annotation.Nullable;

/** Maps a GMS EntityResponse representing a Data Object to a GraphQL DataObject object. */
public class DataObjectMapper {

  public static DataObject map(
      @Nullable QueryContext context, final EntityResponse entityResponse) {
    final DataObject result = new DataObject();
    final Urn entityUrn = entityResponse.getUrn();
    final EnvelopedAspectMap aspects = entityResponse.getAspects();

    result.setUrn(entityUrn.toString());
    result.setType(EntityType.DATA_OBJECT);

    // Map Data Object Key aspect (platform, name fallback, origin)
    final EnvelopedAspect envelopedKey = aspects.get(Constants.DATA_OBJECT_KEY_ASPECT_NAME);
    if (envelopedKey != null) {
      final DataObjectKey key = new DataObjectKey(envelopedKey.getValue().data());
      result.setName(key.getName());
      result.setPlatform(
          com.linkedin.datahub.graphql.generated.DataPlatform.builder()
              .setType(EntityType.DATA_PLATFORM)
              .setUrn(key.getPlatform().toString())
              .build());
    }

    // Map Data Object Properties aspect
    final EnvelopedAspect envelopedProperties =
        aspects.get(Constants.DATA_OBJECT_PROPERTIES_ASPECT_NAME);
    if (envelopedProperties != null) {
      final DataObjectProperties gmsProperties =
          new DataObjectProperties(envelopedProperties.getValue().data());
      result.setProperties(mapDataObjectProperties(gmsProperties, entityUrn, result.getName()));
      if (gmsProperties.getName() != null) {
        result.setName(gmsProperties.getName());
      }
    }

    // Map Object Storage Properties aspect
    final EnvelopedAspect envelopedStorage =
        aspects.get(Constants.OBJECT_STORAGE_PROPERTIES_ASPECT_NAME);
    if (envelopedStorage != null) {
      result.setStorage(
          mapObjectStorageProperties(
              new ObjectStorageProperties(envelopedStorage.getValue().data())));
    }

    // Map Parent Data Object aspect to a stub resolved lazily by the GraphQL parent resolver
    final EnvelopedAspect envelopedParent = aspects.get(Constants.PARENT_DATA_OBJECT_ASPECT_NAME);
    if (envelopedParent != null) {
      final ParentDataObject parent = new ParentDataObject(envelopedParent.getValue().data());
      if (parent.hasObject()) {
        final DataObject stubParent = new DataObject();
        stubParent.setUrn(parent.getObject().toString());
        stubParent.setType(EntityType.DATA_OBJECT);
        result.setParent(stubParent);
      }
    }

    // Map SubTypes aspect
    final EnvelopedAspect envelopedSubTypes = aspects.get(Constants.SUB_TYPES_ASPECT_NAME);
    if (envelopedSubTypes != null) {
      result.setSubTypes(
          SubTypesMapper.map(context, new SubTypes(envelopedSubTypes.getValue().data())));
    }

    // Map Container aspect to a stub resolved lazily by the GraphQL container resolver
    final EnvelopedAspect envelopedContainer = aspects.get(Constants.CONTAINER_ASPECT_NAME);
    if (envelopedContainer != null) {
      final com.linkedin.container.Container gmsContainer =
          new com.linkedin.container.Container(envelopedContainer.getValue().data());
      result.setContainer(
          Container.builder()
              .setType(EntityType.CONTAINER)
              .setUrn(gmsContainer.getContainer().toString())
              .build());
    }

    // Map DataPlatformInstance aspect
    final EnvelopedAspect envelopedDataPlatformInstance =
        aspects.get(Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME);
    if (envelopedDataPlatformInstance != null) {
      final DataPlatformInstance dataPlatformInstance =
          new DataPlatformInstance(envelopedDataPlatformInstance.getValue().data());
      if (dataPlatformInstance.hasInstance()) {
        result.setDataPlatformInstance(
            DataPlatformInstanceAspectMapper.map(context, dataPlatformInstance));
      }
    }

    // Map Ownership aspect
    final EnvelopedAspect envelopedOwnership = aspects.get(Constants.OWNERSHIP_ASPECT_NAME);
    if (envelopedOwnership != null) {
      result.setOwnership(
          OwnershipMapper.map(
              context, new Ownership(envelopedOwnership.getValue().data()), entityUrn));
    }

    // Map Global Tags aspect (and the deprecated globalTags alias for sidebar parity)
    final EnvelopedAspect envelopedGlobalTags = aspects.get(Constants.GLOBAL_TAGS_ASPECT_NAME);
    if (envelopedGlobalTags != null) {
      final com.linkedin.datahub.graphql.generated.GlobalTags mappedTags =
          GlobalTagsMapper.map(
              context, new GlobalTags(envelopedGlobalTags.getValue().data()), entityUrn);
      result.setTags(mappedTags);
      result.setGlobalTags(mappedTags);
    }

    // Map Glossary Terms aspect
    final EnvelopedAspect envelopedGlossaryTerms =
        aspects.get(Constants.GLOSSARY_TERMS_ASPECT_NAME);
    if (envelopedGlossaryTerms != null) {
      result.setGlossaryTerms(
          GlossaryTermsMapper.map(
              context, new GlossaryTerms(envelopedGlossaryTerms.getValue().data()), entityUrn));
    }

    // Map Domains aspect
    final EnvelopedAspect envelopedDomains = aspects.get(Constants.DOMAINS_ASPECT_NAME);
    if (envelopedDomains != null) {
      final Domains domains = new Domains(envelopedDomains.getValue().data());
      if (domains.hasDomains() && !domains.getDomains().isEmpty()) {
        result.setDomain(DomainAssociationMapper.map(context, domains, entityUrn.toString()));
      }
    }

    // Map Applications aspect (application membership)
    final EnvelopedAspect envelopedApplications =
        aspects.get(Constants.APPLICATION_MEMBERSHIP_ASPECT_NAME);
    if (envelopedApplications != null) {
      result.setApplications(
          ApplicationAssociationMapper.mapList(
              context,
              new Applications(envelopedApplications.getValue().data()),
              entityUrn.toString()));
    }

    // Map Status aspect for soft delete
    final EnvelopedAspect envelopedStatus = aspects.get(Constants.STATUS_ASPECT_NAME);
    if (envelopedStatus != null) {
      result.setExists(!new Status(envelopedStatus.getValue().data()).isRemoved());
    }

    // Map Deprecation aspect
    final EnvelopedAspect envelopedDeprecation = aspects.get(Constants.DEPRECATION_ASPECT_NAME);
    if (envelopedDeprecation != null) {
      result.setDeprecation(
          DeprecationMapper.map(context, new Deprecation(envelopedDeprecation.getValue().data())));
    }

    // Map Institutional Memory aspect (links)
    final EnvelopedAspect envelopedInstitutionalMemory =
        aspects.get(Constants.INSTITUTIONAL_MEMORY_ASPECT_NAME);
    if (envelopedInstitutionalMemory != null) {
      result.setInstitutionalMemory(
          InstitutionalMemoryMapper.map(
              context,
              new InstitutionalMemory(envelopedInstitutionalMemory.getValue().data()),
              entityUrn));
    }

    // Map Structured Properties aspect
    final EnvelopedAspect envelopedStructuredProps =
        aspects.get(Constants.STRUCTURED_PROPERTIES_ASPECT_NAME);
    if (envelopedStructuredProps != null) {
      result.setStructuredProperties(
          StructuredPropertiesMapper.map(
              context,
              new StructuredProperties(envelopedStructuredProps.getValue().data()),
              entityUrn));
    }

    // Map Forms aspect
    final EnvelopedAspect envelopedForms = aspects.get(Constants.FORMS_ASPECT_NAME);
    if (envelopedForms != null) {
      result.setForms(
          FormsMapper.map(new Forms(envelopedForms.getValue().data()), entityUrn.toString()));
    }

    // Map Documentation aspect
    final EnvelopedAspect envelopedDocumentation = aspects.get(Constants.DOCUMENTATION_ASPECT_NAME);
    if (envelopedDocumentation != null) {
      result.setDocumentation(
          DocumentationMapper.map(
              context, new Documentation(envelopedDocumentation.getValue().data())));
    }

    // Map Browse Paths V2 aspect
    final EnvelopedAspect envelopedBrowsePathsV2 =
        aspects.get(Constants.BROWSE_PATHS_V2_ASPECT_NAME);
    if (envelopedBrowsePathsV2 != null) {
      result.setBrowsePathV2(
          BrowsePathsV2Mapper.map(
              context, new BrowsePathsV2(envelopedBrowsePathsV2.getValue().data())));
    }

    if (context != null && !canView(context.getOperationContext(), entityUrn)) {
      return com.linkedin.datahub.graphql.authorization.AuthorizationUtils.restrictEntity(
          result, DataObject.class);
    } else {
      return result;
    }
  }

  /** Maps the DataObjectProperties PDL model to the GraphQL model. */
  private static com.linkedin.datahub.graphql.generated.DataObjectProperties
      mapDataObjectProperties(
          final DataObjectProperties gmsProperties,
          final Urn entityUrn,
          @Nullable final String nameFallback) {
    final com.linkedin.datahub.graphql.generated.DataObjectProperties result =
        new com.linkedin.datahub.graphql.generated.DataObjectProperties();

    if (gmsProperties.getName() != null) {
      result.setName(gmsProperties.getName());
    } else {
      result.setName(nameFallback);
    }
    result.setQualifiedName(gmsProperties.getQualifiedName());
    result.setDescription(gmsProperties.getDescription());
    if (gmsProperties.getExternalUrl() != null) {
      result.setExternalUrl(gmsProperties.getExternalUrl().toString());
    }
    result.setCustomProperties(
        CustomPropertiesMapper.map(gmsProperties.getCustomProperties(), entityUrn));

    final com.linkedin.common.AuditStamp created = gmsProperties.getCreated();
    if (created != null) {
      result.setCreated(
          new AuditStamp(
              created.getTime(),
              created.getActor() == null ? null : created.getActor().toString()));
    }
    final com.linkedin.common.AuditStamp lastModified = gmsProperties.getLastModified();
    if (lastModified != null) {
      result.setLastModified(
          new AuditStamp(
              lastModified.getTime(),
              lastModified.getActor() == null ? null : lastModified.getActor().toString()));
    }

    return result;
  }

  /** Maps the ObjectStorageProperties PDL model to the GraphQL model. */
  private static com.linkedin.datahub.graphql.generated.ObjectStorageProperties
      mapObjectStorageProperties(final ObjectStorageProperties gmsStorage) {
    final com.linkedin.datahub.graphql.generated.ObjectStorageProperties result =
        new com.linkedin.datahub.graphql.generated.ObjectStorageProperties();

    result.setMimeType(gmsStorage.getMimeType());
    result.setSizeBytes(gmsStorage.getSizeBytes());
    result.setStorageClass(gmsStorage.getStorageClass());
    result.setStoragePath(gmsStorage.getStoragePath());
    result.setEtag(gmsStorage.getEtag());
    result.setContentEncoding(gmsStorage.getContentEncoding());

    return result;
  }
}
