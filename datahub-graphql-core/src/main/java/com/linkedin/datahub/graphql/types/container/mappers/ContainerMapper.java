package com.linkedin.datahub.graphql.types.container.mappers;

import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.Deprecation;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.Ownership;
import com.linkedin.common.SubTypes;
import com.linkedin.common.urn.Urn;
import com.linkedin.container.ContainerProperties;
import com.linkedin.container.EditableContainerProperties;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.generated.Container;
import com.linkedin.datahub.graphql.generated.DataPlatform;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.common.mappers.DataPlatformInstanceAspectMapper;
import com.linkedin.datahub.graphql.types.common.mappers.DeprecationMapper;
import com.linkedin.datahub.graphql.types.common.mappers.InstitutionalMemoryMapper;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.datahub.graphql.types.common.mappers.CustomPropertiesMapper;
import com.linkedin.datahub.graphql.types.common.mappers.util.SystemMetadataUtils;
import com.linkedin.datahub.graphql.types.domain.DomainAssociationMapper;
import com.linkedin.datahub.graphql.types.glossary.mappers.GlossaryTermsMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.GlobalTagsMapper;
import com.linkedin.domain.Domains;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import javax.annotation.Nullable;

import static com.linkedin.metadata.Constants.*;


public class ContainerMapper {

  @Nullable
  public static Container map(final EntityResponse entityResponse) {
    final Container result = new Container();
    final Urn entityUrn = entityResponse.getUrn();
    final EnvelopedAspectMap aspects = entityResponse.getAspects();
    Long lastIngested = SystemMetadataUtils.getLastIngested(aspects);
    result.setLastIngested(lastIngested);

    result.setUrn(entityUrn.toString());
    result.setType(EntityType.CONTAINER);

    final EnvelopedAspect envelopedPlatformInstance = aspects.get(Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME);
    if (envelopedPlatformInstance != null) {
      final DataMap data = envelopedPlatformInstance.getValue().data();
      result.setPlatform(mapPlatform(new DataPlatformInstance(data)));
      result.setDataPlatformInstance(DataPlatformInstanceAspectMapper.map(new DataPlatformInstance(data)));
    } else {
      final DataPlatform unknownPlatform = new DataPlatform();
      unknownPlatform.setUrn(UNKNOWN_DATA_PLATFORM);
      result.setPlatform(unknownPlatform);
    }

    final EnvelopedAspect envelopedContainerProperties = aspects.get(Constants.CONTAINER_PROPERTIES_ASPECT_NAME);
    if (envelopedContainerProperties != null) {
      result.setProperties(mapContainerProperties(new ContainerProperties(envelopedContainerProperties.getValue().data()), entityUrn));
    }

    final EnvelopedAspect envelopedEditableContainerProperties = aspects.get(Constants.CONTAINER_EDITABLE_PROPERTIES_ASPECT_NAME);
    if (envelopedEditableContainerProperties != null) {
      result.setEditableProperties(mapContainerEditableProperties(new EditableContainerProperties(envelopedEditableContainerProperties.getValue().data())));
    }

    final EnvelopedAspect envelopedOwnership = aspects.get(Constants.OWNERSHIP_ASPECT_NAME);
    if (envelopedOwnership != null) {
      result.setOwnership(OwnershipMapper.map(new Ownership(envelopedOwnership.getValue().data()), entityUrn));
    }

    final EnvelopedAspect envelopedTags = aspects.get(Constants.GLOBAL_TAGS_ASPECT_NAME);
    if (envelopedTags != null) {
      com.linkedin.datahub.graphql.generated.GlobalTags globalTags = GlobalTagsMapper.map(new GlobalTags(envelopedTags.getValue().data()), entityUrn);
      result.setTags(globalTags);
    }

    final EnvelopedAspect envelopedTerms = aspects.get(Constants.GLOSSARY_TERMS_ASPECT_NAME);
    if (envelopedTerms != null) {
      result.setGlossaryTerms(GlossaryTermsMapper.map(new GlossaryTerms(envelopedTerms.getValue().data()), entityUrn));
    }

    final EnvelopedAspect envelopedInstitutionalMemory = aspects.get(Constants.INSTITUTIONAL_MEMORY_ASPECT_NAME);
    if (envelopedInstitutionalMemory != null) {
      result.setInstitutionalMemory(InstitutionalMemoryMapper.map(new InstitutionalMemory(envelopedInstitutionalMemory.getValue().data())));
    }

    final EnvelopedAspect envelopedSubTypes = aspects.get(Constants.SUB_TYPES_ASPECT_NAME);
    if (envelopedSubTypes != null) {
      result.setSubTypes(mapSubTypes(new SubTypes(envelopedSubTypes.getValue().data())));
    }

    final EnvelopedAspect envelopedContainer = aspects.get(Constants.CONTAINER_ASPECT_NAME);
    if (envelopedContainer != null) {
      final com.linkedin.container.Container gmsContainer = new com.linkedin.container.Container(envelopedContainer.getValue().data());
      result.setContainer(Container
          .builder()
          .setType(EntityType.CONTAINER)
          .setUrn(gmsContainer.getContainer().toString())
          .build());
    }

    final EnvelopedAspect envelopedDomains = aspects.get(Constants.DOMAINS_ASPECT_NAME);
    if (envelopedDomains != null) {
      final Domains domains = new Domains(envelopedDomains.getValue().data());
      // Currently we only take the first domain if it exists.
      result.setDomain(DomainAssociationMapper.map(domains, entityUrn.toString()));
    }

    final EnvelopedAspect envelopedDeprecation = aspects.get(Constants.DEPRECATION_ASPECT_NAME);
    if (envelopedDeprecation != null) {
      result.setDeprecation(DeprecationMapper.map(new Deprecation(envelopedDeprecation.getValue().data())));
    }

    return result;
  }

  private static com.linkedin.datahub.graphql.generated.ContainerProperties mapContainerProperties(final ContainerProperties gmsProperties, Urn entityUrn) {
    final com.linkedin.datahub.graphql.generated.ContainerProperties propertiesResult = new com.linkedin.datahub.graphql.generated.ContainerProperties();
    propertiesResult.setName(gmsProperties.getName());
    propertiesResult.setDescription(gmsProperties.getDescription());
    if (gmsProperties.hasExternalUrl()) {
      propertiesResult.setExternalUrl(gmsProperties.getExternalUrl().toString());
    }
    if (gmsProperties.hasCustomProperties()) {
      propertiesResult.setCustomProperties(CustomPropertiesMapper.map(gmsProperties.getCustomProperties(), entityUrn));
    }
    return propertiesResult;
  }

  private static com.linkedin.datahub.graphql.generated.ContainerEditableProperties mapContainerEditableProperties(
      final EditableContainerProperties gmsProperties) {
    final com.linkedin.datahub.graphql.generated.ContainerEditableProperties editableContainerProperties =
        new com.linkedin.datahub.graphql.generated.ContainerEditableProperties();
    editableContainerProperties.setDescription(gmsProperties.getDescription());
    return editableContainerProperties;
  }

  private static com.linkedin.datahub.graphql.generated.SubTypes mapSubTypes(final SubTypes gmsSubTypes) {
    final com.linkedin.datahub.graphql.generated.SubTypes subTypes = new com.linkedin.datahub.graphql.generated.SubTypes();
    subTypes.setTypeNames(gmsSubTypes.getTypeNames());
    return subTypes;
  }

  private static DataPlatform mapPlatform(final DataPlatformInstance platformInstance) {
    // Set dummy platform to be resolved.
    final DataPlatform dummyPlatform = new DataPlatform();
    dummyPlatform.setUrn(platformInstance.getPlatform().toString());
    return dummyPlatform;
  }

  private ContainerMapper() { }
}
