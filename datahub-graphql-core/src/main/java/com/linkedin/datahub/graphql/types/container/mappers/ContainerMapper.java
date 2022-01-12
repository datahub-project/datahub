package com.linkedin.datahub.graphql.types.container.mappers;

import com.linkedin.common.SubTypes;
import com.linkedin.common.urn.TupleKey;
import com.linkedin.common.urn.Urn;
import com.linkedin.container.ContainerProperties;
import com.linkedin.container.EditableContainerProperties;
import com.linkedin.datahub.graphql.generated.Container;
import com.linkedin.datahub.graphql.generated.DataPlatform;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.ContainerKey;


public class ContainerMapper {

  public static Container map(final EntityResponse entityResponse) {
    final Container result = new Container();
    final Urn entityUrn = entityResponse.getUrn();
    final EnvelopedAspectMap aspects = entityResponse.getAspects();

    result.setUrn(entityUrn.toString());
    result.setType(EntityType.CONTAINER);

    final EnvelopedAspect envelopedContainerProperties = aspects.get(Constants.CONTAINER_PROPERTIES_ASPECT_NAME);
    if (envelopedContainerProperties != null) {
      result.setProperties(mapContainerProperties(new ContainerProperties(envelopedContainerProperties.getValue().data())));
    }

    final EnvelopedAspect envelopedEditableContainerProperties = aspects.get(Constants.CONTAINER_EDITABLE_PROPERTIES_ASPECT_NAME);
    if (envelopedEditableContainerProperties != null) {
      result.setEditableProperties(mapContainerEditableProperties(new EditableContainerProperties(envelopedEditableContainerProperties.getValue().data())));
    }

    final EnvelopedAspect envelopedSubTypes = aspects.get(Constants.SUB_TYPES_ASPECT_NAME);
    if (envelopedSubTypes != null) {
      result.setSubTypes(mapSubTypes(new SubTypes(envelopedSubTypes.getValue().data())));
    }

    final EnvelopedAspect envelopedContainerKey = aspects.get(Constants.CONTAINER_KEY_ASPECT_NAME);
    if (envelopedContainerKey != null) {
      result.setPlatform(mapPlatform(new ContainerKey(envelopedContainerKey.getValue().data())));
    }

    return result;
  }

  private static com.linkedin.datahub.graphql.generated.ContainerProperties mapContainerProperties(final ContainerProperties gmsProperties) {
    final com.linkedin.datahub.graphql.generated.ContainerProperties propertiesResult = new com.linkedin.datahub.graphql.generated.ContainerProperties();
    propertiesResult.setName(gmsProperties.getName());
    propertiesResult.setDescription(gmsProperties.getDescription());
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

  private static DataPlatform mapPlatform(final ContainerKey gmsKey) {
    // Set dummy platform to be resolved.
    final DataPlatform dummyPlatform = new DataPlatform();
    dummyPlatform.setUrn(new Urn(Constants.DATASET_ENTITY_NAME, new TupleKey(gmsKey.getPlatformType())).toString());
    return dummyPlatform;
  }

  private ContainerMapper() { }
}
