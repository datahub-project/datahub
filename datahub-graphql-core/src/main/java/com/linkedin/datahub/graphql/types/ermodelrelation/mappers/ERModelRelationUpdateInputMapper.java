package com.linkedin.datahub.graphql.types.ermodelrelation.mappers;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.generated.ERModelRelationEditablePropertiesUpdate;
import com.linkedin.datahub.graphql.generated.ERModelRelationPropertiesInput;
import com.linkedin.datahub.graphql.generated.ERModelRelationUpdateInput;
import com.linkedin.datahub.graphql.generated.RelationshipFieldMappingInput;
import com.linkedin.datahub.graphql.types.common.mappers.util.UpdateMappingHelper;
import com.linkedin.datahub.graphql.types.mappers.InputModelMapper;
import com.linkedin.ermodelrelation.ERModelRelationProperties;
import com.linkedin.ermodelrelation.EditableERModelRelationProperties;
import com.linkedin.ermodelrelation.RelationshipFieldMappingArray;
import com.linkedin.mxe.MetadataChangeProposal;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import javax.annotation.Nonnull;

public class ERModelRelationUpdateInputMapper
    implements InputModelMapper<
        ERModelRelationUpdateInput, Collection<MetadataChangeProposal>, Urn> {
  public static final ERModelRelationUpdateInputMapper INSTANCE =
      new ERModelRelationUpdateInputMapper();

  public static Collection<MetadataChangeProposal> map(
      @Nonnull final ERModelRelationUpdateInput ermodelrelationUpdateInput,
      @Nonnull final Urn actor) {
    return INSTANCE.apply(ermodelrelationUpdateInput, actor);
  }

  @Override
  public Collection<MetadataChangeProposal> apply(ERModelRelationUpdateInput input, Urn actor) {
    final Collection<MetadataChangeProposal> proposals = new ArrayList<>(8);
    final UpdateMappingHelper updateMappingHelper =
        new UpdateMappingHelper(ERMODELRELATION_ENTITY_NAME);
    final long currentTime = System.currentTimeMillis();
    final AuditStamp auditstamp = new AuditStamp();
    auditstamp.setActor(actor, SetMode.IGNORE_NULL);
    auditstamp.setTime(currentTime);
    if (input.getProperties() != null) {
      com.linkedin.ermodelrelation.ERModelRelationProperties ermodelrelationProperties =
          createERModelRelationProperties(input.getProperties(), auditstamp);
      proposals.add(
          updateMappingHelper.aspectToProposal(
              ermodelrelationProperties, ERMODELRELATION_PROPERTIES_ASPECT_NAME));
    }
    if (input.getEditableProperties() != null) {
      final EditableERModelRelationProperties editableERModelRelationProperties =
          ermodelrelationEditablePropsSettings(input.getEditableProperties());
      proposals.add(
          updateMappingHelper.aspectToProposal(
              editableERModelRelationProperties, EDITABLE_ERMODELRELATION_PROPERTIES_ASPECT_NAME));
    }
    return proposals;
  }

  private ERModelRelationProperties createERModelRelationProperties(
      ERModelRelationPropertiesInput inputProperties, AuditStamp auditstamp) {
    com.linkedin.ermodelrelation.ERModelRelationProperties ermodelrelationProperties =
        new com.linkedin.ermodelrelation.ERModelRelationProperties();
    if (inputProperties.getName() != null) {
      ermodelrelationProperties.setName(inputProperties.getName());
    }
    try {
      if (inputProperties.getSource() != null) {
        ermodelrelationProperties.setSource(
            DatasetUrn.createFromString(inputProperties.getSource()));
      }
      if (inputProperties.getDestination() != null) {
        ermodelrelationProperties.setDestination(
            DatasetUrn.createFromString(inputProperties.getDestination()));
      }
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }

    if (inputProperties.getRelationshipFieldmappings() != null) {
      if (inputProperties.getRelationshipFieldmappings().size() > 0) {
        com.linkedin.ermodelrelation.RelationshipFieldMappingArray relationshipFieldMappingsArray =
            ermodelrelationFieldMappingSettings(inputProperties.getRelationshipFieldmappings());

        ermodelrelationProperties.setRelationshipfieldMappings(relationshipFieldMappingsArray);
      }

      if (inputProperties.getCreated() != null && inputProperties.getCreated()) {
        ermodelrelationProperties.setCreated(auditstamp);
      } else {
        if (inputProperties.getCreatedBy() != null && inputProperties.getCreatedAt() != 0) {
          final AuditStamp auditstampEdit = new AuditStamp();
          try {
            auditstampEdit.setActor(Urn.createFromString(inputProperties.getCreatedBy()));
          } catch (URISyntaxException e) {
            throw new RuntimeException(e);
          }
          auditstampEdit.setTime(inputProperties.getCreatedAt());
          ermodelrelationProperties.setCreated(auditstampEdit);
        }
      }
      ermodelrelationProperties.setLastModified(auditstamp);
    }
    return ermodelrelationProperties;
  }

  private com.linkedin.ermodelrelation.RelationshipFieldMappingArray
      ermodelrelationFieldMappingSettings(
          List<RelationshipFieldMappingInput> ermodelrelationFieldMapping) {

    List<com.linkedin.ermodelrelation.RelationshipFieldMapping> relationshipFieldMappingList =
        this.mapRelationshipFieldMapping(ermodelrelationFieldMapping);
    com.linkedin.ermodelrelation.RelationshipFieldMappingArray relationshipFieldMappingArray =
        new RelationshipFieldMappingArray(relationshipFieldMappingList);

    return relationshipFieldMappingArray;
  }

  private List<com.linkedin.ermodelrelation.RelationshipFieldMapping> mapRelationshipFieldMapping(
      List<RelationshipFieldMappingInput> ermodelrelationFieldMapping) {

    List<com.linkedin.ermodelrelation.RelationshipFieldMapping> relationshipFieldMappingList =
        new ArrayList<>();

    ermodelrelationFieldMapping.forEach(
        relationshipFieldMappingInput -> {
          com.linkedin.ermodelrelation.RelationshipFieldMapping relationshipFieldMapping =
              new com.linkedin.ermodelrelation.RelationshipFieldMapping();
          relationshipFieldMapping.setSourceField(relationshipFieldMappingInput.getSourceField());
          relationshipFieldMapping.setDestinationField(
              relationshipFieldMappingInput.getDestinationField());
          relationshipFieldMappingList.add(relationshipFieldMapping);
        });

    return relationshipFieldMappingList;
  }

  private static EditableERModelRelationProperties ermodelrelationEditablePropsSettings(
      ERModelRelationEditablePropertiesUpdate editPropsInput) {
    final EditableERModelRelationProperties editableERModelRelationProperties =
        new EditableERModelRelationProperties();
    if (editPropsInput.getName() != null && editPropsInput.getName().trim().length() > 0) {
      editableERModelRelationProperties.setName(editPropsInput.getName());
    }
    if (editPropsInput.getDescription() != null
        && editPropsInput.getDescription().trim().length() > 0) {
      editableERModelRelationProperties.setDescription(editPropsInput.getDescription());
    }
    return editableERModelRelationProperties;
  }
}
