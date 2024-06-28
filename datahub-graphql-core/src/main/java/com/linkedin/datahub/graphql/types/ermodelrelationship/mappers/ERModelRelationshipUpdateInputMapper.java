package com.linkedin.datahub.graphql.types.ermodelrelationship.mappers;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.ERModelRelationshipEditablePropertiesUpdate;
import com.linkedin.datahub.graphql.generated.ERModelRelationshipPropertiesInput;
import com.linkedin.datahub.graphql.generated.ERModelRelationshipUpdateInput;
import com.linkedin.datahub.graphql.generated.RelationshipFieldMappingInput;
import com.linkedin.datahub.graphql.types.common.mappers.util.UpdateMappingHelper;
import com.linkedin.datahub.graphql.types.mappers.InputModelMapper;
import com.linkedin.ermodelrelation.ERModelRelationshipCardinality;
import com.linkedin.ermodelrelation.ERModelRelationshipProperties;
import com.linkedin.ermodelrelation.EditableERModelRelationshipProperties;
import com.linkedin.ermodelrelation.RelationshipFieldMappingArray;
import com.linkedin.mxe.MetadataChangeProposal;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class ERModelRelationshipUpdateInputMapper
    implements InputModelMapper<
        ERModelRelationshipUpdateInput, Collection<MetadataChangeProposal>, Urn> {
  public static final ERModelRelationshipUpdateInputMapper INSTANCE =
      new ERModelRelationshipUpdateInputMapper();

  public static Collection<MetadataChangeProposal> map(
      @Nullable final QueryContext context,
      @Nonnull final ERModelRelationshipUpdateInput ermodelrelationUpdateInput,
      @Nonnull final Urn actor) {
    return INSTANCE.apply(context, ermodelrelationUpdateInput, actor);
  }

  @Override
  public Collection<MetadataChangeProposal> apply(
      @Nullable final QueryContext context, ERModelRelationshipUpdateInput input, Urn actor) {
    final Collection<MetadataChangeProposal> proposals = new ArrayList<>(8);
    final UpdateMappingHelper updateMappingHelper =
        new UpdateMappingHelper(ER_MODEL_RELATIONSHIP_ENTITY_NAME);
    final long currentTime = System.currentTimeMillis();
    final AuditStamp auditstamp = new AuditStamp();
    auditstamp.setActor(actor, SetMode.IGNORE_NULL);
    auditstamp.setTime(currentTime);
    if (input.getProperties() != null) {
      com.linkedin.ermodelrelation.ERModelRelationshipProperties ermodelrelationProperties =
          createERModelRelationProperties(input.getProperties(), auditstamp);
      proposals.add(
          updateMappingHelper.aspectToProposal(
              ermodelrelationProperties, ER_MODEL_RELATIONSHIP_PROPERTIES_ASPECT_NAME));
    }
    if (input.getEditableProperties() != null) {
      final EditableERModelRelationshipProperties editableERModelRelationProperties =
          ermodelrelationshipEditablePropsSettings(input.getEditableProperties());
      proposals.add(
          updateMappingHelper.aspectToProposal(
              editableERModelRelationProperties,
              EDITABLE_ER_MODEL_RELATIONSHIP_PROPERTIES_ASPECT_NAME));
    }
    return proposals;
  }

  private ERModelRelationshipProperties createERModelRelationProperties(
      ERModelRelationshipPropertiesInput inputProperties, AuditStamp auditstamp) {
    com.linkedin.ermodelrelation.ERModelRelationshipProperties ermodelrelationProperties =
        new com.linkedin.ermodelrelation.ERModelRelationshipProperties();
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
        ermodelrelationProperties.setCardinality(
            ermodelrelationCardinalitySettings(inputProperties.getRelationshipFieldmappings()));
        ermodelrelationProperties.setRelationshipFieldMappings(relationshipFieldMappingsArray);
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

  private com.linkedin.ermodelrelation.ERModelRelationshipCardinality
      ermodelrelationCardinalitySettings(
          List<RelationshipFieldMappingInput> ermodelrelationFieldMapping) {

    Set<String> sourceFields = new HashSet<>();
    Set<String> destFields = new HashSet<>();
    AtomicInteger sourceCount = new AtomicInteger();
    AtomicInteger destCount = new AtomicInteger();

    ermodelrelationFieldMapping.forEach(
        relationshipFieldMappingInput -> {
          sourceFields.add(relationshipFieldMappingInput.getSourceField());
          sourceCount.getAndIncrement();
          destFields.add(relationshipFieldMappingInput.getDestinationField());
          destCount.getAndIncrement();
        });

    if (sourceFields.size() == sourceCount.get()) {
      if (destFields.size() == destCount.get()) {
        return ERModelRelationshipCardinality.ONE_ONE;
      } else {
        return ERModelRelationshipCardinality.N_ONE;
      }
    } else {
      if (destFields.size() == destCount.get()) {
        return ERModelRelationshipCardinality.ONE_N;
      } else {
        return ERModelRelationshipCardinality.N_N;
      }
    }
  }

  private com.linkedin.ermodelrelation.RelationshipFieldMappingArray
      ermodelrelationFieldMappingSettings(
          List<RelationshipFieldMappingInput> ermodelrelationFieldMapping) {

    List<com.linkedin.ermodelrelation.RelationshipFieldMapping> relationshipFieldMappingList =
        this.mapRelationshipFieldMapping(ermodelrelationFieldMapping);

    return new RelationshipFieldMappingArray(relationshipFieldMappingList);
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

  private static EditableERModelRelationshipProperties ermodelrelationshipEditablePropsSettings(
      ERModelRelationshipEditablePropertiesUpdate editPropsInput) {
    final EditableERModelRelationshipProperties editableERModelRelationProperties =
        new EditableERModelRelationshipProperties();
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
