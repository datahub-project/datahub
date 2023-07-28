package com.linkedin.datahub.graphql.types.join.mappers;

import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.generated.JoinFieldMappingInput;
import com.linkedin.datahub.graphql.generated.JoinUpdateInput;
import com.linkedin.datahub.graphql.generated.JoinPropertiesInput;
import com.linkedin.datahub.graphql.generated.JoinEditablePropertiesUpdate;
import com.linkedin.datahub.graphql.types.common.mappers.InstitutionalMemoryUpdateMapper;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipUpdateMapper;
import com.linkedin.datahub.graphql.types.common.mappers.util.UpdateMappingHelper;
import com.linkedin.datahub.graphql.types.mappers.InputModelMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.TagAssociationUpdateMapper;
import com.linkedin.join.JoinFieldMapping;
import com.linkedin.join.JoinProperties;
import com.linkedin.join.EditableJoinProperties;
import com.linkedin.join.FieldMapArray;
import com.linkedin.join.FieldMap;
import com.linkedin.mxe.MetadataChangeProposal;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import static com.linkedin.metadata.Constants.*;


public class JoinUpdateInputMapper
    implements InputModelMapper<JoinUpdateInput, Collection<MetadataChangeProposal>, Urn> {
  public static final JoinUpdateInputMapper INSTANCE = new JoinUpdateInputMapper();

  public static Collection<MetadataChangeProposal> map(@Nonnull final JoinUpdateInput joinUpdateInput,
      @Nonnull final Urn actor) {
    return INSTANCE.apply(joinUpdateInput, actor);
  }

  @Override
  public Collection<MetadataChangeProposal> apply(JoinUpdateInput input, Urn actor) {
    final Collection<MetadataChangeProposal> proposals = new ArrayList<>(8);
    final UpdateMappingHelper updateMappingHelper = new UpdateMappingHelper(JOIN_ENTITY_NAME);
    final long currentTime = System.currentTimeMillis();
    final AuditStamp auditstamp = new AuditStamp();
    auditstamp.setActor(actor, SetMode.IGNORE_NULL);
    auditstamp.setTime(currentTime);
    if (input.getProperties() != null) {
      com.linkedin.join.JoinProperties joinProperties = createJoinProperties(input.getProperties(), auditstamp);
      proposals.add(updateMappingHelper.aspectToProposal(joinProperties, JOIN_PROPERTIES_ASPECT_NAME));
    }
      if (input.getOwnership() != null) {
        proposals.add(updateMappingHelper.aspectToProposal(OwnershipUpdateMapper.map(input.getOwnership(), actor),
            OWNERSHIP_ASPECT_NAME));
      }

      if (input.getInstitutionalMemory() != null) {
        proposals.add(updateMappingHelper.aspectToProposal(InstitutionalMemoryUpdateMapper.map(input.getInstitutionalMemory()),
            INSTITUTIONAL_MEMORY_ASPECT_NAME));
      }

      if (input.getTags() != null) {
        final GlobalTags globalTags = new GlobalTags();
        if (input.getTags() != null) {
          globalTags.setTags(new TagAssociationArray(
              input.getTags().getTags().stream().map(TagAssociationUpdateMapper::map).collect(Collectors.toList())));
        }
        proposals.add(updateMappingHelper.aspectToProposal(globalTags, GLOBAL_TAGS_ASPECT_NAME));
      }
      if (input.getEditableProperties() != null) {
        final EditableJoinProperties editableJoinProperties = joinEditablePropsSettings(input.getEditableProperties());
        proposals.add(updateMappingHelper.aspectToProposal(editableJoinProperties, EDITABLE_JOIN_PROPERTIES_ASPECT_NAME));
      }
    return proposals;
  }
  private JoinProperties createJoinProperties(JoinPropertiesInput inputProperties, AuditStamp auditstamp) {
    com.linkedin.join.JoinProperties joinProperties = new com.linkedin.join.JoinProperties();
    if (inputProperties.getName() != null) {
      joinProperties.setName(inputProperties.getName());
    }
    try {
      if (inputProperties.getDataSetA() != null) {
        joinProperties.setDatasetA(DatasetUrn.createFromString(inputProperties.getDataSetA()));
      }
      if (inputProperties.getDatasetB() != null) {
        joinProperties.setDatasetB(DatasetUrn.createFromString(inputProperties.getDatasetB()));
      }
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }

    if (inputProperties.getJoinFieldmapping() != null) {
      JoinFieldMappingInput joinFieldMapping = inputProperties.getJoinFieldmapping();
      if (joinFieldMapping.getDetails() != null || (joinFieldMapping.getFieldMappings() != null
              && joinFieldMapping.getFieldMappings().size() > 0)) {
        JoinFieldMapping joinFieldMappingUnit = joinFieldMappingSettings(joinFieldMapping);
        joinProperties.setJoinFieldMapping(joinFieldMappingUnit);
      }
      if (inputProperties.getCreated() != null && inputProperties.getCreated()) {
        joinProperties.setCreated(auditstamp);
      } else {
        if (inputProperties.getCreatedBy() != null && inputProperties.getCreatedAt() != 0) {
          final AuditStamp auditstampEdit = new AuditStamp();
          try {
            auditstampEdit.setActor(Urn.createFromString(inputProperties.getCreatedBy()));
          } catch (URISyntaxException e) {
            throw new RuntimeException(e);
          }
          auditstampEdit.setTime(inputProperties.getCreatedAt());
          joinProperties.setCreated(auditstampEdit);
        }
      }
      joinProperties.setLastModified(auditstamp);
    }
    return joinProperties;
  }

  private static JoinFieldMapping joinFieldMappingSettings(JoinFieldMappingInput joinFieldMapping) {
    JoinFieldMapping joinFieldMappingUnit = new JoinFieldMapping();
    if (joinFieldMapping.getDetails() != null) {
      joinFieldMappingUnit.setDetails(joinFieldMapping.getDetails());
    }

    if (joinFieldMapping.getFieldMappings() != null && joinFieldMapping.getFieldMappings().size() > 0) {
      com.linkedin.join.FieldMapArray fieldMapArray = new FieldMapArray();
      joinFieldMapping.getFieldMappings().forEach(fieldMappingInput -> {
        FieldMap fieldMap = new FieldMap();
        if (fieldMappingInput.getAfield() != null) {
          fieldMap.setAfield(fieldMappingInput.getAfield());
        }
        if (fieldMappingInput.getBfield() != null) {
          fieldMap.setBfield(fieldMappingInput.getBfield());
        }
        fieldMapArray.add(fieldMap);
      });
      joinFieldMappingUnit.setFieldMappings(fieldMapArray);
    }
    return joinFieldMappingUnit;
  }
  private static EditableJoinProperties joinEditablePropsSettings(JoinEditablePropertiesUpdate editPropsInput) {
    final EditableJoinProperties editableJoinProperties = new EditableJoinProperties();
    if (editPropsInput.getName() != null
            && editPropsInput.getName().trim().length() > 0) {
      editableJoinProperties.setName(editPropsInput.getName());
    }
    if (editPropsInput.getDescription() != null
            && editPropsInput.getDescription().trim().length() > 0) {
      editableJoinProperties.setDescription(editPropsInput.getDescription());
    }
    return editableJoinProperties;
  }
}

