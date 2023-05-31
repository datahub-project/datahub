package com.linkedin.datahub.graphql.types.join.mappers;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.generated.JoinFieldMappingInput;
import com.linkedin.datahub.graphql.generated.JoinUpdateInput;
import com.linkedin.datahub.graphql.types.common.mappers.InstitutionalMemoryUpdateMapper;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipUpdateMapper;
import com.linkedin.datahub.graphql.types.common.mappers.util.UpdateMappingHelper;
import com.linkedin.datahub.graphql.types.mappers.InputModelMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.TagAssociationUpdateMapper;
import com.linkedin.join.EditableJoinProperties;
import com.linkedin.join.FieldMap;
import com.linkedin.join.FieldMapArray;
import com.linkedin.join.JoinFieldMapping;
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
    final AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(actor, SetMode.IGNORE_NULL);
    auditStamp.setTime(System.currentTimeMillis());

    if (input.getProperties() != null) {
      com.linkedin.join.JoinProperties joinProperties = new com.linkedin.join.JoinProperties();
      if (input.getProperties().getName() != null) {
        joinProperties.setName(input.getProperties().getName());
      }
      try {
        if (input.getProperties().getDataSetA() != null) {
          joinProperties.setDatasetA(DatasetUrn.createFromString(input.getProperties().getDataSetA()));
        }
        if (input.getProperties().getDatasetB() != null) {
          joinProperties.setDatasetB(DatasetUrn.createFromString(input.getProperties().getDatasetB()));
        }
      } catch (URISyntaxException e) {
        e.printStackTrace();
      }

      if (input.getProperties().getJoinFieldmappings() != null) {
        JoinFieldMappingInput joinFieldMapping = input.getProperties().getJoinFieldmappings();
        if (joinFieldMapping.getDetails() != null || (joinFieldMapping.getFieldMapping() != null && joinFieldMapping.getFieldMapping().size() > 0)) {
          JoinFieldMapping joinFieldMapping1 = new JoinFieldMapping();
          if (joinFieldMapping.getDetails() != null) {
            joinFieldMapping1.setDetails(joinFieldMapping.getDetails());
          }

          if (joinFieldMapping.getFieldMapping() != null && joinFieldMapping.getFieldMapping().size() > 0) {
            com.linkedin.join.FieldMapArray fieldMapArray = new FieldMapArray();
            joinFieldMapping.getFieldMapping().forEach(fieldMappingInput -> {
              FieldMap fieldMap = new FieldMap();
              if (fieldMappingInput.getAfield() != null) {
                fieldMap.setAfield(fieldMappingInput.getAfield());
              }
              if (fieldMappingInput.getBfield() != null) {
                fieldMap.setBfield(fieldMappingInput.getBfield());
              }
              fieldMapArray.add(fieldMap);
            });
            joinFieldMapping1.setFieldMapping(fieldMapArray);
          }
          joinProperties.setJoinFieldMappings(joinFieldMapping1);
        }
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
        final EditableJoinProperties editableJoinProperties = new EditableJoinProperties();
        editableJoinProperties.setName(input.getEditableProperties().getName());
        editableJoinProperties.setDescription(input.getEditableProperties().getDescription());
        proposals.add(updateMappingHelper.aspectToProposal(editableJoinProperties, EDITABLE_JOIN_PROPERTIES_ASPECT_NAME));
      }
    }
    return proposals;
  }
}

