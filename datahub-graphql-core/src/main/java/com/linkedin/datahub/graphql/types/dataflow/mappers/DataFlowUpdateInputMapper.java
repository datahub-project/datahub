package com.linkedin.datahub.graphql.types.dataflow.mappers;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.generated.DataFlowUpdateInput;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipUpdateMapper;
import com.linkedin.datahub.graphql.types.common.mappers.util.UpdateMappingHelper;
import com.linkedin.datahub.graphql.types.mappers.InputModelMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.TagAssociationUpdateMapper;
import com.linkedin.datajob.EditableDataFlowProperties;
import com.linkedin.mxe.MetadataChangeProposal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class DataFlowUpdateInputMapper
    implements InputModelMapper<DataFlowUpdateInput, Collection<MetadataChangeProposal>, Urn> {
  public static final DataFlowUpdateInputMapper INSTANCE = new DataFlowUpdateInputMapper();

  public static Collection<MetadataChangeProposal> map(
      @Nonnull final DataFlowUpdateInput dataFlowUpdateInput, @Nonnull final Urn actor) {
    return INSTANCE.apply(dataFlowUpdateInput, actor);
  }

  @Override
  public Collection<MetadataChangeProposal> apply(
      @Nonnull final DataFlowUpdateInput dataFlowUpdateInput, @Nonnull final Urn actor) {
    final Collection<MetadataChangeProposal> proposals = new ArrayList<>(3);
    final AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(actor, SetMode.IGNORE_NULL);
    auditStamp.setTime(System.currentTimeMillis());
    final UpdateMappingHelper updateMappingHelper = new UpdateMappingHelper(DATA_FLOW_ENTITY_NAME);

    if (dataFlowUpdateInput.getOwnership() != null) {
      proposals.add(
          updateMappingHelper.aspectToProposal(
              OwnershipUpdateMapper.map(dataFlowUpdateInput.getOwnership(), actor),
              OWNERSHIP_ASPECT_NAME));
    }

    if (dataFlowUpdateInput.getTags() != null || dataFlowUpdateInput.getGlobalTags() != null) {
      final GlobalTags globalTags = new GlobalTags();
      if (dataFlowUpdateInput.getGlobalTags() != null) {
        globalTags.setTags(
            new TagAssociationArray(
                dataFlowUpdateInput.getGlobalTags().getTags().stream()
                    .map(TagAssociationUpdateMapper::map)
                    .collect(Collectors.toList())));
      } else {
        globalTags.setTags(
            new TagAssociationArray(
                dataFlowUpdateInput.getTags().getTags().stream()
                    .map(TagAssociationUpdateMapper::map)
                    .collect(Collectors.toList())));
      }
      proposals.add(updateMappingHelper.aspectToProposal(globalTags, GLOBAL_TAGS_ASPECT_NAME));
    }

    if (dataFlowUpdateInput.getEditableProperties() != null) {
      final EditableDataFlowProperties editableDataFlowProperties =
          new EditableDataFlowProperties();
      editableDataFlowProperties.setDescription(
          dataFlowUpdateInput.getEditableProperties().getDescription());
      editableDataFlowProperties.setCreated(auditStamp);
      editableDataFlowProperties.setLastModified(auditStamp);
      proposals.add(
          updateMappingHelper.aspectToProposal(
              editableDataFlowProperties, EDITABLE_DATA_FLOW_PROPERTIES_ASPECT_NAME));
    }

    return proposals;
  }
}
