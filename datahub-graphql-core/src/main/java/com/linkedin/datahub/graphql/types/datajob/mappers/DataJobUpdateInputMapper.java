package com.linkedin.datahub.graphql.types.datajob.mappers;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.generated.DataJobUpdateInput;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipUpdateMapper;
import com.linkedin.datahub.graphql.types.common.mappers.util.UpdateMappingHelper;
import com.linkedin.datahub.graphql.types.mappers.InputModelMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.TagAssociationUpdateMapper;
import com.linkedin.datajob.EditableDataJobProperties;
import com.linkedin.mxe.MetadataChangeProposal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class DataJobUpdateInputMapper
    implements InputModelMapper<DataJobUpdateInput, Collection<MetadataChangeProposal>, Urn> {
  public static final DataJobUpdateInputMapper INSTANCE = new DataJobUpdateInputMapper();

  public static Collection<MetadataChangeProposal> map(
      @Nonnull final DataJobUpdateInput dataJobUpdateInput, @Nonnull final Urn actor) {
    return INSTANCE.apply(dataJobUpdateInput, actor);
  }

  @Override
  public Collection<MetadataChangeProposal> apply(
      @Nonnull final DataJobUpdateInput dataJobUpdateInput, @Nonnull final Urn actor) {
    final Collection<MetadataChangeProposal> proposals = new ArrayList<>(3);
    final UpdateMappingHelper updateMappingHelper = new UpdateMappingHelper(DATA_JOB_ENTITY_NAME);

    final AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(actor, SetMode.IGNORE_NULL);
    auditStamp.setTime(System.currentTimeMillis());

    if (dataJobUpdateInput.getOwnership() != null) {
      proposals.add(
          updateMappingHelper.aspectToProposal(
              OwnershipUpdateMapper.map(dataJobUpdateInput.getOwnership(), actor),
              OWNERSHIP_ASPECT_NAME));
    }

    if (dataJobUpdateInput.getTags() != null || dataJobUpdateInput.getGlobalTags() != null) {
      final GlobalTags globalTags = new GlobalTags();
      if (dataJobUpdateInput.getGlobalTags() != null) {
        globalTags.setTags(
            new TagAssociationArray(
                dataJobUpdateInput.getGlobalTags().getTags().stream()
                    .map(TagAssociationUpdateMapper::map)
                    .collect(Collectors.toList())));
      } else {
        globalTags.setTags(
            new TagAssociationArray(
                dataJobUpdateInput.getTags().getTags().stream()
                    .map(TagAssociationUpdateMapper::map)
                    .collect(Collectors.toList())));
      }
      proposals.add(updateMappingHelper.aspectToProposal(globalTags, GLOBAL_TAGS_ASPECT_NAME));
    }

    if (dataJobUpdateInput.getEditableProperties() != null) {
      final EditableDataJobProperties editableDataJobProperties = new EditableDataJobProperties();
      editableDataJobProperties.setDescription(
          dataJobUpdateInput.getEditableProperties().getDescription());
      editableDataJobProperties.setCreated(auditStamp);
      editableDataJobProperties.setLastModified(auditStamp);
      proposals.add(
          updateMappingHelper.aspectToProposal(
              editableDataJobProperties, EDITABLE_DATA_JOB_PROPERTIES_ASPECT_NAME));
    }

    return proposals;
  }
}
