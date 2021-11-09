package com.linkedin.datahub.graphql.types.dataflow.mappers;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.DataFlowUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.generated.DataFlowUpdateInput;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipUpdateMapper;
import com.linkedin.datahub.graphql.types.mappers.InputModelMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.TagAssociationUpdateMapper;
import com.linkedin.datajob.EditableDataFlowProperties;

import com.linkedin.metadata.aspect.DataFlowAspect;
import com.linkedin.metadata.aspect.DataFlowAspectArray;
import com.linkedin.metadata.snapshot.DataFlowSnapshot;
import java.net.URISyntaxException;
import javax.annotation.Nonnull;
import java.util.stream.Collectors;

public class DataFlowUpdateInputSnapshotMapper implements InputModelMapper<DataFlowUpdateInput, DataFlowSnapshot, Urn> {
  public static final DataFlowUpdateInputSnapshotMapper INSTANCE = new DataFlowUpdateInputSnapshotMapper();

  public static DataFlowSnapshot map(@Nonnull final DataFlowUpdateInput dataFlowUpdateInput,
      @Nonnull final Urn actor) {
    return INSTANCE.apply(dataFlowUpdateInput, actor);
  }

  @Override
  public DataFlowSnapshot apply(
      @Nonnull final DataFlowUpdateInput dataFlowUpdateInput,
      @Nonnull final Urn actor) {
    final DataFlowSnapshot result = new DataFlowSnapshot();
    final AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(actor, SetMode.IGNORE_NULL);
    auditStamp.setTime(System.currentTimeMillis());

    try {
      result.setUrn(DataFlowUrn.createFromString(dataFlowUpdateInput.getUrn()));
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(
          String.format("Failed to validate provided urn with value %s", dataFlowUpdateInput.getUrn()));
    }

    final DataFlowAspectArray aspects = new DataFlowAspectArray();

    if (dataFlowUpdateInput.getOwnership() != null) {
      aspects.add(DataFlowAspect.create(OwnershipUpdateMapper.map(dataFlowUpdateInput.getOwnership(), actor)));
    }

    if (dataFlowUpdateInput.getGlobalTags() != null) {
      final GlobalTags globalTags = new GlobalTags();
      globalTags.setTags(
          new TagAssociationArray(
              dataFlowUpdateInput.getGlobalTags().getTags().stream().map(TagAssociationUpdateMapper::map
              ).collect(Collectors.toList())
          )
      );
      aspects.add(DataFlowAspect.create(globalTags));
    }

    if (dataFlowUpdateInput.getEditableProperties() != null) {
      final EditableDataFlowProperties editableDataFlowProperties = new EditableDataFlowProperties();
      editableDataFlowProperties.setDescription(dataFlowUpdateInput.getEditableProperties().getDescription());
      editableDataFlowProperties.setCreated(auditStamp);
      editableDataFlowProperties.setLastModified(auditStamp);
      aspects.add(DataFlowAspect.create(editableDataFlowProperties));
    }

    result.setAspects(aspects);

    return result;
  }
}
