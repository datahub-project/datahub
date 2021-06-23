package com.linkedin.datahub.graphql.types.dataflow.mappers;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.generated.DataFlowUpdateInput;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipUpdateMapper;
import com.linkedin.datahub.graphql.types.mappers.InputModelMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.TagAssociationUpdateMapper;
import com.linkedin.datajob.DataFlow;
import com.linkedin.datajob.EditableDataFlowProperties;

import javax.annotation.Nonnull;
import java.util.stream.Collectors;

public class DataFlowUpdateInputMapper implements InputModelMapper<DataFlowUpdateInput, DataFlow, Urn> {
    public static final DataFlowUpdateInputMapper INSTANCE = new DataFlowUpdateInputMapper();

    public static DataFlow map(@Nonnull final DataFlowUpdateInput dataFlowUpdateInput,
                               @Nonnull final Urn actor) {
        return INSTANCE.apply(dataFlowUpdateInput, actor);
    }

    @Override
    public DataFlow apply(@Nonnull final DataFlowUpdateInput dataFlowUpdateInput,
                          @Nonnull final Urn actor) {
        final DataFlow result = new DataFlow();
        final AuditStamp auditStamp = new AuditStamp();
        auditStamp.setActor(actor, SetMode.IGNORE_NULL);
        auditStamp.setTime(System.currentTimeMillis());

        if (dataFlowUpdateInput.getOwnership() != null) {
            result.setOwnership(OwnershipUpdateMapper.map(dataFlowUpdateInput.getOwnership(), actor));
        }

        if (dataFlowUpdateInput.getGlobalTags() != null) {
            final GlobalTags globalTags = new GlobalTags();
            globalTags.setTags(
                    new TagAssociationArray(
                            dataFlowUpdateInput.getGlobalTags().getTags().stream().map(
                                    element -> TagAssociationUpdateMapper.map(element)
                            ).collect(Collectors.toList())
                    )
            );
            result.setGlobalTags(globalTags);
        }

        if (dataFlowUpdateInput.getEditableProperties() != null) {
            final EditableDataFlowProperties editableDataFlowProperties = new EditableDataFlowProperties();
            editableDataFlowProperties.setDescription(dataFlowUpdateInput.getEditableProperties().getDescription());
            if (!editableDataFlowProperties.hasCreated()) {
                editableDataFlowProperties.setCreated(auditStamp);
            }
            editableDataFlowProperties.setLastModified(auditStamp);
            result.setEditableProperties(editableDataFlowProperties);
        }
        return result;
    }
}
