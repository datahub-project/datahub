package com.linkedin.datahub.graphql.types.datajob.mappers;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.generated.DataJobUpdateInput;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipUpdateMapper;
import com.linkedin.datahub.graphql.types.mappers.InputModelMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.TagAssociationUpdateMapper;
import com.linkedin.datajob.DataJob;
import com.linkedin.datajob.EditableDatajobProperties;

import javax.annotation.Nonnull;
import java.util.stream.Collectors;

public class DataJobUpdateInputMapper implements InputModelMapper<DataJobUpdateInput, DataJob, Urn> {
    public static final DataJobUpdateInputMapper INSTANCE = new DataJobUpdateInputMapper();

    public static DataJob map(@Nonnull final DataJobUpdateInput dataJobUpdateInput,
                              @Nonnull final Urn actor) {
        return INSTANCE.apply(dataJobUpdateInput, actor);
    }

    @Override
    public DataJob apply(@Nonnull final DataJobUpdateInput dataJobUpdateInput,
                         @Nonnull final Urn actor) {
        final DataJob result = new DataJob();
        final AuditStamp auditStamp = new AuditStamp();
        auditStamp.setActor(actor, SetMode.IGNORE_NULL);
        auditStamp.setTime(System.currentTimeMillis());
        if (dataJobUpdateInput.getOwnership() != null) {
            result.setOwnership(OwnershipUpdateMapper.map(dataJobUpdateInput.getOwnership(), actor));
        }

        if (dataJobUpdateInput.getGlobalTags() != null) {
            final GlobalTags globalTags = new GlobalTags();
            globalTags.setTags(
                    new TagAssociationArray(
                            dataJobUpdateInput.getGlobalTags().getTags().stream().map(
                                    element -> TagAssociationUpdateMapper.map(element)
                            ).collect(Collectors.toList())
                    )
            );
            result.setGlobalTags(globalTags);
        }

        if (dataJobUpdateInput.getEditableProperties() != null) {
            final EditableDatajobProperties editableDatajobProperties = new EditableDatajobProperties();
            editableDatajobProperties.setDescription(dataJobUpdateInput.getEditableProperties().getDescription());
            if (!editableDatajobProperties.hasCreated()) {
                editableDatajobProperties.setCreated(auditStamp);
            }
            editableDatajobProperties.setLastModified(auditStamp);
            result.setEditableProperties(editableDatajobProperties);
        }
        return result;
    }
}