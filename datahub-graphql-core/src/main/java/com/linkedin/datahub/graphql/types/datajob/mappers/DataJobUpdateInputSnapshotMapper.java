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
import com.linkedin.datajob.EditableDataJobProperties;

import com.linkedin.metadata.aspect.DataJobAspect;
import com.linkedin.metadata.aspect.DataJobAspectArray;
import com.linkedin.metadata.snapshot.DataJobSnapshot;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;


public class DataJobUpdateInputSnapshotMapper implements InputModelMapper<DataJobUpdateInput, DataJobSnapshot, Urn> {
    public static final DataJobUpdateInputSnapshotMapper INSTANCE = new DataJobUpdateInputSnapshotMapper();

    public static DataJobSnapshot map(
        @Nonnull final DataJobUpdateInput dataJobUpdateInput,
        @Nonnull final Urn actor) {
        return INSTANCE.apply(dataJobUpdateInput, actor);
    }

    @Override
    public DataJobSnapshot apply(
        @Nonnull final DataJobUpdateInput dataJobUpdateInput,
        @Nonnull final Urn actor) {
        final DataJobSnapshot result = new DataJobSnapshot();

        final AuditStamp auditStamp = new AuditStamp();
        auditStamp.setActor(actor, SetMode.IGNORE_NULL);
        auditStamp.setTime(System.currentTimeMillis());

        final DataJobAspectArray aspects = new DataJobAspectArray();

        if (dataJobUpdateInput.getOwnership() != null) {
            aspects.add(DataJobAspect.create(OwnershipUpdateMapper.map(dataJobUpdateInput.getOwnership(), actor)));
        }

        if (dataJobUpdateInput.getTags() != null || dataJobUpdateInput.getGlobalTags() != null) {
            final GlobalTags globalTags = new GlobalTags();
            if (dataJobUpdateInput.getGlobalTags() != null) {
                globalTags.setTags(
                    new TagAssociationArray(
                        dataJobUpdateInput.getGlobalTags().getTags().stream().map(TagAssociationUpdateMapper::map
                        ).collect(Collectors.toList())
                    )
                );
            } else {
                globalTags.setTags(
                    new TagAssociationArray(
                        dataJobUpdateInput.getTags().getTags().stream().map(TagAssociationUpdateMapper::map
                        ).collect(Collectors.toList())
                    )
                );
            }
            aspects.add(DataJobAspect.create(globalTags));
        }

        if (dataJobUpdateInput.getEditableProperties() != null) {
            final EditableDataJobProperties editableDataJobProperties = new EditableDataJobProperties();
            editableDataJobProperties.setDescription(dataJobUpdateInput.getEditableProperties().getDescription());
            editableDataJobProperties.setCreated(auditStamp);
            editableDataJobProperties.setLastModified(auditStamp);
            aspects.add(DataJobAspect.create(editableDataJobProperties));
        }

        result.setAspects(aspects);

        return result;
    }
}
