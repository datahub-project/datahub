package com.linkedin.datahub.graphql.types.dashboard.mappers;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.GlobalTags;

import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.dashboard.EditableDashboardProperties;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.generated.DashboardUpdateInput;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipUpdateMapper;
import com.linkedin.datahub.graphql.types.mappers.InputModelMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.TagAssociationUpdateMapper;
import com.linkedin.metadata.aspect.DashboardAspect;
import com.linkedin.metadata.aspect.DashboardAspectArray;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.snapshot.DashboardSnapshot;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;


public class DashboardUpdateInputSnapshotMapper implements
                                                InputModelMapper<DashboardUpdateInput, DashboardSnapshot, Urn> {
    public static final DashboardUpdateInputSnapshotMapper INSTANCE = new DashboardUpdateInputSnapshotMapper();

    public static DashboardSnapshot map(@Nonnull final DashboardUpdateInput dashboardUpdateInput,
                                @Nonnull final Urn actor) {
        return INSTANCE.apply(dashboardUpdateInput, actor);
    }

    @Override
    public DashboardSnapshot apply(@Nonnull final DashboardUpdateInput dashboardUpdateInput,
                           @Nonnull final Urn actor) {
        final DashboardSnapshot result = new DashboardSnapshot();
        final AuditStamp auditStamp = new AuditStamp();
        auditStamp.setActor(actor, SetMode.IGNORE_NULL);
        auditStamp.setTime(System.currentTimeMillis());

        final DashboardAspectArray aspects = new DashboardAspectArray();

        if (dashboardUpdateInput.getOwnership() != null) {
            aspects.add(DashboardAspect.create(OwnershipUpdateMapper.map(dashboardUpdateInput.getOwnership(), actor)));
        }

        if (dashboardUpdateInput.getTags() != null || dashboardUpdateInput.getGlobalTags() != null) {
            final GlobalTags globalTags = new GlobalTags();
            if (dashboardUpdateInput.getGlobalTags() != null) {
                globalTags.setTags(
                    new TagAssociationArray(
                        dashboardUpdateInput.getGlobalTags().getTags().stream().map(
                            element -> TagAssociationUpdateMapper.map(element)
                        ).collect(Collectors.toList())
                    )
                );
            } else {
                // Tags override global tags
                globalTags.setTags(
                    new TagAssociationArray(
                        dashboardUpdateInput.getTags().getTags().stream().map(
                            element -> TagAssociationUpdateMapper.map(element)
                        ).collect(Collectors.toList())
                    )
                );
            }
            aspects.add(DashboardAspect.create(globalTags));
        }

        if (dashboardUpdateInput.getEditableProperties() != null) {
            final EditableDashboardProperties editableDashboardProperties = new EditableDashboardProperties();
            editableDashboardProperties.setDescription(dashboardUpdateInput.getEditableProperties().getDescription());
            if (!editableDashboardProperties.hasCreated()) {
                editableDashboardProperties.setCreated(auditStamp);
            }
            editableDashboardProperties.setLastModified(auditStamp);
            aspects.add(ModelUtils.newAspectUnion(DashboardAspect.class, editableDashboardProperties));
        }

        result.setAspects(aspects);

        return result;
    }

}
