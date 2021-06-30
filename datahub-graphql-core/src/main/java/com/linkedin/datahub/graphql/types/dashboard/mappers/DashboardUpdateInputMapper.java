package com.linkedin.datahub.graphql.types.dashboard.mappers;

import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.dashboard.Dashboard;
import com.linkedin.dashboard.EditableDashboardProperties;
import com.linkedin.datahub.graphql.generated.DashboardUpdateInput;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipUpdateMapper;
import com.linkedin.datahub.graphql.types.mappers.InputModelMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.TagAssociationUpdateMapper;

import javax.annotation.Nonnull;
import java.util.stream.Collectors;

public class DashboardUpdateInputMapper implements InputModelMapper<DashboardUpdateInput, Dashboard, Urn> {
    public static final DashboardUpdateInputMapper INSTANCE = new DashboardUpdateInputMapper();

    public static Dashboard map(@Nonnull final DashboardUpdateInput dashboardUpdateInput,
                                @Nonnull final Urn actor) {
        return INSTANCE.apply(dashboardUpdateInput, actor);
    }

    @Override
    public Dashboard apply(@Nonnull final DashboardUpdateInput dashboardUpdateInput,
                           @Nonnull final Urn actor) {
        final Dashboard result = new Dashboard();

        if (dashboardUpdateInput.getOwnership() != null) {
            result.setOwnership(OwnershipUpdateMapper.map(dashboardUpdateInput.getOwnership(), actor));
        }

        if (dashboardUpdateInput.getGlobalTags() != null) {
            final GlobalTags globalTags = new GlobalTags();
            globalTags.setTags(
                    new TagAssociationArray(
                            dashboardUpdateInput.getGlobalTags().getTags().stream().map(
                                    element -> TagAssociationUpdateMapper.map(element)
                            ).collect(Collectors.toList())
                    )
            );
            result.setGlobalTags(globalTags);
        }

        if (dashboardUpdateInput.getEditableProperties() != null) {
            final EditableDashboardProperties editableDashboardProperties = new EditableDashboardProperties();
            editableDashboardProperties.setDescription(dashboardUpdateInput.getEditableProperties().getDescription());
            result.setEditableProperties(editableDashboardProperties);
        }
        return result;
    }

}
