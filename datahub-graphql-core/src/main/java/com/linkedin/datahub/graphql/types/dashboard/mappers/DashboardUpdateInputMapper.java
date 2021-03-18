package com.linkedin.datahub.graphql.types.dashboard.mappers;

import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.dashboard.Dashboard;
import com.linkedin.datahub.graphql.generated.DashboardUpdateInput;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.TagAssociationUpdateMapper;

import javax.annotation.Nonnull;
import java.util.stream.Collectors;

public class DashboardUpdateInputMapper implements ModelMapper<DashboardUpdateInput, Dashboard> {
    public static final DashboardUpdateInputMapper INSTANCE = new DashboardUpdateInputMapper();

    public static Dashboard map(@Nonnull final DashboardUpdateInput dashboardUpdateInput) {
        return INSTANCE.apply(dashboardUpdateInput);
    }

    @Override
    public Dashboard apply(@Nonnull final DashboardUpdateInput dashboardUpdateInput) {
        final Dashboard result = new Dashboard();

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
        return result;
    }

}
