package com.linkedin.datahub.graphql.types.dashboard.mappers;

import com.linkedin.datahub.graphql.generated.AccessLevel;
import com.linkedin.datahub.graphql.generated.Chart;
import com.linkedin.datahub.graphql.generated.Dashboard;
import com.linkedin.datahub.graphql.generated.DashboardInfo;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.common.mappers.AuditStampMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.datahub.graphql.types.common.mappers.StatusMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.GlobalTagsMapper;

import javax.annotation.Nonnull;
import java.util.stream.Collectors;

public class DashboardMapper implements ModelMapper<com.linkedin.dashboard.Dashboard, Dashboard> {

    public static final DashboardMapper INSTANCE = new DashboardMapper();

    public static Dashboard map(@Nonnull final com.linkedin.dashboard.Dashboard dashboard) {
        return INSTANCE.apply(dashboard);
    }

    @Override
    public Dashboard apply(@Nonnull final com.linkedin.dashboard.Dashboard dashboard) {
        final Dashboard result = new Dashboard();
        result.setUrn(dashboard.getUrn().toString());
        result.setType(EntityType.DASHBOARD);
        result.setDashboardId(dashboard.getDashboardId());
        result.setTool(dashboard.getTool());
        if (dashboard.hasInfo()) {
            result.setInfo(mapDashboardInfo(dashboard.getInfo()));
        }
        if (dashboard.hasOwnership()) {
            result.setOwnership(OwnershipMapper.map(dashboard.getOwnership()));
        }
        if (dashboard.hasStatus()) {
            result.setStatus(StatusMapper.map(dashboard.getStatus()));
        }
        if (dashboard.hasGlobalTags()) {
            result.setGlobalTags(GlobalTagsMapper.map(dashboard.getGlobalTags()));
        }
        return result;
    }

    private DashboardInfo mapDashboardInfo(final com.linkedin.dashboard.DashboardInfo info) {
        final DashboardInfo result = new DashboardInfo();
        result.setDescription(info.getDescription());
        result.setName(info.getTitle());
        result.setLastRefreshed(info.getLastRefreshed());
        result.setCharts(info.getCharts().stream().map(urn -> {
            final Chart chart = new Chart();
            chart.setUrn(urn.toString());
            return chart;
        }).collect(Collectors.toList()));

        if (info.hasAccess()) {
            result.setAccess(AccessLevel.valueOf(info.getAccess().toString()));
        }
        if (info.hasDashboardUrl()) {
            result.setUrl(info.getDashboardUrl().toString());
        }
        result.setLastModified(AuditStampMapper.map(info.getLastModified().getLastModified()));
        result.setCreated(AuditStampMapper.map(info.getLastModified().getCreated()));
        if (info.getLastModified().hasDeleted()) {
            result.setDeleted(AuditStampMapper.map(info.getLastModified().getDeleted()));
        }
        return result;
    }
}
