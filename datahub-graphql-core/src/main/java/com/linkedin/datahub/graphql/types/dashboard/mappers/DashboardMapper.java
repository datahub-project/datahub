package com.linkedin.datahub.graphql.types.dashboard.mappers;

import com.linkedin.common.GlobalTags;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.Ownership;
import com.linkedin.common.Status;
import com.linkedin.dashboard.EditableDashboardProperties;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.generated.AccessLevel;
import com.linkedin.datahub.graphql.generated.Chart;
import com.linkedin.datahub.graphql.generated.Dashboard;
import com.linkedin.datahub.graphql.generated.DashboardEditableProperties;
import com.linkedin.datahub.graphql.generated.DashboardInfo;
import com.linkedin.datahub.graphql.generated.DashboardProperties;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.common.mappers.AuditStampMapper;
import com.linkedin.datahub.graphql.types.common.mappers.InstitutionalMemoryMapper;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.datahub.graphql.types.common.mappers.StatusMapper;
import com.linkedin.datahub.graphql.types.common.mappers.StringMapMapper;
import com.linkedin.datahub.graphql.types.glossary.mappers.GlossaryTermsMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.GlobalTagsMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.metadata.key.DashboardKey;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import static com.linkedin.metadata.Constants.*;


public class DashboardMapper implements ModelMapper<EntityResponse, Dashboard> {

    public static final DashboardMapper INSTANCE = new DashboardMapper();

    public static Dashboard map(@Nonnull final EntityResponse entityResponse) {
        return INSTANCE.apply(entityResponse);
    }

    @Override
    public Dashboard apply(@Nonnull final EntityResponse entityResponse) {
        final Dashboard result = new Dashboard();
        result.setUrn(entityResponse.getUrn().toString());
        result.setType(EntityType.DASHBOARD);

        entityResponse.getAspects().forEach((name, aspect) -> {
            DataMap data = aspect.getValue().data();
            if (DASHBOARD_KEY_ASPECT_NAME.equals(name)) {
                final DashboardKey gmsKey = new DashboardKey(data);
                result.setDashboardId(gmsKey.getDashboardId());
                result.setTool(gmsKey.getDashboardTool());
            } else if (DASHBOARD_INFO_ASPECT_NAME.equals(name)) {
                final com.linkedin.dashboard.DashboardInfo gmsDashboardInfo = new com.linkedin.dashboard.DashboardInfo(data);
                result.setInfo(mapDashboardInfo(gmsDashboardInfo));
                result.setProperties(mapDashboardInfoToProperties(gmsDashboardInfo));
            } else if (EDITABLE_DASHBOARD_PROPERTIES_ASPECT_NAME.equals(name)) {
                final EditableDashboardProperties editableDashboardProperties = new EditableDashboardProperties(data);
                final DashboardEditableProperties dashboardEditableProperties = new DashboardEditableProperties();
                dashboardEditableProperties.setDescription(editableDashboardProperties.getDescription());
                result.setEditableProperties(dashboardEditableProperties);
            } else if (OWNERSHIP_ASPECT_NAME.equals(name)) {
                result.setOwnership(OwnershipMapper.map(new Ownership(data)));
            } else if (STATUS_ASPECT_NAME.equals(name)) {
                result.setStatus(StatusMapper.map(new Status(data)));
            } else if (GLOBAL_TAGS_ASPECT_NAME.equals(name)) {
                com.linkedin.datahub.graphql.generated.GlobalTags globalTags = GlobalTagsMapper.map(new GlobalTags(data));
                result.setGlobalTags(globalTags);
                result.setTags(globalTags);
            } else if (INSTITUTIONAL_MEMORY_ASPECT_NAME.equals(name)) {
                result.setInstitutionalMemory(InstitutionalMemoryMapper.map(new InstitutionalMemory(data)));
            } else if (GLOSSARY_TERMS_ASPECT_NAME.equals(name)) {
                result.setGlossaryTerms(GlossaryTermsMapper.map(new GlossaryTerms(data)));
            }
        });

        return result;
    }

    /**
     * Maps GMS {@link com.linkedin.dashboard.DashboardInfo} to deprecated GraphQL {@link DashboardInfo}
     */
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
        if (info.hasExternalUrl()) {
            result.setExternalUrl(info.getExternalUrl().toString());
        } else if (info.hasDashboardUrl()) {
            // TODO: Migrate to using the External URL field for consistency.
            result.setExternalUrl(info.getDashboardUrl().toString());
        }
        if (info.hasCustomProperties()) {
            result.setCustomProperties(StringMapMapper.map(info.getCustomProperties()));
        }
        if (info.hasAccess()) {
            result.setAccess(AccessLevel.valueOf(info.getAccess().toString()));
        }
        result.setLastModified(AuditStampMapper.map(info.getLastModified().getLastModified()));
        result.setCreated(AuditStampMapper.map(info.getLastModified().getCreated()));
        if (info.getLastModified().hasDeleted()) {
            result.setDeleted(AuditStampMapper.map(info.getLastModified().getDeleted()));
        }
        return result;
    }

    /**
     * Maps GMS {@link com.linkedin.dashboard.DashboardInfo} to new GraphQL {@link DashboardProperties}
     */
    private DashboardProperties mapDashboardInfoToProperties(final com.linkedin.dashboard.DashboardInfo info) {
        final DashboardProperties result = new DashboardProperties();
        result.setDescription(info.getDescription());
        result.setName(info.getTitle());
        result.setLastRefreshed(info.getLastRefreshed());

        if (info.hasExternalUrl()) {
            result.setExternalUrl(info.getExternalUrl().toString());
        } else if (info.hasDashboardUrl()) {
            // TODO: Migrate to using the External URL field for consistency.
            result.setExternalUrl(info.getDashboardUrl().toString());
        }
        if (info.hasCustomProperties()) {
            result.setCustomProperties(StringMapMapper.map(info.getCustomProperties()));
        }
        if (info.hasAccess()) {
            result.setAccess(AccessLevel.valueOf(info.getAccess().toString()));
        }
        result.setLastModified(AuditStampMapper.map(info.getLastModified().getLastModified()));
        result.setCreated(AuditStampMapper.map(info.getLastModified().getCreated()));
        if (info.getLastModified().hasDeleted()) {
            result.setDeleted(AuditStampMapper.map(info.getLastModified().getDeleted()));
        }
        return result;
    }
}
