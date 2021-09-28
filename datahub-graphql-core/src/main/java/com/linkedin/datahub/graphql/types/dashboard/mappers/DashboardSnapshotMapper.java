package com.linkedin.datahub.graphql.types.dashboard.mappers;

import com.linkedin.common.GlobalTags;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.Ownership;
import com.linkedin.common.Status;
import com.linkedin.dashboard.EditableDashboardProperties;
import com.linkedin.datahub.graphql.generated.AccessLevel;
import com.linkedin.datahub.graphql.generated.Chart;
import com.linkedin.datahub.graphql.generated.Dashboard;
import com.linkedin.datahub.graphql.generated.DashboardInfo;
import com.linkedin.datahub.graphql.generated.DashboardProperties;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.DashboardEditableProperties;
import com.linkedin.datahub.graphql.types.common.mappers.AuditStampMapper;
import com.linkedin.datahub.graphql.types.common.mappers.InstitutionalMemoryMapper;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.datahub.graphql.types.common.mappers.StatusMapper;
import com.linkedin.datahub.graphql.types.common.mappers.StringMapMapper;
import com.linkedin.datahub.graphql.types.glossary.mappers.GlossaryTermsMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.GlobalTagsMapper;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.snapshot.DashboardSnapshot;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;


public class DashboardSnapshotMapper implements ModelMapper<DashboardSnapshot, Dashboard> {

    public static final DashboardSnapshotMapper INSTANCE = new DashboardSnapshotMapper();

    public static Dashboard map(@Nonnull final DashboardSnapshot dashboard) {
        return INSTANCE.apply(dashboard);
    }

    @Override
    public Dashboard apply(@Nonnull final DashboardSnapshot dashboard) {
        final Dashboard result = new Dashboard();
        result.setUrn(dashboard.getUrn().toString());
        result.setType(EntityType.DASHBOARD);
        result.setDashboardId(dashboard.getUrn().getDashboardIdEntity());
        result.setTool(dashboard.getUrn().getDashboardToolEntity());

        ModelUtils.getAspectsFromSnapshot(dashboard).forEach(aspect -> {
            if (aspect instanceof com.linkedin.dashboard.DashboardInfo) {
                com.linkedin.dashboard.DashboardInfo info = com.linkedin.dashboard.DashboardInfo.class.cast(aspect);
                result.setInfo(mapDashboardInfo(info));
                result.setProperties(mapDashboardInfoToProperties(info));
            } else if (aspect instanceof Ownership) {
                Ownership ownership = Ownership.class.cast(aspect);
                result.setOwnership(OwnershipMapper.map(ownership));
            } else if (aspect instanceof Status) {
                Status status = Status.class.cast(aspect);
                result.setStatus(StatusMapper.map(status));
            } else if (aspect instanceof GlobalTags) {
                result.setGlobalTags(GlobalTagsMapper.map(GlobalTags.class.cast(aspect)));
                result.setTags(GlobalTagsMapper.map(GlobalTags.class.cast(aspect)));
            } else if (aspect instanceof EditableDashboardProperties) {
                final DashboardEditableProperties dashboardEditableProperties = new DashboardEditableProperties();
                dashboardEditableProperties.setDescription(((EditableDashboardProperties) aspect).getDescription());
                result.setEditableProperties(dashboardEditableProperties);
            } else if (aspect instanceof InstitutionalMemory) {
                result.setInstitutionalMemory(InstitutionalMemoryMapper.map((InstitutionalMemory) aspect));
            } else if (aspect instanceof GlossaryTerms) {
                result.setGlossaryTerms(GlossaryTermsMapper.map((GlossaryTerms) aspect));
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
