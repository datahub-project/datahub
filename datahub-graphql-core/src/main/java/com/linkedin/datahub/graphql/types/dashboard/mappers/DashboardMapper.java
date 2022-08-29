package com.linkedin.datahub.graphql.types.dashboard.mappers;

import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.Deprecation;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.InputFields;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.Ownership;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.dashboard.EditableDashboardProperties;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.generated.AccessLevel;
import com.linkedin.datahub.graphql.generated.Chart;
import com.linkedin.datahub.graphql.generated.Container;
import com.linkedin.datahub.graphql.generated.Dashboard;
import com.linkedin.datahub.graphql.generated.DashboardEditableProperties;
import com.linkedin.datahub.graphql.generated.DashboardInfo;
import com.linkedin.datahub.graphql.generated.DashboardProperties;
import com.linkedin.datahub.graphql.generated.DataPlatform;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.chart.mappers.InputFieldsMapper;
import com.linkedin.datahub.graphql.types.common.mappers.AuditStampMapper;
import com.linkedin.datahub.graphql.types.common.mappers.DataPlatformInstanceAspectMapper;
import com.linkedin.datahub.graphql.types.common.mappers.DeprecationMapper;
import com.linkedin.datahub.graphql.types.common.mappers.InstitutionalMemoryMapper;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.datahub.graphql.types.common.mappers.StatusMapper;
import com.linkedin.datahub.graphql.types.common.mappers.CustomPropertiesMapper;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.datahub.graphql.types.common.mappers.util.SystemMetadataUtils;
import com.linkedin.datahub.graphql.types.domain.DomainAssociationMapper;
import com.linkedin.datahub.graphql.types.glossary.mappers.GlossaryTermsMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.GlobalTagsMapper;
import com.linkedin.domain.Domains;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.key.DashboardKey;
import com.linkedin.metadata.key.DataPlatformKey;
import com.linkedin.metadata.utils.EntityKeyUtils;
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
        Urn entityUrn = entityResponse.getUrn();

        result.setUrn(entityResponse.getUrn().toString());
        result.setType(EntityType.DASHBOARD);
        EnvelopedAspectMap aspectMap = entityResponse.getAspects();
        Long lastIngested = SystemMetadataUtils.getLastIngested(aspectMap);
        result.setLastIngested(lastIngested);

        MappingHelper<Dashboard> mappingHelper = new MappingHelper<>(aspectMap, result);
        mappingHelper.mapToResult(DASHBOARD_KEY_ASPECT_NAME, this::mapDashboardKey);
        mappingHelper.mapToResult(DASHBOARD_INFO_ASPECT_NAME, (entity, dataMap) -> this.mapDashboardInfo(entity, dataMap, entityUrn));
        mappingHelper.mapToResult(EDITABLE_DASHBOARD_PROPERTIES_ASPECT_NAME, this::mapEditableDashboardProperties);
        mappingHelper.mapToResult(OWNERSHIP_ASPECT_NAME, (dashboard, dataMap) ->
            dashboard.setOwnership(OwnershipMapper.map(new Ownership(dataMap), entityUrn)));
        mappingHelper.mapToResult(STATUS_ASPECT_NAME, (dashboard, dataMap) ->
            dashboard.setStatus(StatusMapper.map(new Status(dataMap))));
        mappingHelper.mapToResult(INSTITUTIONAL_MEMORY_ASPECT_NAME, (dashboard, dataMap) ->
            dashboard.setInstitutionalMemory(InstitutionalMemoryMapper.map(new InstitutionalMemory(dataMap))));
        mappingHelper.mapToResult(GLOSSARY_TERMS_ASPECT_NAME, (dashboard, dataMap) ->
            dashboard.setGlossaryTerms(GlossaryTermsMapper.map(new GlossaryTerms(dataMap), entityUrn)));
        mappingHelper.mapToResult(CONTAINER_ASPECT_NAME, this::mapContainers);
        mappingHelper.mapToResult(DOMAINS_ASPECT_NAME, this::mapDomains);
        mappingHelper.mapToResult(DEPRECATION_ASPECT_NAME, (dashboard, dataMap) ->
            dashboard.setDeprecation(DeprecationMapper.map(new Deprecation(dataMap))));
        mappingHelper.mapToResult(GLOBAL_TAGS_ASPECT_NAME, (dataset, dataMap) -> this.mapGlobalTags(dataset, dataMap, entityUrn));
        mappingHelper.mapToResult(DATA_PLATFORM_INSTANCE_ASPECT_NAME, (dataset, dataMap) ->
            dataset.setDataPlatformInstance(DataPlatformInstanceAspectMapper.map(new DataPlatformInstance(dataMap))));
        mappingHelper.mapToResult(INPUT_FIELDS_ASPECT_NAME, (dashboard, dataMap) ->
            dashboard.setInputFields(InputFieldsMapper.map(new InputFields(dataMap), entityUrn)));

        return mappingHelper.getResult();
    }

    private void mapDashboardKey(@Nonnull Dashboard dashboard, @Nonnull DataMap dataMap) {
        final DashboardKey gmsKey = new DashboardKey(dataMap);
        dashboard.setDashboardId(gmsKey.getDashboardId());
        dashboard.setTool(gmsKey.getDashboardTool());
        dashboard.setPlatform(DataPlatform.builder()
            .setType(EntityType.DATA_PLATFORM)
            .setUrn(EntityKeyUtils
                .convertEntityKeyToUrn(new DataPlatformKey()
                    .setPlatformName(gmsKey.getDashboardTool()), DATA_PLATFORM_ENTITY_NAME).toString()).build());
    }

    private void mapDashboardInfo(@Nonnull Dashboard dashboard, @Nonnull DataMap dataMap, Urn entityUrn) {
        final com.linkedin.dashboard.DashboardInfo gmsDashboardInfo = new com.linkedin.dashboard.DashboardInfo(dataMap);
        dashboard.setInfo(mapInfo(gmsDashboardInfo, entityUrn));
        dashboard.setProperties(mapDashboardInfoToProperties(gmsDashboardInfo, entityUrn));
    }

    /**
     * Maps GMS {@link com.linkedin.dashboard.DashboardInfo} to deprecated GraphQL {@link DashboardInfo}
     */
    private DashboardInfo mapInfo(final com.linkedin.dashboard.DashboardInfo info, Urn entityUrn) {
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
            result.setCustomProperties(CustomPropertiesMapper.map(info.getCustomProperties(), entityUrn));
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
    private DashboardProperties mapDashboardInfoToProperties(final com.linkedin.dashboard.DashboardInfo info, Urn entityUrn) {
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
            result.setCustomProperties(CustomPropertiesMapper.map(info.getCustomProperties(), entityUrn));
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

    private void mapEditableDashboardProperties(@Nonnull Dashboard dashboard, @Nonnull DataMap dataMap) {
        final EditableDashboardProperties editableDashboardProperties = new EditableDashboardProperties(dataMap);
        final DashboardEditableProperties dashboardEditableProperties = new DashboardEditableProperties();
        dashboardEditableProperties.setDescription(editableDashboardProperties.getDescription());
        dashboard.setEditableProperties(dashboardEditableProperties);
    }

    private void mapGlobalTags(@Nonnull Dashboard dashboard, @Nonnull DataMap dataMap, @Nonnull Urn entityUrn) {
        com.linkedin.datahub.graphql.generated.GlobalTags globalTags = GlobalTagsMapper.map(new GlobalTags(dataMap), entityUrn);
        dashboard.setGlobalTags(globalTags);
        dashboard.setTags(globalTags);
    }

    private void mapContainers(@Nonnull Dashboard dashboard, @Nonnull DataMap dataMap) {
        final com.linkedin.container.Container gmsContainer = new com.linkedin.container.Container(dataMap);
        dashboard.setContainer(Container
            .builder()
            .setType(EntityType.CONTAINER)
            .setUrn(gmsContainer.getContainer().toString())
            .build());
    }

    private void mapDomains(@Nonnull Dashboard dashboard, @Nonnull DataMap dataMap) {
        final Domains domains = new Domains(dataMap);
        dashboard.setDomain(DomainAssociationMapper.map(domains, dashboard.getUrn()));
    }
}
