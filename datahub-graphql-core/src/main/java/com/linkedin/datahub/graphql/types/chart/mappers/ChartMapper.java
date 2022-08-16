package com.linkedin.datahub.graphql.types.chart.mappers;

import com.linkedin.chart.EditableChartProperties;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.Deprecation;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.Ownership;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.generated.AccessLevel;
import com.linkedin.datahub.graphql.generated.Chart;
import com.linkedin.datahub.graphql.generated.ChartEditableProperties;
import com.linkedin.datahub.graphql.generated.ChartInfo;
import com.linkedin.datahub.graphql.generated.ChartProperties;
import com.linkedin.datahub.graphql.generated.ChartQuery;
import com.linkedin.datahub.graphql.generated.ChartQueryType;
import com.linkedin.datahub.graphql.generated.ChartType;
import com.linkedin.datahub.graphql.generated.Container;
import com.linkedin.datahub.graphql.generated.DataPlatform;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.EntityType;
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
import com.linkedin.metadata.key.ChartKey;
import com.linkedin.metadata.key.DataPlatformKey;
import com.linkedin.metadata.utils.EntityKeyUtils;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import static com.linkedin.metadata.Constants.*;


public class ChartMapper implements ModelMapper<EntityResponse, Chart> {

    public static final ChartMapper INSTANCE = new ChartMapper();

    public static Chart map(@Nonnull final EntityResponse entityResponse) {
        return INSTANCE.apply(entityResponse);
    }

    @Override
    public Chart apply(@Nonnull final EntityResponse entityResponse) {
        final Chart result = new Chart();
        Urn entityUrn = entityResponse.getUrn();

        result.setUrn(entityResponse.getUrn().toString());
        result.setType(EntityType.CHART);
        EnvelopedAspectMap aspectMap = entityResponse.getAspects();
        Long lastIngested = SystemMetadataUtils.getLastIngested(aspectMap);
        result.setLastIngested(lastIngested);

        MappingHelper<Chart> mappingHelper = new MappingHelper<>(aspectMap, result);
        mappingHelper.mapToResult(CHART_KEY_ASPECT_NAME, this::mapChartKey);
        mappingHelper.mapToResult(CHART_INFO_ASPECT_NAME, (entity, dataMap) -> this.mapChartInfo(entity, dataMap, entityUrn));
        mappingHelper.mapToResult(CHART_QUERY_ASPECT_NAME, this::mapChartQuery);
        mappingHelper.mapToResult(EDITABLE_CHART_PROPERTIES_ASPECT_NAME, this::mapEditableChartProperties);
        mappingHelper.mapToResult(OWNERSHIP_ASPECT_NAME, (chart, dataMap) ->
            chart.setOwnership(OwnershipMapper.map(new Ownership(dataMap), entityUrn)));
        mappingHelper.mapToResult(STATUS_ASPECT_NAME, (chart, dataMap) ->
            chart.setStatus(StatusMapper.map(new Status(dataMap))));
        mappingHelper.mapToResult(GLOBAL_TAGS_ASPECT_NAME, (dataset, dataMap) -> this.mapGlobalTags(dataset, dataMap, entityUrn));
        mappingHelper.mapToResult(INSTITUTIONAL_MEMORY_ASPECT_NAME, (chart, dataMap) ->
            chart.setInstitutionalMemory(InstitutionalMemoryMapper.map(new InstitutionalMemory(dataMap))));
        mappingHelper.mapToResult(GLOSSARY_TERMS_ASPECT_NAME, (chart, dataMap) ->
            chart.setGlossaryTerms(GlossaryTermsMapper.map(new GlossaryTerms(dataMap), entityUrn)));
        mappingHelper.mapToResult(CONTAINER_ASPECT_NAME, this::mapContainers);
        mappingHelper.mapToResult(DOMAINS_ASPECT_NAME, this::mapDomains);
        mappingHelper.mapToResult(DEPRECATION_ASPECT_NAME, (chart, dataMap) ->
            chart.setDeprecation(DeprecationMapper.map(new Deprecation(dataMap))));
        mappingHelper.mapToResult(DATA_PLATFORM_INSTANCE_ASPECT_NAME, (dataset, dataMap) ->
            dataset.setDataPlatformInstance(DataPlatformInstanceAspectMapper.map(new DataPlatformInstance(dataMap))));

        return mappingHelper.getResult();
    }

    private void mapChartKey(@Nonnull Chart chart, @Nonnull DataMap dataMap) {
        final ChartKey gmsKey = new ChartKey(dataMap);
        chart.setChartId(gmsKey.getChartId());
        chart.setTool(gmsKey.getDashboardTool());
        chart.setPlatform(DataPlatform.builder()
            .setType(EntityType.DATA_PLATFORM)
            .setUrn(EntityKeyUtils
                .convertEntityKeyToUrn(new DataPlatformKey()
                    .setPlatformName(gmsKey.getDashboardTool()), DATA_PLATFORM_ENTITY_NAME).toString()).build());
    }

    private void mapChartInfo(@Nonnull Chart chart, @Nonnull DataMap dataMap, @Nonnull Urn entityUrn) {
        final com.linkedin.chart.ChartInfo gmsChartInfo = new com.linkedin.chart.ChartInfo(dataMap);
        chart.setInfo(mapInfo(gmsChartInfo, entityUrn));
        chart.setProperties(mapChartInfoToProperties(gmsChartInfo, entityUrn));
    }

    /**
     * Maps GMS {@link com.linkedin.chart.ChartInfo} to deprecated GraphQL {@link ChartInfo}
     */
    private ChartInfo mapInfo(final com.linkedin.chart.ChartInfo info, @Nonnull Urn entityUrn) {
        final ChartInfo result = new ChartInfo();
        result.setDescription(info.getDescription());
        result.setName(info.getTitle());
        result.setLastRefreshed(info.getLastRefreshed());

        if (info.hasInputs()) {
            result.setInputs(info.getInputs().stream().map(input -> {
                final Dataset dataset = new Dataset();
                dataset.setUrn(input.getDatasetUrn().toString());
                return dataset;
            }).collect(Collectors.toList()));
        }

        if (info.hasAccess()) {
            result.setAccess(AccessLevel.valueOf(info.getAccess().toString()));
        }
        if (info.hasType()) {
            result.setType(ChartType.valueOf(info.getType().toString()));
        }
        result.setLastModified(AuditStampMapper.map(info.getLastModified().getLastModified()));
        result.setCreated(AuditStampMapper.map(info.getLastModified().getCreated()));
        if (info.getLastModified().hasDeleted()) {
            result.setDeleted(AuditStampMapper.map(info.getLastModified().getDeleted()));
        }
        if (info.hasExternalUrl()) {
            result.setExternalUrl(info.getExternalUrl().toString());
        } else if (info.hasChartUrl()) {
            // TODO: Migrate to using the External URL field for consistency.
            result.setExternalUrl(info.getChartUrl().toString());
        }
        if (info.hasCustomProperties()) {
            result.setCustomProperties(CustomPropertiesMapper.map(info.getCustomProperties(), entityUrn));
        }
        return result;
    }

    /**
     * Maps GMS {@link com.linkedin.chart.ChartInfo} to new GraphQL {@link ChartProperties}
     */
    private ChartProperties mapChartInfoToProperties(final com.linkedin.chart.ChartInfo info, @Nonnull Urn entityUrn) {
        final ChartProperties result = new ChartProperties();
        result.setDescription(info.getDescription());
        result.setName(info.getTitle());
        result.setLastRefreshed(info.getLastRefreshed());

        if (info.hasAccess()) {
            result.setAccess(AccessLevel.valueOf(info.getAccess().toString()));
        }
        if (info.hasType()) {
            result.setType(ChartType.valueOf(info.getType().toString()));
        }
        result.setLastModified(AuditStampMapper.map(info.getLastModified().getLastModified()));
        result.setCreated(AuditStampMapper.map(info.getLastModified().getCreated()));
        if (info.getLastModified().hasDeleted()) {
            result.setDeleted(AuditStampMapper.map(info.getLastModified().getDeleted()));
        }
        if (info.hasExternalUrl()) {
            result.setExternalUrl(info.getExternalUrl().toString());
        } else if (info.hasChartUrl()) {
            // TODO: Migrate to using the External URL field for consistency.
            result.setExternalUrl(info.getChartUrl().toString());
        }
        if (info.hasCustomProperties()) {
            result.setCustomProperties(CustomPropertiesMapper.map(info.getCustomProperties(), entityUrn));
        }
        return result;
    }

    private void mapChartQuery(@Nonnull Chart chart, @Nonnull DataMap dataMap) {
        final com.linkedin.chart.ChartQuery gmsChartQuery = new com.linkedin.chart.ChartQuery(dataMap);
        chart.setQuery(mapQuery(gmsChartQuery));
    }

    private ChartQuery mapQuery(final com.linkedin.chart.ChartQuery query) {
        final ChartQuery result = new ChartQuery();
        result.setRawQuery(query.getRawQuery());
        result.setType(ChartQueryType.valueOf(query.getType().toString()));
        return result;
    }

    private void mapEditableChartProperties(@Nonnull Chart chart, @Nonnull DataMap dataMap) {
        final EditableChartProperties editableChartProperties = new EditableChartProperties(dataMap);
        final ChartEditableProperties chartEditableProperties = new ChartEditableProperties();
        chartEditableProperties.setDescription(editableChartProperties.getDescription());
        chart.setEditableProperties(chartEditableProperties);
    }

    private void mapGlobalTags(@Nonnull Chart chart, @Nonnull DataMap dataMap, @Nonnull Urn entityUrn) {
        com.linkedin.datahub.graphql.generated.GlobalTags globalTags = GlobalTagsMapper.map(new GlobalTags(dataMap), entityUrn);
        chart.setGlobalTags(globalTags);
        chart.setTags(globalTags);
    }

    private void mapContainers(@Nonnull Chart chart, @Nonnull DataMap dataMap) {
        final com.linkedin.container.Container gmsContainer = new com.linkedin.container.Container(dataMap);
        chart.setContainer(Container
            .builder()
            .setType(EntityType.CONTAINER)
            .setUrn(gmsContainer.getContainer().toString())
            .build());
    }

    private void mapDomains(@Nonnull Chart chart, @Nonnull DataMap dataMap) {
        final Domains domains = new Domains(dataMap);
        chart.setDomain(DomainAssociationMapper.map(domains, chart.getUrn()));
    }
}
