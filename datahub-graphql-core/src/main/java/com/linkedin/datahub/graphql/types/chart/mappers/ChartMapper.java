package com.linkedin.datahub.graphql.types.chart.mappers;

import com.linkedin.chart.EditableChartProperties;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.Ownership;
import com.linkedin.common.Status;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.generated.AccessLevel;
import com.linkedin.datahub.graphql.generated.Chart;
import com.linkedin.datahub.graphql.generated.ChartInfo;
import com.linkedin.datahub.graphql.generated.ChartProperties;
import com.linkedin.datahub.graphql.generated.ChartQuery;
import com.linkedin.datahub.graphql.generated.ChartQueryType;
import com.linkedin.datahub.graphql.generated.ChartType;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.ChartEditableProperties;
import com.linkedin.datahub.graphql.types.common.mappers.AuditStampMapper;
import com.linkedin.datahub.graphql.types.common.mappers.InstitutionalMemoryMapper;
import com.linkedin.datahub.graphql.types.common.mappers.StringMapMapper;
import com.linkedin.datahub.graphql.types.glossary.mappers.GlossaryTermsMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.GlobalTagsMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.datahub.graphql.types.common.mappers.StatusMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.metadata.key.ChartKey;

import javax.annotation.Nonnull;
import java.util.stream.Collectors;

import static com.linkedin.metadata.Constants.*;


public class ChartMapper implements ModelMapper<EntityResponse, Chart> {

    public static final ChartMapper INSTANCE = new ChartMapper();

    public static Chart map(@Nonnull final EntityResponse entityResponse) {
        return INSTANCE.apply(entityResponse);
    }

    @Override
    public Chart apply(@Nonnull final EntityResponse entityResponse) {
        final Chart result = new Chart();

        result.setUrn(entityResponse.getUrn().toString());
        result.setType(EntityType.CHART);

        entityResponse.getAspects().forEach((name, aspect) -> {
            DataMap data = aspect.getValue().data();
            if (CHART_KEY_ASPECT_NAME.equals(name)) {
                final ChartKey gmsKey = new ChartKey(data);
                result.setChartId(gmsKey.getChartId());
                result.setTool(gmsKey.getDashboardTool());
            } else if (CHART_INFO_ASPECT_NAME.equals(name)) {
                final com.linkedin.chart.ChartInfo gmsChartInfo = new com.linkedin.chart.ChartInfo(data);
                result.setInfo(mapChartInfo(gmsChartInfo));
                result.setProperties(mapChartInfoToProperties(gmsChartInfo));
            } else if (CHART_QUERY_ASPECT_NAME.equals(name)) {
                final com.linkedin.chart.ChartQuery gmsChartQuery = new com.linkedin.chart.ChartQuery(data);
                result.setQuery(mapChartQuery(gmsChartQuery));
            } else if (EDITABLE_CHART_PROPERTIES_ASPECT_NAME.equals(name)) {
                final EditableChartProperties editableChartProperties = new EditableChartProperties(data);
                final ChartEditableProperties chartEditableProperties = new ChartEditableProperties();
                chartEditableProperties.setDescription(editableChartProperties.getDescription());
                result.setEditableProperties(chartEditableProperties);
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
     * Maps GMS {@link com.linkedin.chart.ChartInfo} to deprecated GraphQL {@link ChartInfo}
     */
    private ChartInfo mapChartInfo(final com.linkedin.chart.ChartInfo info) {
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
            result.setCustomProperties(StringMapMapper.map(info.getCustomProperties()));
        }
        return result;
    }

    /**
     * Maps GMS {@link com.linkedin.chart.ChartInfo} to new GraphQL {@link ChartProperties}
     */
    private ChartProperties mapChartInfoToProperties(final com.linkedin.chart.ChartInfo info) {
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
            result.setCustomProperties(StringMapMapper.map(info.getCustomProperties()));
        }
        return result;
    }

    private ChartQuery mapChartQuery(final com.linkedin.chart.ChartQuery query) {
        final ChartQuery result = new ChartQuery();
        result.setRawQuery(query.getRawQuery());
        result.setType(ChartQueryType.valueOf(query.getType().toString()));
        return result;
    }
}
