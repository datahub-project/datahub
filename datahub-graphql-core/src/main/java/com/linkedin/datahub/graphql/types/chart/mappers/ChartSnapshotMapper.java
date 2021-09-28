package com.linkedin.datahub.graphql.types.chart.mappers;

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
import com.linkedin.metadata.aspect.ChartAspect;
import com.linkedin.metadata.snapshot.ChartSnapshot;

import javax.annotation.Nonnull;
import java.util.stream.Collectors;

public class ChartSnapshotMapper implements ModelMapper<ChartSnapshot, Chart> {

    public static final ChartSnapshotMapper INSTANCE = new ChartSnapshotMapper();

    public static Chart map(@Nonnull final ChartSnapshot chart) {
        return INSTANCE.apply(chart);
    }

    @Override
    public Chart apply(@Nonnull final ChartSnapshot chart) {
        final Chart result = new Chart();

        // TODO: Replace with EntityKey to avoid requiring hardcoded URN.
        result.setUrn(chart.getUrn().toString());
        result.setType(EntityType.CHART);
        result.setChartId(chart.getUrn().getChartIdEntity());
        result.setTool(chart.getUrn().getDashboardToolEntity());

        for (ChartAspect aspect : chart.getAspects()) {
            if (aspect.isChartInfo()) {
                result.setInfo(mapChartInfo(aspect.getChartInfo()));
                result.setProperties(mapChartInfoToProperties(aspect.getChartInfo()));
            } else if (aspect.isChartQuery()) {
                result.setQuery(mapChartQuery(aspect.getChartQuery()));
            } else if (aspect.isOwnership()) {
                result.setOwnership(OwnershipMapper.map(aspect.getOwnership()));
            } else if (aspect.isStatus()) {
                result.setStatus(StatusMapper.map(aspect.getStatus()));
            } else if (aspect.isGlobalTags()) {
                result.setGlobalTags(GlobalTagsMapper.map(aspect.getGlobalTags()));
                result.setTags(GlobalTagsMapper.map(aspect.getGlobalTags()));
            } else if (aspect.isEditableChartProperties()) {
                final ChartEditableProperties chartEditableProperties = new ChartEditableProperties();
                chartEditableProperties.setDescription(aspect.getEditableChartProperties().getDescription());
                result.setEditableProperties(chartEditableProperties);
            } else if (aspect.isInstitutionalMemory()) {
                result.setInstitutionalMemory(InstitutionalMemoryMapper.map(aspect.getInstitutionalMemory()));
            } else if (aspect.isGlossaryTerms()) {
                result.setGlossaryTerms(GlossaryTermsMapper.map(aspect.getGlossaryTerms()));
            }
        }

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
