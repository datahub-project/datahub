package com.linkedin.datahub.graphql.types.mappers;

import com.linkedin.datahub.graphql.generated.AccessLevel;
import com.linkedin.datahub.graphql.generated.Chart;
import com.linkedin.datahub.graphql.generated.ChartInfo;
import com.linkedin.datahub.graphql.generated.ChartQuery;
import com.linkedin.datahub.graphql.generated.ChartQueryType;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.EntityType;

import javax.annotation.Nonnull;
import java.util.stream.Collectors;

public class ChartMapper implements ModelMapper<com.linkedin.dashboard.Chart, Chart> {

    public static final ChartMapper INSTANCE = new ChartMapper();

    public static Chart map(@Nonnull final com.linkedin.dashboard.Chart chart) {
        return INSTANCE.apply(chart);
    }

    @Override
    public Chart apply(@Nonnull final com.linkedin.dashboard.Chart chart) {
        final Chart result = new Chart();
        result.setUrn(chart.getUrn().toString());
        result.setType(EntityType.CHART);
        result.setChartId(chart.getChartId());
        result.setTool(chart.getTool());
        if (chart.hasInfo()) {
            result.setInfo(mapChartInfo(chart.getInfo()));
        }
        if (chart.hasQuery()) {
            result.setQuery(mapChartQuery(chart.getQuery()));
        }
        if (chart.hasOwnership()) {
            result.setOwnership(OwnershipMapper.map(chart.getOwnership()));
        }
        if (chart.hasStatus()) {
            result.setStatus(StatusMapper.map(chart.getStatus()));
        }
        return result;
    }

    private ChartInfo mapChartInfo(final com.linkedin.chart.ChartInfo info) {
        final ChartInfo result = new ChartInfo();
        result.setDescription(info.getDescription());
        result.setTitle(info.getTitle());
        result.setLastRefreshed(info.getLastRefreshed());
        result.setInputs(info.getInputs().stream().map(input -> {
            final Dataset dataset = new Dataset();
            dataset.setUrn(input.getDatasetUrn().toString());
            return dataset;
        }).collect(Collectors.toList()));

        if (info.hasAccess()) {
            result.setAccess(AccessLevel.valueOf(info.getAccess().toString()));
        }
        if (info.hasChartUrl()) {
            result.setUrl(info.getChartUrl().toString());
        }
        result.setLastModified(AuditStampMapper.map(info.getLastModified().getLastModified()));
        result.setCreated(AuditStampMapper.map(info.getLastModified().getCreated()));
        if (info.getLastModified().hasDeleted()) {
            result.setDeleted(AuditStampMapper.map(info.getLastModified().getDeleted()));
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
