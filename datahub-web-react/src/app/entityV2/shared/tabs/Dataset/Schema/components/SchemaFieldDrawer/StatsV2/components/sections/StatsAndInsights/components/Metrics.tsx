import React from 'react';
import styled from 'styled-components';
import { isValuePresent } from '@src/app/entityV2/shared/containers/profile/sidebar/shared/utils';
import { StatsProps } from '../../../../../StatsSidebarView';
import useStatsTabContext from '../../../../hooks/useStatsTabContext';
import { formatNumber } from '../../../../utils';
import Metric from './Metric';

const MetricsContainer = styled.div`
    display: flex;
    flex-wrap: wrap;

    & div {
        // two metrics in a row
        flex: 50%;
    }
`;

interface MetricRenderingRules {
    label: string;
    key: string;
    value: (properties?: StatsProps['properties']) => string | undefined | null;
    isHidden?: (properties?: StatsProps['properties']) => boolean;
}

const METRICS: MetricRenderingRules[] = [
    {
        label: 'Null Count',
        key: 'null-count',
        value: (properties) => formatNumber(properties?.fieldProfile?.nullCount),
        isHidden: (properties) => !properties?.expandedField?.nullable,
    },
    {
        label: 'Null %',
        key: 'null-percent',
        value: (properties) => {
            const nullProportion = properties?.fieldProfile?.nullProportion;
            if (!isValuePresent(nullProportion)) return null;
            const nullPercent = (nullProportion as number) * 100;
            return formatNumber(nullPercent)?.concat('%');
        },
        isHidden: (properties) => !properties?.expandedField?.nullable,
    },
    {
        label: 'Distinct Count',
        key: 'distinct-count',
        value: (properties) => formatNumber(properties?.fieldProfile?.uniqueCount),
    },
    {
        label: 'Distinct %',
        key: 'distinct-percent',
        value: (properties) => {
            const uniqueProportion = properties?.fieldProfile?.uniqueProportion;
            if (!isValuePresent(uniqueProportion)) return null;
            const uniquePercent = (uniqueProportion as number) * 100;
            return formatNumber(uniquePercent)?.concat('%');
        },
    },
    {
        label: 'Max',
        key: 'max',
        value: (properties) => {
            const max = properties?.fieldProfile?.max;
            if (!isValuePresent(max)) return null;
            return formatNumber(parseFloat(max as string));
        },
    },
    {
        label: 'Min',
        key: 'min',
        value: (properties) => {
            const min = properties?.fieldProfile?.min;
            if (!isValuePresent(min)) return null;
            return formatNumber(parseFloat(min as string));
        },
    },
    {
        label: 'Median',
        key: 'median',
        value: (properties) => {
            const median = properties?.fieldProfile?.median;
            if (!isValuePresent(median)) return null;
            return formatNumber(parseFloat(median as string));
        },
    },
    {
        label: 'Standard Deviation',
        key: 'standard-deviation',
        value: (properties) => {
            const stdev = parseFloat(`${properties?.fieldProfile?.stdev}`);
            const mean = parseFloat(`${properties?.fieldProfile?.mean}`);

            if (Number.isNaN(stdev) || Number.isNaN(mean)) return null;
            const stdevPercent = (stdev / mean) * 100;

            return formatNumber(stdevPercent)?.concat('%');
        },
    },
    // TODO: add empty string metric
];

export default function Metrics() {
    const { properties } = useStatsTabContext();

    return (
        <MetricsContainer>
            {METRICS.map((metric) => {
                if (metric.isHidden?.(properties)) return null;

                const value = metric.value(properties);

                if (!isValuePresent(value)) return null;

                return <Metric label={metric.label} value={value as string} key={metric.key} dataTestId={metric.key} />;
            })}
        </MetricsContainer>
    );
}
