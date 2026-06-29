import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { StatsProps } from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsSidebarView';
import Metric from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/StatsAndInsights/components/Metric';
import useStatsTabContext from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/hooks/useStatsTabContext';
import { formatNumber } from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/utils';
import { isValuePresent } from '@src/app/entityV2/shared/containers/profile/sidebar/shared/utils';
import { SchemaFieldDataType } from '@src/types.generated';

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

const EMPTY_VALUE = '-';

function useMetricRenderingRules(): MetricRenderingRules[] {
    const { t } = useTranslation('entity.profile.schema');
    return [
        {
            label: t('statsV2Insights.nullCount'),
            key: 'null-count',
            value: (props) => formatNumber(props?.fieldProfile?.nullCount),
            isHidden: (props) => !props?.expandedField?.nullable,
        },
        {
            label: t('statsV2Insights.nullPercent'),
            key: 'null-percent',
            value: (props) => {
                const nullProportion = props?.fieldProfile?.nullProportion;
                if (!isValuePresent(nullProportion)) return null;
                const nullPercent = (nullProportion as number) * 100;
                return formatNumber(nullPercent)?.concat('%');
            },
            isHidden: (props) => !props?.expandedField?.nullable,
        },
        {
            label: t('statsV2Insights.distinctCount'),
            key: 'distinct-count',
            value: (props) => formatNumber(props?.fieldProfile?.uniqueCount),
        },
        {
            label: t('statsV2Insights.distinctPercent'),
            key: 'distinct-percent',
            value: (props) => {
                const uniqueProportion = props?.fieldProfile?.uniqueProportion;
                if (!isValuePresent(uniqueProportion)) return null;
                const uniquePercent = (uniqueProportion as number) * 100;
                return formatNumber(uniquePercent)?.concat('%');
            },
        },
        {
            label: t('statsV2Insights.max'),
            key: 'max',
            value: (props) => {
                const max = props?.fieldProfile?.max;
                if (!isValuePresent(max)) return null;
                return formatNumber(parseFloat(max as string));
            },
            isHidden: (props) => {
                const fieldType = props?.expandedField?.type;
                if (fieldType === undefined) return true;
                return ![SchemaFieldDataType.Number, SchemaFieldDataType.Date, SchemaFieldDataType.Time].includes(
                    fieldType,
                );
            },
        },
        {
            label: t('statsV2Insights.min'),
            key: 'min',
            value: (props) => {
                const min = props?.fieldProfile?.min;
                if (!isValuePresent(min)) return null;
                return formatNumber(parseFloat(min as string));
            },
            isHidden: (props) => {
                const fieldType = props?.expandedField?.type;
                if (fieldType === undefined) return true;
                return ![SchemaFieldDataType.Number, SchemaFieldDataType.Date, SchemaFieldDataType.Time].includes(
                    fieldType,
                );
            },
        },
        {
            label: t('statsV2Insights.median'),
            key: 'median',
            value: (props) => {
                const median = props?.fieldProfile?.median;
                if (!isValuePresent(median)) return null;
                return formatNumber(parseFloat(median as string));
            },
            isHidden: (props) => props?.expandedField?.type !== SchemaFieldDataType.Number,
        },
        {
            label: t('statsV2Insights.standardDeviation'),
            key: 'standard-deviation',
            value: (props) => {
                const stdev = parseFloat(`${props?.fieldProfile?.stdev}`);
                const mean = parseFloat(`${props?.fieldProfile?.mean}`);

                if (Number.isNaN(stdev) || Number.isNaN(mean)) return null;
                const stdevPercent = (stdev / mean) * 100;

                return formatNumber(stdevPercent)?.concat('%');
            },
            isHidden: (props) => props?.expandedField?.type !== SchemaFieldDataType.Number,
        },
    ];
}

export default function Metrics() {
    const { properties } = useStatsTabContext();
    const metrics = useMetricRenderingRules();

    return (
        <MetricsContainer>
            {metrics.map((metric) => {
                if (metric.isHidden?.(properties)) return null;
                const value = metric.value(properties);
                return (
                    <Metric
                        label={metric.label}
                        value={value || EMPTY_VALUE}
                        key={metric.key}
                        dataTestId={metric.key}
                    />
                );
            })}
        </MetricsContainer>
    );
}
