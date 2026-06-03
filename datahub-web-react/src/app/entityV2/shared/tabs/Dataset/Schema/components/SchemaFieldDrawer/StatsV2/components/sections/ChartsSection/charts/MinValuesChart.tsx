import React from 'react';
import { useTranslation } from 'react-i18next';

import MetricLineChart from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/ChartsSection/charts/components/MetricLineChart';
import useStatsTabContext from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/hooks/useStatsTabContext';
import { SchemaFieldDataType } from '@src/types.generated';

const MIN_METRIC = 'min';

export default function MinValuesChart() {
    const { t } = useTranslation('entity.profile.schema');
    const { properties } = useStatsTabContext();
    const fieldType = properties?.expandedField?.type;

    // Only number type supported
    if (fieldType !== SchemaFieldDataType.Number) return null;

    return (
        <MetricLineChart
            metric={MIN_METRIC}
            title={t('statsV2Charts.minValuesTitle')}
            subTitle={t('statsV2Charts.minValuesSubtitle')}
            dataAggregationFunction={(values) => Math.min(...values)}
        />
    );
}
