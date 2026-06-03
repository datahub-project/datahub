import React from 'react';
import { useTranslation } from 'react-i18next';

import MetricLineChart from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/ChartsSection/charts/components/MetricLineChart';
import useStatsTabContext from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/hooks/useStatsTabContext';
import { SchemaFieldDataType } from '@src/types.generated';

const MEDIAN_METRIC = 'median';

export default function MedianValuesChart() {
    const { t } = useTranslation('entity.profile.schema');
    const { properties } = useStatsTabContext();
    const fieldType = properties?.expandedField?.type;

    // Only number type supported
    if (fieldType !== SchemaFieldDataType.Number) return null;

    return (
        <MetricLineChart
            metric={MEDIAN_METRIC}
            title={t('statsV2Charts.medianValuesTitle')}
            subTitle={t('statsV2Charts.medianValuesSubtitle')}
        />
    );
}
