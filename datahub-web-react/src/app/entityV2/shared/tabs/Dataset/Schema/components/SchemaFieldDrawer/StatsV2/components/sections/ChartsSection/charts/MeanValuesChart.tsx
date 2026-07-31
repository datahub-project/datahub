import React from 'react';
import { useTranslation } from 'react-i18next';

import MetricLineChart from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/ChartsSection/charts/components/MetricLineChart';
import useStatsTabContext from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/hooks/useStatsTabContext';
import { SchemaFieldDataType } from '@src/types.generated';

const MEAN_METRIC = 'mean';

export default function MeanValuesChart() {
    const { t } = useTranslation('entity.profile.schema');
    const { properties } = useStatsTabContext();
    const fieldType = properties?.expandedField?.type;

    // Only number type supported
    if (fieldType !== SchemaFieldDataType.Number) return null;

    return (
        <MetricLineChart
            metric={MEAN_METRIC}
            title={t('statsV2Charts.meanValuesTitle')}
            subTitle={t('statsV2Charts.meanValuesSubtitle')}
        />
    );
}
