import React from 'react';
import { useTranslation } from 'react-i18next';

import MetricWithProportionLineChart from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/ChartsSection/charts/components/MetricWithProportionLineChart';

const DISTINCT_COUNT_METRIC = 'uniqueCount';
const DISTINCT_PROPORTION_METRIC = 'uniqueProportion';

export default function DistinctValuesChart() {
    const { t } = useTranslation('entity.profile.schema');

    return (
        <MetricWithProportionLineChart
            metric={DISTINCT_COUNT_METRIC}
            proportionMetric={DISTINCT_PROPORTION_METRIC}
            title={t('statsV2Charts.distinctValuesTitle')}
            subTitle={t('statsV2Charts.distinctValuesSubtitle')}
        />
    );
}
