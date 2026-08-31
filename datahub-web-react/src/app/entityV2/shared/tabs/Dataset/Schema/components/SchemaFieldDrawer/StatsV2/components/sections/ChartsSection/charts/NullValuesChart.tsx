import React from 'react';
import { useTranslation } from 'react-i18next';

import MetricWithProportionLineChart from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/ChartsSection/charts/components/MetricWithProportionLineChart';
import useStatsTabContext from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/hooks/useStatsTabContext';

const NULL_COUNT_METRIC = 'nullCount';
const NULL_PROPORTION_METRIC = 'nullProportion';

export default function NullValuesChart() {
    const { t } = useTranslation('entity.profile.schema');
    const { properties } = useStatsTabContext();

    // Show metric for nullable fields only
    if (!properties?.expandedField?.nullable) return null;

    return (
        <MetricWithProportionLineChart
            metric={NULL_COUNT_METRIC}
            proportionMetric={NULL_PROPORTION_METRIC}
            title={t('statsV2Charts.nullValuesTitle')}
            subTitle={t('statsV2Charts.nullValuesSubtitle')}
        />
    );
}
