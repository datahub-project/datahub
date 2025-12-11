/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import MetricWithProportionLineChart from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/ChartsSection/charts/components/MetricWithProportionLineChart';

export default function DistinctValuesChart() {
    return (
        <MetricWithProportionLineChart
            metric="uniqueCount"
            proportionMetric="uniqueProportion"
            title="Distinct Values"
            subTitle="Number of rows with distinct values for this column"
        />
    );
}
