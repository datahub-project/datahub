/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React, { useMemo } from 'react';

import useStatsTabContext from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/hooks/useStatsTabContext';
import { GraphCard, WhiskerChart } from '@src/alchemy-components';
import { WhiskerDatum } from '@src/alchemy-components/components/WhiskerChart/types';

export default function NumericDistributionChart() {
    const { properties } = useStatsTabContext();
    const fieldProfile = properties?.fieldProfile;

    const whiskerData: WhiskerDatum | null = useMemo(() => {
        const min = Number(fieldProfile?.min);
        const firstQuartile = Number(fieldProfile?.quantiles?.find((entry) => entry.quantile === '0.25')?.value);
        const median = Number(fieldProfile?.median);
        const thirdQuartile = Number(fieldProfile?.quantiles?.find((entry) => entry.quantile === '0.75')?.value);
        const max = Number(fieldProfile?.max);

        if ([min, firstQuartile, median, thirdQuartile, max].filter((metric) => Number.isNaN(metric)).length > 0)
            return null;

        return {
            key: 'numeric-distribution',
            min,
            firstQuartile,
            median,
            thirdQuartile,
            max,
        };
    }, [fieldProfile]);

    if (whiskerData === null) return null;

    return (
        <GraphCard
            title="Numeric Column Distribution"
            subTitle="Numeric distribution for this column"
            graphHeight="200px"
            renderGraph={() => <WhiskerChart data={[whiskerData]} axisLabel="Column Value" />}
        />
    );
}
