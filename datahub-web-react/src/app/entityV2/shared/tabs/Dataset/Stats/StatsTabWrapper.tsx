/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import StatsTab from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTab';
import { StatsSectionsContextProvider } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/StatsSectionsContext';
import StatsTabV2 from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/StatsTabV2';
import { useAppConfig } from '@src/app/useAppConfig';

const StatsTabWrapper = () => {
    const { config } = useAppConfig();
    const { showStatsTabRedesign } = config.featureFlags;
    return (
        <StatsSectionsContextProvider>
            {showStatsTabRedesign ? <StatsTabV2 /> : <StatsTab />}
        </StatsSectionsContextProvider>
    );
};

export default StatsTabWrapper;
