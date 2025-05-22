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
            {showStatsTabRedesign || true ? <StatsTabV2 /> : <StatsTab />}
        </StatsSectionsContextProvider>
    );
};

export default StatsTabWrapper;
