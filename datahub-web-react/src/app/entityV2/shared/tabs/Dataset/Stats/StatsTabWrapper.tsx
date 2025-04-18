import { useAppConfig } from '@src/app/useAppConfig';
import React from 'react';
import StatsTab from './StatsTab';
import { StatsSectionsContextProvider } from './StatsTabV2/StatsSectionsContext';
import StatsTabV2 from './StatsTabV2/StatsTabV2';

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
