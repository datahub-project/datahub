import { useAppConfig } from '@src/app/useAppConfig';
import React from 'react';
import StatsTabV2 from './StatsTabV2/StatsTabV2';
import StatsTab from './StatsTab';

const StatsTabWrapper = () => {
    const { config } = useAppConfig();
    const { showStatsTabRedesign } = config.featureFlags;
    return <>{showStatsTabRedesign ? <StatsTabV2 /> : <StatsTab />}</>;
};

export default StatsTabWrapper;
