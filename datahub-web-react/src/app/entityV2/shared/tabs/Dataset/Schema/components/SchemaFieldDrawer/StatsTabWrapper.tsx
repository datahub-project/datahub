import React from 'react';
import { useAppConfig } from '@src/app/useAppConfig';
import StatsTabContent from './StatsV2/StatsTabContent';
import StatsSidebarView, { StatsProps } from './StatsSidebarView';

export default function StatsTabWrapper(props: StatsProps) {
    const { config } = useAppConfig();
    const { showStatsTabRedesign } = config.featureFlags;

    return showStatsTabRedesign ? <StatsTabContent {...props} /> : <StatsSidebarView {...props} />;
}
