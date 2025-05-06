import React from 'react';

import StatsSidebarView, {
    StatsProps,
} from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsSidebarView';
import StatsTabContent from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/StatsTabContent';
import { useAppConfig } from '@src/app/useAppConfig';

export default function StatsTabWrapper(props: StatsProps) {
    const { config } = useAppConfig();
    const { showStatsTabRedesign } = config.featureFlags;

    return showStatsTabRedesign || true? <StatsTabContent {...props} /> : <StatsSidebarView {...props} />;
}
