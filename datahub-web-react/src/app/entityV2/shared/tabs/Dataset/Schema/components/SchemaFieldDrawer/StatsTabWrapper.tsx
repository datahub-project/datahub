/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import StatsSidebarView, {
    StatsProps,
} from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsSidebarView';
import StatsTabContent from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/StatsTabContent';
import { useAppConfig } from '@src/app/useAppConfig';

export default function StatsTabWrapper(props: StatsProps) {
    const { config } = useAppConfig();
    const { showStatsTabRedesign } = config.featureFlags;

    return showStatsTabRedesign ? <StatsTabContent {...props} /> : <StatsSidebarView {...props} />;
}
