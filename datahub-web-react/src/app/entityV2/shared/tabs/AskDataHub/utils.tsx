import { Sparkle } from '@phosphor-icons/react';
import React from 'react';

import AskDataHubTabWithProvider from '@app/entityV2/shared/tabs/AskDataHub/AskDataHubTabWithProvider';
import { EntitySidebarTab, TabContextType } from '@app/entityV2/shared/types';

/**
 * Adds Ask DataHub tab to sidebar tabs when enabled
 * This tab provides a gateway to the full chat experience with entity context
 */
export function withAskDataHubTab(
    tabs: EntitySidebarTab[],
    isAskDataHubEnabled: boolean,
    contextType?: TabContextType,
): EntitySidebarTab[] {
    // Show Ask DataHub in main contexts where users interact with individual entities
    // Excluded only from lineage view due to space constraints
    const shouldShowAskDataHub =
        isAskDataHubEnabled &&
        (contextType === TabContextType.CHROME_SIDEBAR ||
            contextType === TabContextType.PROFILE_SIDEBAR ||
            contextType === TabContextType.SEARCH_SIDEBAR);

    if (!shouldShowAskDataHub) {
        return tabs;
    }

    const askDataHubTab: EntitySidebarTab = {
        name: 'Ask',
        component: AskDataHubTabWithProvider, // Wrapped with provider to scope context
        description: 'Ask questions about this asset using AI',
        icon: Sparkle,
        selectedIcon: Sparkle,
        display: {
            visible: () => true,
            enabled: () => true,
        },
        id: 'ask-datahub',
    };

    // Position last to avoid disrupting users' muscle memory of existing tab locations
    return [...tabs, askDataHubTab];
}

/**
 * Hook to get Ask DataHub-enabled sidebar tabs
 */
export function useAskDataHubSidebarTabs(
    baseTabs: EntitySidebarTab[],
    isAskDataHubEnabled: boolean,
    contextType?: TabContextType,
): EntitySidebarTab[] {
    return React.useMemo(
        () => withAskDataHubTab(baseTabs, isAskDataHubEnabled, contextType),
        [baseTabs, isAskDataHubEnabled, contextType],
    );
}
