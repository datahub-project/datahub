/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { EventType } from '@app/analytics';
import analytics from '@app/analytics/analytics';
import { useUserContext } from '@app/context/useUserContext';
import useToggle from '@app/shared/useToggle';

const useToggleSidebar = () => {
    const { localState, updateLocalState } = useUserContext();

    const { isOpen: isSidebarOpen, toggle: toggleSidebar } = useToggle({
        initialValue: localState.showBrowseV2Sidebar ?? true,
        onToggle: (isNowOpen: boolean) => {
            analytics.event({
                type: EventType.BrowseV2ToggleSidebarEvent,
                action: isNowOpen ? 'open' : 'close',
            });
            updateLocalState({ ...localState, showBrowseV2Sidebar: isNowOpen });
        },
    });

    return { isSidebarOpen, toggleSidebar } as const;
};

export default useToggleSidebar;
