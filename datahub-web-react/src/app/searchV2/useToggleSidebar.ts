import { EventType } from '../analytics';
import analytics from '../analytics/analytics';
import { useUserContext } from '../context/useUserContext';
import useToggle from '../shared/useToggle';

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
