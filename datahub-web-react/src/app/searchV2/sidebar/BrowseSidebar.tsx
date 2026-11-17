import React, { useCallback, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { SEARCH_RESULTS_BROWSE_SIDEBAR_ID } from '@app/onboarding/config/SearchOnboardingConfig';
import { useIsPlatformBrowseMode } from '@app/searchV2/sidebar/BrowseContext';
import EntityBrowse from '@app/searchV2/sidebar/EntityBrowse';
import PlatformBrowse from '@app/searchV2/sidebar/PlatformBrowse';
import HierarchicalBrowseSidebar from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/HierarchicalBrowseSidebar';
import { TreeContainer } from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/HierarchicalBrowseSidebar.components';
import {
    SEARCH_BROWSE_SIDEBAR_DEFAULT_WIDTH,
    SEARCH_BROWSE_SIDEBAR_WIDTH_STORAGE_KEY,
} from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/constants';
import { useShowNavBarRedesign } from '@app/useShowNavBarRedesign';

/**
 * Hides the sidebar entirely when there are no platforms to browse — PlatformBrowse stays mounted so
 * it can un-hide the sidebar when platforms come back. The app shell owns the outer inset in the
 * redesigned nav; the legacy layout still insets from the page edges here.
 */
const SidebarSlot = styled.div<{ $isHidden: boolean; $isShowNavBarRedesign: boolean }>`
    display: ${(props) => (props.$isHidden ? 'none' : 'flex')};
    flex-shrink: 0;
    margin: ${(props) => (props.$isShowNavBarRedesign ? '0' : '12px 0 12px 12px')};
`;

/** Browse rows keep their single-line layout; the shell's tree container owns scrolling. */
const BrowseBody = styled.div`
    white-space: nowrap;
`;

type Props = {
    visible: boolean;
};

/**
 * Search page "Navigate" sidebar: the shared {@link HierarchicalBrowseSidebar} shell (header,
 * collapse, scroll, drag-resize) around the platform / entity browse tree.
 */
const BrowseSidebar = ({ visible }: Props) => {
    const { t } = useTranslation('search');
    const isPlatformBrowseMode = useIsPlatformBrowseMode();
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const [isCollapsed, setIsCollapsed] = useState(false);
    const [isHidden, setIsHidden] = useState(false);

    const hideSidebar = useCallback(() => {
        setIsHidden(true);
        setIsCollapsed(true);
    }, []);
    const unhideSidebar = useCallback(() => setIsHidden(false), []);
    const expand = useCallback(() => setIsCollapsed(false), []);

    const renderBrowse = (collapsed: boolean) => (
        <BrowseBody id={SEARCH_RESULTS_BROWSE_SIDEBAR_ID}>
            {isPlatformBrowseMode ? (
                <PlatformBrowse
                    collapsed={collapsed}
                    expand={expand}
                    visible={visible}
                    hideSidebar={hideSidebar}
                    unhideSidebar={unhideSidebar}
                />
            ) : (
                <EntityBrowse visible={visible} />
            )}
        </BrowseBody>
    );

    return (
        <SidebarSlot $isHidden={isHidden} $isShowNavBarRedesign={isShowNavBarRedesign}>
            <HierarchicalBrowseSidebar
                title={t('sidebar.navigate')}
                isCollapsed={isCollapsed}
                onToggleCollapsed={() => setIsCollapsed((prev) => !prev)}
                id="browse-v2"
                dataTestId="browse-v2-results"
                collapseButtonTestId="browse-v2-toggle"
                defaultWidth={SEARCH_BROWSE_SIDEBAR_DEFAULT_WIDTH}
                widthStorageKey={SEARCH_BROWSE_SIDEBAR_WIDTH_STORAGE_KEY}
            >
                {/* Render prop keeps the browse tree mounted across collapse; collapsed, the
                    platform rows render as an icon-only column. */}
                {({ isCollapsed: collapsed }) =>
                    collapsed ? <TreeContainer $isCollapsed>{renderBrowse(true)}</TreeContainer> : renderBrowse(false)
                }
            </HierarchicalBrowseSidebar>
        </SidebarSlot>
    );
};

export default BrowseSidebar;
