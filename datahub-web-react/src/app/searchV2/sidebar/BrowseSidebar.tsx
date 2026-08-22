import { SearchBar } from '@components';
import { MagnifyingGlass } from '@phosphor-icons/react/dist/csr/MagnifyingGlass';
import { SquaresFour } from '@phosphor-icons/react/dist/csr/SquaresFour';
import React, { useCallback, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { SEARCH_RESULTS_BROWSE_SIDEBAR_ID } from '@app/onboarding/config/SearchOnboardingConfig';
import { useIsPlatformBrowseMode } from '@app/searchV2/sidebar/BrowseContext';
import BrowseSidebarSearchResults from '@app/searchV2/sidebar/BrowseSidebarSearchResults';
import EntityBrowse from '@app/searchV2/sidebar/EntityBrowse';
import PlatformBrowse from '@app/searchV2/sidebar/PlatformBrowse';
import { useOnChangeFilters, useSelectedFilters } from '@app/searchV2/sidebar/SidebarContext';
import { clearBrowseNavigationFilters, hasBrowseNavigationFilter } from '@app/searchV2/sidebar/browseContextUtils';
import { isBrowseSidebarSearchActive } from '@app/searchV2/sidebar/browseSidebarSearch';
import useBrowseSidebarSearch from '@app/searchV2/sidebar/useBrowseSidebarSearch';
import HierarchicalBrowseSidebar from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/HierarchicalBrowseSidebar';
import {
    CollapsedScrollColumn,
    SearchIconButton,
} from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/HierarchicalBrowseSidebar.components';
import SidebarHomeNavLink from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/SidebarHomeNavLink';
import {
    HIERARCHICAL_BROWSE_GAP_PX,
    HIERARCHICAL_BROWSE_LAYOUT_PADDING_PX,
    TREE_ROW_ENTITY_ICON_SIZE,
} from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/constants';
import { useShowNavBarRedesign } from '@app/useShowNavBarRedesign';

const BrowseSidebarSlot = styled.div<{ $hidden: boolean; $isShowNavBarRedesign?: boolean }>`
    display: ${(props) => (props.$hidden ? 'none' : 'flex')};
    align-self: stretch;
    min-height: 0;
    ${(props) =>
        props.$isShowNavBarRedesign
            ? `
                padding: ${HIERARCHICAL_BROWSE_LAYOUT_PADDING_PX}px 0 ${HIERARCHICAL_BROWSE_LAYOUT_PADDING_PX}px ${HIERARCHICAL_BROWSE_LAYOUT_PADDING_PX}px;
                margin-right: ${HIERARCHICAL_BROWSE_GAP_PX}px;
            `
            : 'margin: 12px 0 12px 12px;'}
`;

type Props = {
    visible: boolean;
};

const BrowseSidebar = ({ visible }: Props) => {
    const { t } = useTranslation('search');
    const { t: tc } = useTranslation('common.actions');
    const isPlatformBrowseMode = useIsPlatformBrowseMode();
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const [isCollapsed, setIsCollapsed] = useState(false);
    const [isHidden, setIsHidden] = useState(false);
    const [searchInput, setSearchInput] = useState('');
    const { hits, loading, isRefreshing } = useBrowseSidebarSearch({ searchInput });
    const searchActive = isBrowseSidebarSearchActive(searchInput);
    const selectedFilters = useSelectedFilters();
    const onChangeFilters = useOnChangeFilters();
    const isAllSelected = !hasBrowseNavigationFilter(selectedFilters);

    const hideSidebar = useCallback(() => {
        setIsHidden(true);
        setIsCollapsed(true);
    }, []);

    const unhideSidebar = useCallback(() => setIsHidden(false), []);
    const expandSidebar = useCallback(() => setIsCollapsed(false), []);
    const toggleCollapsed = useCallback(() => setIsCollapsed((prev) => !prev), []);
    const clearSearch = useCallback(() => setSearchInput(''), []);
    const goToAll = useCallback(() => {
        setSearchInput('');
        onChangeFilters(clearBrowseNavigationFilters(selectedFilters));
    }, [onChangeFilters, selectedFilters]);

    const platformBrowse = (
        <PlatformBrowse
            collapsed={isCollapsed}
            expand={expandSidebar}
            visible={visible}
            hideSidebar={hideSidebar}
            unhideSidebar={unhideSidebar}
        />
    );

    return (
        <BrowseSidebarSlot $hidden={isHidden} $isShowNavBarRedesign={isShowNavBarRedesign} id="browse-v2">
            <HierarchicalBrowseSidebar
                title={t('sidebar.navigate')}
                isCollapsed={isCollapsed}
                onToggleCollapsed={toggleCollapsed}
                onExpandSidebar={expandSidebar}
                id={SEARCH_RESULTS_BROWSE_SIDEBAR_ID}
                dataTestId="browse-v2-results"
                collapseButtonTestId="browse-v2-toggle"
                expandTooltip={tc('expand')}
                collapseTooltip={tc('collapse')}
                search={
                    <SearchBar
                        placeholder={t('sidebar.searchPlaceholder')}
                        value={searchInput}
                        onChange={(value) => setSearchInput(value)}
                        data-testid="browse-v2-search-input"
                    />
                }
                homeNav={
                    <SidebarHomeNavLink
                        label={t('sidebar.all')}
                        icon={SquaresFour}
                        isSelected={isAllSelected}
                        onClick={goToAll}
                        data-testid="browse-v2-all"
                    />
                }
            >
                {({ isCollapsed: collapsed }) => {
                    if (collapsed) {
                        return (
                            <>
                                <SearchIconButton
                                    type="button"
                                    onClick={expandSidebar}
                                    aria-label={t('sidebar.searchAriaLabel')}
                                    data-testid="browse-v2-search-icon"
                                >
                                    <MagnifyingGlass size={TREE_ROW_ENTITY_ICON_SIZE} weight="regular" />
                                </SearchIconButton>
                                {isPlatformBrowseMode ? (
                                    <CollapsedScrollColumn>{platformBrowse}</CollapsedScrollColumn>
                                ) : null}
                            </>
                        );
                    }

                    if (searchActive) {
                        return (
                            <BrowseSidebarSearchResults
                                hits={hits}
                                loading={loading}
                                isRefreshing={isRefreshing}
                                onClear={clearSearch}
                            />
                        );
                    }

                    if (!isPlatformBrowseMode) {
                        return <EntityBrowse visible={visible} />;
                    }

                    return platformBrowse;
                }}
            </HierarchicalBrowseSidebar>
        </BrowseSidebarSlot>
    );
};

export default BrowseSidebar;
