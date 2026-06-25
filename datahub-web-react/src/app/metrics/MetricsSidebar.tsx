import { Button, EmptyState, SearchBar, Tooltip } from '@components';
import { ArrowLineLeft } from '@phosphor-icons/react/dist/csr/ArrowLineLeft';
import { ArrowLineRight } from '@phosphor-icons/react/dist/csr/ArrowLineRight';
import { MagnifyingGlass } from '@phosphor-icons/react/dist/csr/MagnifyingGlass';
import { Plus } from '@phosphor-icons/react/dist/csr/Plus';
import { Sigma } from '@phosphor-icons/react/dist/csr/Sigma';
import { SquaresFour } from '@phosphor-icons/react/dist/csr/SquaresFour';
import { Divider } from 'antd';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { matchPath, useHistory, useLocation } from 'react-router-dom';
import styled from 'styled-components';

import { MetricsTreeItem } from '@app/metrics/MetricsTreeItem';
import { useShowNavBarRedesign } from '@app/useShowNavBarRedesign';
import { PageRoutes } from '@conf/Global';

const SIDEBAR_TRANSITION_MS = 300;
export const SIDEBAR_COLLAPSED_WIDTH = 63;

// Visual styling intentionally mirrors `ContextSidebar` so the Metrics
// sidebar reads as the same kind of "browse tree" surface as Documents.
// No outer margin — gaps between the three columns are owned by the
// parent `ContentWrapper` in `MetricsPage.tsx`.
const SidebarContainer = styled.div<{
    $width: number;
    $isCollapsed: boolean;
    $isShowNavBarRedesign?: boolean;
}>`
    flex-shrink: 0;
    max-height: 100%;
    width: ${(props) => (props.$isCollapsed ? `${SIDEBAR_COLLAPSED_WIDTH}px` : `${props.$width}px`)};
    transition: width ${SIDEBAR_TRANSITION_MS}ms ease-in-out;
    background-color: ${(props) => props.theme.colors.bg};
    border-radius: ${(props) =>
        props.$isShowNavBarRedesign ? props.theme.styles['border-radius-navbar-redesign'] : '8px'};
    display: flex;
    flex-direction: column;
    overflow: hidden;
    ${(props) => props.$isShowNavBarRedesign && `box-shadow: ${props.theme.colors.shadowSm};`}
`;

const HeaderControls = styled.div<{ $isCollapsed: boolean }>`
    display: flex;
    align-items: center;
    justify-content: ${(props) => (props.$isCollapsed ? 'center' : 'space-between')};
    padding: 12px;
    height: 50px;
    overflow: hidden;
`;

const SidebarTitle = styled.div`
    font-size: 16px;
    font-weight: bold;
    color: ${(props) => props.theme.colors.text};
`;

// Right side of the header: groups the create button with the collapse
// toggle. Mirrors `HeaderButtons` in `ContextSidebar.tsx`.
const HeaderButtons = styled.div`
    display: flex;
    align-items: center;
    gap: 4px;
`;

// Small circular "+ New Metric" button. Sized to sit comfortably next to
// the collapse toggle in the header — copied from `StyledButton` in
// `ContextSidebar.tsx` so the two sidebars feel identical.
const StyledButton = styled(Button)`
    padding: 2px;
    svg {
        width: 20px;
        height: 20px;
    }
`;

const ThinDivider = styled(Divider)`
    margin: 0px;
    padding: 0px;
`;

const SearchWrapper = styled.div`
    flex-shrink: 0;
`;

const SearchInputWrapper = styled.div`
    padding: 12px;
`;

const SearchIconButton = styled.button`
    display: flex;
    align-items: center;
    justify-content: center;
    width: 100%;
    padding: 16px 0;
    border: none;
    background: transparent;
    cursor: pointer;
    color: ${(props) => props.theme.colors.icon};

    &:hover {
        color: ${(props) => props.theme.colors.iconHover};
    }
`;

const TreeContainer = styled.div`
    flex: 1;
    overflow-y: auto;
    overflow-x: hidden;
    padding: 8px 12px;
    display: flex;
    flex-direction: column;

    &::-webkit-scrollbar {
        width: 6px;
    }
    &::-webkit-scrollbar-track {
        background: transparent;
    }
    &::-webkit-scrollbar-thumb {
        background: ${(props) => props.theme.colors.scrollbarThumb};
        border-radius: 3px;
    }
    scrollbar-width: thin;
    scrollbar-color: ${(props) => props.theme.colors.scrollbarThumb} transparent;
`;

const EmptyStateWrapper = styled.div`
    flex: 1;
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 24px 12px;
`;

type Props = {
    width: number;
    isCollapsed: boolean;
    onToggleCollapsed: () => void;
    onExpandSidebar: () => void;
};

/**
 * MetricsSidebar - Left navigation sidebar for the Metrics page.
 *
 * Shape mirrors `app/context/ContextSidebar.tsx`:
 *   - header with title + "+ New Metric" circle button + collapse toggle
 *   - search bar (no-op until a metric index exists)
 *   - browse tree: Overview row + empty state until metrics exist
 *
 * Tree rows render through `MetricsTreeItem`, which mirrors
 * `DocumentTreeItem` exactly — same row chrome (38px height, 4px vertical
 * padding, `bgSelectedSubtle` + `shadowFocusBrand` selection, `bgHover` +
 * `shadowFocus` hover) AND same icon-vs-caret-on-hover behaviour for rows
 * with children. The Overview row is the only consumer today; when
 * semantic-model rows arrive they'll pass `hasChildren` and get the caret
 * swap for free.
 *
 * Selection is purely route-driven — same model as Documents. Today only
 * `/metrics` exists, so Overview is lit when it matches. Once `/metric/:urn`
 * and `/semantic-model/:urn` exist, add equivalent `matchPath` checks for
 * the new rows and selection will continue to work without any state.
 */
export default function MetricsSidebar({ width, isCollapsed, onToggleCollapsed, onExpandSidebar }: Props) {
    const { t } = useTranslation('misc');
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const history = useHistory();
    const location = useLocation();
    const [searchInput, setSearchInput] = useState('');

    const isOverviewRouteActive = !!matchPath(location.pathname, { path: PageRoutes.METRICS, exact: true });

    return (
        <SidebarContainer
            $width={width}
            $isCollapsed={isCollapsed}
            $isShowNavBarRedesign={isShowNavBarRedesign}
            data-testid="metrics-sidebar"
        >
            <HeaderControls $isCollapsed={isCollapsed}>
                {!isCollapsed && <SidebarTitle>{t('metrics.sidebarTitle')}</SidebarTitle>}
                <HeaderButtons>
                    {!isCollapsed && (
                        <Tooltip title={t('metrics.newMetric')} placement="bottom" showArrow={false}>
                            <span style={{ display: 'inline-block' }}>
                                <StyledButton
                                    variant="filled"
                                    color="primary"
                                    isCircle
                                    icon={{ icon: Plus }}
                                    onClick={() => {
                                        /* stub: wired up once the create-metric mutation exists */
                                    }}
                                    data-testid="metrics-sidebar-new-metric"
                                />
                            </span>
                        </Tooltip>
                    )}
                    <Button
                        variant="text"
                        color="gray"
                        size="lg"
                        isCircle
                        icon={{ icon: isCollapsed ? ArrowLineRight : ArrowLineLeft }}
                        isActive={!isCollapsed}
                        onClick={onToggleCollapsed}
                        data-testid="metrics-sidebar-collapse-button"
                    />
                </HeaderButtons>
            </HeaderControls>
            <ThinDivider />

            <SearchWrapper>
                {isCollapsed ? (
                    <SearchIconButton
                        onClick={onExpandSidebar}
                        data-testid="metrics-sidebar-search-icon"
                        aria-label={t('metrics.searchAriaLabel')}
                    >
                        <MagnifyingGlass size={20} weight="regular" />
                    </SearchIconButton>
                ) : (
                    <SearchInputWrapper>
                        <SearchBar
                            placeholder={t('metrics.searchPlaceholder')}
                            value={searchInput}
                            onChange={setSearchInput}
                            data-testid="metrics-sidebar-search-input"
                        />
                    </SearchInputWrapper>
                )}
            </SearchWrapper>

            {!isCollapsed && (
                <>
                    <ThinDivider />
                    <TreeContainer data-testid="metrics-sidebar-tree">
                        <MetricsTreeItem
                            level={0}
                            icon={SquaresFour}
                            title={t('metrics.overview')}
                            isSelected={isOverviewRouteActive}
                            onClick={() => history.push(PageRoutes.METRICS)}
                            testId="metrics-sidebar-overview"
                        />

                        <EmptyStateWrapper>
                            <EmptyState
                                icon={Sigma}
                                title={t('metrics.emptyTreeTitle')}
                                description={t('metrics.emptyTreeDescription')}
                                size="sm"
                            />
                        </EmptyStateWrapper>
                    </TreeContainer>
                </>
            )}
        </SidebarContainer>
    );
}
