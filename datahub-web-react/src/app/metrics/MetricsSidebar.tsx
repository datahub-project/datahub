import { Button, EmptyState, SearchBar, Tooltip } from '@components';
import { ArrowLineLeft } from '@phosphor-icons/react/dist/csr/ArrowLineLeft';
import { ArrowLineRight } from '@phosphor-icons/react/dist/csr/ArrowLineRight';
import { MagnifyingGlass } from '@phosphor-icons/react/dist/csr/MagnifyingGlass';
import { Plus } from '@phosphor-icons/react/dist/csr/Plus';
import { Sigma } from '@phosphor-icons/react/dist/csr/Sigma';
import { SquaresFour } from '@phosphor-icons/react/dist/csr/SquaresFour';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { matchPath, useHistory, useLocation } from 'react-router-dom';
import styled from 'styled-components';

import { MetricsTreeItem } from '@app/metrics/MetricsTreeItem';
import { useShowNavBarRedesign } from '@app/useShowNavBarRedesign';
import { PageRoutes } from '@conf/Global';

const SIDEBAR_TRANSITION_MS = 300;
export const SIDEBAR_COLLAPSED_WIDTH = 63;

// Visual chrome (SidebarContainer / HeaderControls / SidebarTitle /
// HeaderButtons / CircleIconButton / Separator / SearchInputWrapper /
// SearchIconButton) intentionally mirrors `app/context/ContextSidebar.tsx`
// (jjoyce0510) field-for-field so the Metrics sidebar reads as the same
// kind of "browse tree" surface as Documents. The copy is deliberate
// rather than abstracted into a shared shell because (a) the two
// sidebars will diverge as Metrics gets metric-specific filters /
// search, and (b) lifting a shared primitive out of Documents would
// be a separate refactor touching `homeV2/layout/sidebar/documents/*`.
// If the chrome stabilises across both, that's the right follow-up.
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

const HeaderButtons = styled.div`
    display: flex;
    align-items: center;
    gap: 4px;
`;

// Tightens the filled circle button so it visually matches the text
// collapse button beside it. Without this, `size="lg"` filled buttons
// render with a noticeably larger footprint than `size="lg"` text
// buttons because the gradient + shadow chrome adds visual weight.
// Mirrors `StyledButton` in `app/context/ContextSidebar.tsx` (jjoyce0510).
const CircleIconButton = styled(Button)`
    padding: 2px;
    svg {
        width: 20px;
        height: 20px;
    }
`;

// 1px separator. Renders the same line antd's `<Divider />` would,
// without the antd dependency, using the standard theme border token
// (`theme.colors.border`) that other thin separators in this codebase
// use (e.g. `AdvancedSearchFilterOverallUnionTypeSelect`, `SchemaTable`).
const Separator = styled.div`
    height: 1px;
    background: ${(props) => props.theme.colors.border};
`;

const SearchInputWrapper = styled.div`
    padding: 12px;
    flex-shrink: 0;
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
 * Shape mirrors `app/context/ContextSidebar.tsx`: header with title +
 * "+ New Metric" circle button + collapse toggle, search bar (no-op
 * until a metric index exists), and a browse tree with an Overview row
 * + empty state until metrics exist.
 *
 * The parent does a single top-level branch on `isCollapsed` and renders
 * one of two purpose-built children (`CollapsedMetricsSidebar` or
 * `ExpandedMetricsSidebar`). Each child knows exactly what it renders,
 * so neither has internal `isCollapsed` checks. Per PR #18047 review
 * (ani-malgari) — keeps the conditional rendering at one site instead
 * of sprinkled across the body.
 *
 * Tree rows render through `MetricsTreeItem`, which mirrors
 * `DocumentTreeItem` exactly — same row chrome (38px height, 4px vertical
 * padding, `bgSelectedSubtle` + `shadowFocusBrand` selection, `bgHover` +
 * `shadowFocus` hover) AND same icon-vs-caret-on-hover behaviour for rows
 * with children. The Overview row is the only consumer today; when
 * semantic-model rows arrive they'll pass `hasChildren` and get the caret
 * swap for free.
 *
 * Selection is purely route-driven — same model as Documents. Today
 * only `/metrics` exists, so Overview lights when it matches. Once
 * `/metric/:urn` and `/semantic-model/:urn` exist, add `matchPath`
 * checks alongside the Overview one (and at that point it's worth
 * lifting the derivation into a `useMetricsSidebarSelection` hook).
 */
export default function MetricsSidebar({ width, isCollapsed, onToggleCollapsed, onExpandSidebar }: Props) {
    const isShowNavBarRedesign = useShowNavBarRedesign();

    return (
        <SidebarContainer
            $width={width}
            $isCollapsed={isCollapsed}
            $isShowNavBarRedesign={isShowNavBarRedesign}
            data-testid="metrics-sidebar"
        >
            {isCollapsed ? (
                <CollapsedMetricsSidebar onToggleCollapsed={onToggleCollapsed} onExpandSidebar={onExpandSidebar} />
            ) : (
                <ExpandedMetricsSidebar onToggleCollapsed={onToggleCollapsed} />
            )}
        </SidebarContainer>
    );
}

/**
 * Compact icon-only variant shown when the sidebar is collapsed.
 * Two affordances: a magnifier that re-expands the sidebar (back into
 * the search input) and a right-arrow collapse toggle.
 */
function CollapsedMetricsSidebar({
    onToggleCollapsed,
    onExpandSidebar,
}: {
    onToggleCollapsed: () => void;
    onExpandSidebar: () => void;
}) {
    const { t } = useTranslation('misc');

    return (
        <>
            <HeaderControls $isCollapsed>
                <Button
                    variant="text"
                    color="gray"
                    size="lg"
                    isCircle
                    icon={{ icon: ArrowLineRight }}
                    onClick={onToggleCollapsed}
                    data-testid="metrics-sidebar-collapse-button"
                />
            </HeaderControls>
            <Separator />
            <SearchIconButton
                onClick={onExpandSidebar}
                data-testid="metrics-sidebar-search-icon"
                aria-label={t('metrics.searchAriaLabel')}
            >
                <MagnifyingGlass size={20} weight="regular" />
            </SearchIconButton>
        </>
    );
}

/**
 * Full sidebar contents shown when expanded: title + create button +
 * collapse toggle in the header, a real search input, and the browse
 * tree (Overview row + empty state).
 *
 * All expanded-only state (search input, route match for selection)
 * lives here so collapsed renders don't pay for it.
 */
function ExpandedMetricsSidebar({ onToggleCollapsed }: { onToggleCollapsed: () => void }) {
    const { t } = useTranslation('misc');
    const history = useHistory();
    const location = useLocation();
    const [searchInput, setSearchInput] = useState('');

    const isOverviewSelected = !!matchPath(location.pathname, { path: PageRoutes.METRICS, exact: true });

    return (
        <>
            <HeaderControls $isCollapsed={false}>
                <SidebarTitle>{t('metrics.sidebarTitle')}</SidebarTitle>
                <HeaderButtons>
                    <Tooltip title={t('metrics.newMetric')} placement="bottom" showArrow={false}>
                        <CircleIconButton
                            variant="filled"
                            color="primary"
                            isCircle
                            icon={{ icon: Plus }}
                            onClick={() => {
                                /* stub: wired up once the create-metric mutation exists */
                            }}
                            data-testid="metrics-sidebar-new-metric"
                        />
                    </Tooltip>
                    <Button
                        variant="text"
                        color="gray"
                        size="lg"
                        isCircle
                        icon={{ icon: ArrowLineLeft }}
                        isActive
                        onClick={onToggleCollapsed}
                        data-testid="metrics-sidebar-collapse-button"
                    />
                </HeaderButtons>
            </HeaderControls>
            <Separator />

            <SearchInputWrapper>
                <SearchBar
                    placeholder={t('metrics.searchPlaceholder')}
                    value={searchInput}
                    onChange={setSearchInput}
                    data-testid="metrics-sidebar-search-input"
                />
            </SearchInputWrapper>

            <Separator />
            <TreeContainer data-testid="metrics-sidebar-tree">
                <MetricsTreeItem
                    level={0}
                    icon={SquaresFour}
                    title={t('metrics.overview')}
                    isSelected={isOverviewSelected}
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
    );
}
