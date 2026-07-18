import { Button, EmptyState, SearchBar } from '@components';
import { ArrowLineLeft } from '@phosphor-icons/react/dist/csr/ArrowLineLeft';
import { ArrowLineRight } from '@phosphor-icons/react/dist/csr/ArrowLineRight';
import { House } from '@phosphor-icons/react/dist/csr/House';
import { MagnifyingGlass } from '@phosphor-icons/react/dist/csr/MagnifyingGlass';
import { Sigma } from '@phosphor-icons/react/dist/csr/Sigma';
import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Link, matchPath, useLocation } from 'react-router-dom';
import styled from 'styled-components';

import { SimpleSelect } from '@components/components/Select/SimpleSelect';

import { TreeSectionHeader } from '@app/homeV2/layout/sidebar/documents/TreeSectionHeader';
import { SemanticModelRow } from '@app/metrics/SemanticModelRow';
import { useMetricsEntityContext } from '@app/metrics/context/MetricsEntityContext';
import PlatformIcon from '@app/sharedV2/icons/PlatformIcon';
import { useShowNavBarRedesign } from '@app/useShowNavBarRedesign';
import { PageRoutes } from '@conf/Global';

import { useGetSemanticModelsBrowseQuery } from '@graphql/metricsBrowse.generated';
import { DataPlatform } from '@types';

const SIDEBAR_TRANSITION_MS = 300;
export const SIDEBAR_COLLAPSED_WIDTH = 63;

// Visual chrome mirrors `app/context/ContextSidebar.tsx`.
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

const Separator = styled.div`
    height: 1px;
    background: ${(props) => props.theme.colors.border};
`;

const SearchInputWrapper = styled.div`
    padding: 12px;
    flex-shrink: 0;
`;

const FiltersWrapper = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
    padding: 0 12px 12px;
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
    /* Right padding (6px) + scrollbar-gutter (6px) = 12px, aligning the tree
       rows' right edge with the search/filters above. scrollbar-gutter keeps
       that space reserved so rows don't shift when the scrollbar appears. */
    padding: 8px 6px 8px 8px;
    scrollbar-gutter: stable;
    display: flex;
    flex-direction: column;

    &::-webkit-scrollbar {
        width: 6px;
    }
    &::-webkit-scrollbar-track {
        background: ${(props) => props.theme.colors.scrollbarTrack};
    }
    &::-webkit-scrollbar-thumb {
        background: ${(props) => props.theme.colors.scrollbarThumb};
        border-radius: 3px;
    }
    &::-webkit-scrollbar-thumb:hover {
        background: ${(props) => props.theme.colors.scrollbarThumbHover};
    }
    scrollbar-width: thin;
    scrollbar-color: ${(props) => props.theme.colors.scrollbarThumb} ${(props) => props.theme.colors.scrollbarTrack};
`;

const EmptyStateWrapper = styled.div`
    flex: 1;
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 24px 12px;
`;

// Mirrors the "Glossary Home" nav row in `GlossarySidebar`: a 38px row with a
// selected background + brand-tinted focus shadow when active, neutral hover
// otherwise. Sits above the divider, above the semantic-model tree.
const HomeNavLink = styled(Link)<{ $isSelected: boolean }>`
    position: relative;
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 4px 8px;
    margin: 0 2px;
    min-height: 38px;
    height: 38px;
    border-radius: 6px;
    text-decoration: none;
    cursor: pointer;
    transition: background-color 0.15s ease;

    ${(props) =>
        props.$isSelected
            ? `
                background: ${props.theme.colors.bgSelectedSubtle};
                box-shadow: ${props.theme.colors.shadowFocusBrand};
            `
            : `
                &:hover {
                    background: ${props.theme.colors.bgHover};
                    box-shadow: ${props.theme.colors.shadowFocus};
                }
            `}
`;

const HomeNavIcon = styled.div<{ $isSelected: boolean }>`
    display: flex;
    align-items: center;
    justify-content: center;
    width: 20px;
    flex-shrink: 0;

    && svg {
        color: ${(props) => (props.$isSelected ? props.theme.colors.iconBrand : props.theme.colors.icon)};
    }
`;

const HomeNavLabel = styled.span<{ $isSelected: boolean }>`
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    font-size: 14px;
    line-height: 20px;
    color: ${(props) => props.theme.colors.textSecondary};

    ${(props) =>
        props.$isSelected &&
        `
            background: ${props.theme.colors.brandGradientSelected};
            background-clip: text;
            -webkit-text-fill-color: transparent;
            font-weight: 600;
        `}
`;

const ALL_OPTION = '__all__';

type Props = {
    width: number;
    isCollapsed: boolean;
    isEntityProfile?: boolean;
    onToggleCollapsed: () => void;
    onExpandSidebar: () => void;
};

export default function MetricsSidebar({
    width,
    isCollapsed,
    isEntityProfile: _isEntityProfile,
    onToggleCollapsed,
    onExpandSidebar,
}: Props) {
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

function ExpandedMetricsSidebar({ onToggleCollapsed }: { onToggleCollapsed: () => void }) {
    const { t } = useTranslation('misc');
    const location = useLocation();
    const [searchInput, setSearchInput] = useState('');
    const [platformFilter, setPlatformFilter] = useState(ALL_OPTION);
    const [isModelsExpanded, setIsModelsExpanded] = useState(true);

    const {
        expandedSemanticModelUrns,
        expandedMetricUrns,
        selectedUrn,
        childMetricsByModelUrn,
        childMetricsByParentUrn,
        toggleSemanticModel,
        toggleMetric,
        expandAllSemanticModels,
        collapseAllExpanded,
        setChildMetricsForModel,
        setChildMetricsForParent,
        refetchKey,
    } = useMetricsEntityContext();

    const isHomeSelected = !!matchPath(location.pathname, { path: PageRoutes.METRICS, exact: true });

    // Fetch root semantic models.
    const { data: modelsData, refetch: refetchModels } = useGetSemanticModelsBrowseQuery({
        variables: { input: { count: 100, start: 0 } },
    });

    // Re-fetch root when refetchKey changes.
    useEffect(() => {
        if (refetchKey > 0) {
            refetchModels();
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [refetchKey]);

    const allModels = useMemo(() => modelsData?.getSemanticModels?.semanticModels ?? [], [modelsData]);

    // Derive unique platforms for the Platform filter.
    const platformOptions = useMemo(() => {
        const seen = new Map<string, { platform: DataPlatform; label: string }>();
        allModels.forEach(({ platform }) => {
            const urn = platform?.urn;
            const label = platform?.properties?.displayName ?? platform?.info?.displayName ?? platform?.name;
            if (urn && label && platform && !seen.has(urn)) seen.set(urn, { platform, label });
        });
        return Array.from(seen.entries()).map(([urn, { platform, label }]) => ({
            value: urn,
            label,
            icon: (
                <PlatformIcon
                    platform={platform}
                    size={14}
                    styles={{ backgroundColor: 'transparent', padding: '0px', borderRadius: '0px' }}
                />
            ),
        }));
    }, [allModels]);

    // Filter semantic models by platform + search.
    const visibleModels = useMemo(() => {
        return allModels.filter((m) => {
            if (platformFilter !== ALL_OPTION && m.platform?.urn !== platformFilter) return false;
            if (searchInput) {
                const q = searchInput.toLowerCase();
                const nameMatch = (m.info?.name ?? '').toLowerCase().includes(q);
                if (!nameMatch) {
                    // Keep the model visible if any of its cached child metrics match.
                    const childMetrics = childMetricsByModelUrn[m.urn] ?? [];
                    const childMatch = childMetrics.some((metric) =>
                        (metric.info?.name ?? '').toLowerCase().includes(q),
                    );
                    if (!childMatch) return false;
                }
            }
            return true;
        });
    }, [allModels, platformFilter, searchInput, childMetricsByModelUrn]);

    // Expand-all / collapse-all for the "Semantic Models" section. Any expanded
    // model means the section is "open" → the toggle reads as Collapse all.
    const isSectionExpanded = expandedSemanticModelUrns.size > 0 || expandedMetricUrns.size > 0;
    const handleToggleExpandAll = useCallback(() => {
        if (isSectionExpanded) {
            collapseAllExpanded();
            return;
        }
        // Open the section so the freshly-expanded rows are visible.
        setIsModelsExpanded(true);
        const expandable = visibleModels.filter((m) => (m.metrics?.total ?? 0) > 0).map((m) => m.urn);
        expandAllSemanticModels(expandable);
    }, [isSectionExpanded, visibleModels, collapseAllExpanded, expandAllSemanticModels]);

    // Auto-expand the semantic model that directly contains the selected metric.
    // Deeper metric-in-metric ancestors are handled properly in subsequent PR.
    useEffect(() => {
        if (!selectedUrn) return;
        allModels.forEach((m) => {
            const topLevelMetrics = childMetricsByModelUrn[m.urn] ?? [];
            const isDirectChild = topLevelMetrics.some((metric) => metric.urn === selectedUrn);
            if (isDirectChild && !expandedSemanticModelUrns.has(m.urn)) {
                toggleSemanticModel(m.urn);
            }
        });
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [selectedUrn, allModels, childMetricsByModelUrn]);

    return (
        <>
            <HeaderControls $isCollapsed={false}>
                <SidebarTitle>{t('metrics.sidebarTitle')}</SidebarTitle>
                <HeaderButtons>
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

            {platformOptions.length > 1 && (
                <FiltersWrapper>
                    <SimpleSelect
                        size="sm"
                        width="full"
                        showClear={false}
                        selectLabelProps={{ variant: 'labeled', label: t('metrics.filterPlatform') }}
                        options={[{ value: ALL_OPTION, label: t('metrics.filterAll') }, ...platformOptions]}
                        values={[platformFilter]}
                        onUpdate={(vals) => setPlatformFilter(vals[0] ?? ALL_OPTION)}
                        dataTestId="metrics-sidebar-platform-filter"
                    />
                </FiltersWrapper>
            )}

            <Separator />

            <TreeContainer data-testid="metrics-sidebar-tree">
                <HomeNavLink to={PageRoutes.METRICS} $isSelected={isHomeSelected} data-testid="metrics-sidebar-home">
                    <HomeNavIcon $isSelected={isHomeSelected}>
                        <House size={20} weight={isHomeSelected ? 'fill' : 'regular'} />
                    </HomeNavIcon>
                    <HomeNavLabel $isSelected={isHomeSelected}>{t('metrics.allMetrics')}</HomeNavLabel>
                </HomeNavLink>

                <TreeSectionHeader
                    level={0}
                    label={t('metrics.semanticModelsSection')}
                    isExpanded={isModelsExpanded}
                    onToggle={() => setIsModelsExpanded((v) => !v)}
                    onToggleExpandAll={handleToggleExpandAll}
                    isAllExpanded={isSectionExpanded}
                    expandAllLabel={t('metrics.expandAll')}
                    collapseAllLabel={t('metrics.collapseAll')}
                    testId="metrics-sidebar-models-section"
                />
                {isModelsExpanded && (
                    <>
                        {modelsData && visibleModels.length === 0 && (
                            <EmptyStateWrapper>
                                <EmptyState
                                    icon={Sigma}
                                    title={t('metrics.emptyTreeTitle')}
                                    description={t('metrics.emptyTreeDescription')}
                                    size="sm"
                                />
                            </EmptyStateWrapper>
                        )}
                        {visibleModels.map((model) => (
                            <SemanticModelRow
                                key={model.urn}
                                model={model}
                                searchInput={searchInput}
                                isExpanded={expandedSemanticModelUrns.has(model.urn)}
                                isSelected={selectedUrn === model.urn}
                                cachedMetrics={childMetricsByModelUrn[model.urn]}
                                expandedMetricUrns={expandedMetricUrns}
                                childMetricsByParentUrn={childMetricsByParentUrn}
                                selectedUrn={selectedUrn}
                                onToggle={() => toggleSemanticModel(model.urn)}
                                onMetricsFetched={setChildMetricsForModel}
                                onToggleMetric={toggleMetric}
                                onChildMetricsFetched={setChildMetricsForParent}
                            />
                        ))}
                    </>
                )}
            </TreeContainer>
        </>
    );
}
