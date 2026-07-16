import { Button, EmptyState, SearchBar } from '@components';
import { ArrowLineLeft } from '@phosphor-icons/react/dist/csr/ArrowLineLeft';
import { ArrowLineRight } from '@phosphor-icons/react/dist/csr/ArrowLineRight';
import { Cube } from '@phosphor-icons/react/dist/csr/Cube';
import { MagnifyingGlass } from '@phosphor-icons/react/dist/csr/MagnifyingGlass';
import { Sigma } from '@phosphor-icons/react/dist/csr/Sigma';
import { SquaresFour } from '@phosphor-icons/react/dist/csr/SquaresFour';
import React, { useEffect, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { matchPath, useHistory, useLocation } from 'react-router-dom';
import styled from 'styled-components';

import { SimpleSelect } from '@components/components/Select/SimpleSelect';

import { MetricsTreeItem } from '@app/metrics/MetricsTreeItem';
import { useMetricsEntityContext } from '@app/metrics/context/MetricsEntityContext';
import { useShowNavBarRedesign } from '@app/useShowNavBarRedesign';
import { PageRoutes } from '@conf/Global';

import {
    GetSemanticModelMetricsQuery,
    GetSemanticModelsBrowseQuery,
    useGetMetricChildrenLazyQuery,
    useGetSemanticModelMetricsLazyQuery,
    useGetSemanticModelsBrowseQuery,
} from '@graphql/metricsBrowse.generated';
import { EntityType } from '@types';

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

const ALL_OPTION = '__all__';

type SemanticModel = NonNullable<
    NonNullable<GetSemanticModelsBrowseQuery['getSemanticModels']>['semanticModels'][number]
>;

type SearchResultEntity = NonNullable<
    NonNullable<
        NonNullable<GetSemanticModelMetricsQuery['semanticModel']>['metrics']
    >['searchResults'][number]['entity']
>;
type MetricEntity = Extract<SearchResultEntity, { __typename?: 'Metric' }>;

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
    const history = useHistory();
    const location = useLocation();
    const [searchInput, setSearchInput] = useState('');
    const [platformFilter, setPlatformFilter] = useState(ALL_OPTION);

    const {
        expandedSemanticModelUrns,
        expandedMetricUrns,
        selectedUrn,
        childMetricsByModelUrn,
        childMetricsByParentUrn,
        toggleSemanticModel,
        toggleMetric,
        setChildMetricsForModel,
        setChildMetricsForParent,
        refetchKey,
    } = useMetricsEntityContext();

    const isOverviewSelected = !!matchPath(location.pathname, { path: PageRoutes.METRICS, exact: true });

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
        const seen = new Map<string, string>();
        allModels.forEach((m) => {
            const urn = m.platform?.urn;
            const label = m.platform?.properties?.displayName ?? m.platform?.info?.displayName ?? m.platform?.name;
            if (urn && label && !seen.has(urn)) seen.set(urn, label);
        });
        return Array.from(seen.entries()).map(([urn, label]) => ({ value: urn, label }));
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
                <MetricsTreeItem
                    level={0}
                    icon={SquaresFour}
                    title={t('metrics.overview')}
                    isSelected={isOverviewSelected}
                    onClick={() => history.push(PageRoutes.METRICS)}
                    testId="metrics-sidebar-overview"
                />

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
            </TreeContainer>
        </>
    );
}

type SemanticModelRowProps = {
    model: SemanticModel;
    searchInput: string;
    isExpanded: boolean;
    isSelected: boolean;
    cachedMetrics: MetricEntity[] | undefined;
    expandedMetricUrns: Set<string>;
    childMetricsByParentUrn: Record<string, MetricEntity[]>;
    selectedUrn: string | null;
    onToggle: () => void;
    onMetricsFetched: (modelUrn: string, metrics: MetricEntity[]) => void;
    onToggleMetric: (urn: string) => void;
    onChildMetricsFetched: (parentUrn: string, metrics: MetricEntity[]) => void;
};

function SemanticModelRow({
    model,
    searchInput,
    isExpanded,
    isSelected,
    cachedMetrics,
    expandedMetricUrns,
    childMetricsByParentUrn,
    selectedUrn,
    onToggle,
    onMetricsFetched,
    onToggleMetric,
    onChildMetricsFetched,
}: SemanticModelRowProps) {
    const history = useHistory();
    const hasChildren = (model.metrics?.total ?? 0) > 0;

    const [fetchMetrics, { data: metricsData }] = useGetSemanticModelMetricsLazyQuery();

    // Fetch metrics for this model when it's first expanded.
    useEffect(() => {
        if (isExpanded && !cachedMetrics && hasChildren) {
            fetchMetrics({ variables: { urn: model.urn, input: { count: 100, query: '*' } } });
        }
    }, [isExpanded, cachedMetrics, hasChildren, fetchMetrics, model.urn]);

    // When fresh data arrives, populate the cache.
    useEffect(() => {
        if (!metricsData?.semanticModel?.metrics) return;
        const fetched = metricsData.semanticModel.metrics.searchResults
            .map((r) => r.entity)
            .filter((e): e is MetricEntity => e?.__typename === 'Metric');
        onMetricsFetched(model.urn, fetched);
    }, [metricsData, model.urn, onMetricsFetched]);

    const filteredMetrics = useMemo(() => {
        const metrics = cachedMetrics ?? [];
        if (!searchInput) return metrics;
        const q = searchInput.toLowerCase();
        return metrics.filter((m) => (m.info?.name ?? '').toLowerCase().includes(q));
    }, [cachedMetrics, searchInput]);

    const modelTitle = model.info?.name ?? model.urn;

    return (
        <>
            <MetricsTreeItem
                level={0}
                icon={Cube}
                platform={model.platform}
                title={modelTitle}
                isSelected={isSelected}
                hasChildren={hasChildren}
                isExpanded={isExpanded}
                onClick={() => history.push(`${PageRoutes.SEMANTIC_MODEL_ENTITY}/${encodeURIComponent(model.urn)}`)}
                onToggleExpand={hasChildren ? onToggle : undefined}
                testId={`metrics-sidebar-model-${model.urn}`}
            />
            {isExpanded &&
                filteredMetrics.map((metric) => (
                    <MetricRow
                        key={metric.urn}
                        level={1}
                        metric={metric}
                        searchInput={searchInput}
                        isExpanded={expandedMetricUrns.has(metric.urn)}
                        isSelected={selectedUrn === metric.urn}
                        cachedChildren={childMetricsByParentUrn[metric.urn]}
                        expandedMetricUrns={expandedMetricUrns}
                        childMetricsByParentUrn={childMetricsByParentUrn}
                        selectedUrn={selectedUrn}
                        onToggle={() => onToggleMetric(metric.urn)}
                        onChildMetricsFetched={onChildMetricsFetched}
                        onToggleMetric={onToggleMetric}
                    />
                ))}
        </>
    );
}

type MetricRowProps = {
    level: number;
    metric: MetricEntity;
    searchInput: string;
    isExpanded: boolean;
    isSelected: boolean;
    cachedChildren: MetricEntity[] | undefined;
    expandedMetricUrns: Set<string>;
    childMetricsByParentUrn: Record<string, MetricEntity[]>;
    selectedUrn: string | null;
    onToggle: () => void;
    onChildMetricsFetched: (parentUrn: string, metrics: MetricEntity[]) => void;
    onToggleMetric: (urn: string) => void;
};

function MetricRow({
    level,
    metric,
    searchInput,
    isExpanded,
    isSelected,
    cachedChildren,
    expandedMetricUrns,
    childMetricsByParentUrn,
    selectedUrn,
    onToggle,
    onChildMetricsFetched,
    onToggleMetric,
}: MetricRowProps) {
    const history = useHistory();
    const hasChildren = (metric.childMetrics?.total ?? 0) > 0;

    const [fetchChildren, { data: childData }] = useGetMetricChildrenLazyQuery();

    useEffect(() => {
        if (isExpanded && !cachedChildren && hasChildren) {
            fetchChildren({
                variables: {
                    input: {
                        types: [EntityType.Metric],
                        query: '*',
                        count: 100,
                        orFilters: [{ and: [{ field: 'parentMetric', values: [metric.urn] }] }],
                    },
                },
            });
        }
    }, [isExpanded, cachedChildren, hasChildren, metric.urn, fetchChildren]);

    useEffect(() => {
        if (!childData?.searchAcrossEntities) return;
        const directChildren = childData.searchAcrossEntities.searchResults
            .map((r) => r.entity)
            .filter((e): e is MetricEntity => e?.__typename === 'Metric');
        onChildMetricsFetched(metric.urn, directChildren);
    }, [childData, metric.urn, onChildMetricsFetched]);

    const filteredChildren = useMemo(() => {
        const children = cachedChildren ?? [];
        if (!searchInput) return children;
        const q = searchInput.toLowerCase();
        return children.filter((m) => (m.info?.name ?? '').toLowerCase().includes(q));
    }, [cachedChildren, searchInput]);

    const metricTitle = metric.info?.name ?? metric.urn;

    return (
        <>
            <MetricsTreeItem
                level={level}
                icon={Sigma}
                title={metricTitle}
                isSelected={isSelected}
                hasChildren={hasChildren}
                isExpanded={isExpanded}
                onClick={() => history.push(`${PageRoutes.METRIC_ENTITY}/${encodeURIComponent(metric.urn)}`)}
                onToggleExpand={hasChildren ? onToggle : undefined}
                testId={`metrics-sidebar-metric-${metric.urn}`}
            />
            {isExpanded &&
                filteredChildren.map((child) => (
                    <MetricRow
                        key={child.urn}
                        level={level + 1}
                        metric={child}
                        searchInput={searchInput}
                        isExpanded={expandedMetricUrns.has(child.urn)}
                        isSelected={selectedUrn === child.urn}
                        cachedChildren={childMetricsByParentUrn[child.urn]}
                        expandedMetricUrns={expandedMetricUrns}
                        childMetricsByParentUrn={childMetricsByParentUrn}
                        selectedUrn={selectedUrn}
                        onToggle={() => onToggleMetric(child.urn)}
                        onChildMetricsFetched={onChildMetricsFetched}
                        onToggleMetric={onToggleMetric}
                    />
                ))}
        </>
    );
}
