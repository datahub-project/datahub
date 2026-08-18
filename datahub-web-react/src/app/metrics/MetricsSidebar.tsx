import { Avatar, EmptyState, SearchBar } from '@components';
import { Sigma } from '@phosphor-icons/react/dist/csr/Sigma';
import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { matchPath, useLocation } from 'react-router-dom';
import { useDebounce } from 'react-use';
import styled from 'styled-components';

import { AvatarType } from '@components/components/AvatarStack/types';
import { SimpleSelect } from '@components/components/Select/SimpleSelect';

import MetricsSidebarSearchFilters from '@app/metrics/MetricsSidebarSearchFilters';
import MetricsSidebarSearchResults from '@app/metrics/MetricsSidebarSearchResults';
import { SemanticModelRow } from '@app/metrics/SemanticModelRow';
import { useMetricsEntityContext } from '@app/metrics/context/MetricsEntityContext';
import useMetricsSectionExpansion from '@app/metrics/hooks/useMetricsSectionExpansion';
import useMetricsSidebarFacetOptions from '@app/metrics/hooks/useMetricsSidebarFacetOptions';
import useMetricsSidebarSearch from '@app/metrics/hooks/useMetricsSidebarSearch';
import { SemanticModel } from '@app/metrics/metricsTypes';
import useSemanticModelRoots from '@app/metrics/useSemanticModelRoots';
import {
    SECONDARY_BROWSE_FILTERS,
    SecondaryBrowseFilter,
    isMetricsSidebarSearchActive,
    isSecondaryBrowseFilter,
    nextPromotedBrowseFilters,
} from '@app/metrics/utils/metricsSidebarMode';
import {
    DEFAULT_METRICS_SIDEBAR_SORT,
    METRICS_SIDEBAR_SORT,
    MetricsSidebarSortValue,
} from '@app/metrics/utils/metricsSidebarSort';
import HierarchicalBrowseSidebar from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/HierarchicalBrowseSidebar';
import SidebarAddFilter from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/SidebarAddFilter';
import SidebarHomeNavLink from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/SidebarHomeNavLink';
import SidebarSortSelect from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/SidebarSortSelect';
import { TreeSectionHeader } from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/TreeSectionHeader';
import { PageRoutes } from '@conf/Global';

import { useScrollSemanticModelsQuery } from '@graphql/metricsBrowse.generated';
import { EntityType } from '@types';

const EmptyStateWrapper = styled.div`
    flex: 1;
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 24px 12px;
`;

const OwnerOptionRow = styled.span`
    display: flex;
    align-items: center;
    gap: 8px;
`;

type Props = {
    isCollapsed: boolean;
    onToggleCollapsed: () => void;
    onExpandSidebar: () => void;
};

export default function MetricsSidebar({ isCollapsed, onToggleCollapsed, onExpandSidebar }: Props) {
    const { t } = useTranslation('misc');
    const location = useLocation();
    const [searchInput, setSearchInput] = useState('');
    const [debouncedQuery, setDebouncedQuery] = useState('');
    const [selectedPlatformUrns, setSelectedPlatformUrns] = useState<string[]>([]);
    const [selectedDomainUrns, setSelectedDomainUrns] = useState<string[]>([]);
    const [selectedTagUrns, setSelectedTagUrns] = useState<string[]>([]);
    const [selectedTermUrns, setSelectedTermUrns] = useState<string[]>([]);
    const [selectedOwnerUrns, setSelectedOwnerUrns] = useState<string[]>([]);
    const [sortSelection, setSortSelection] = useState<MetricsSidebarSortValue>(DEFAULT_METRICS_SIDEBAR_SORT);
    const [promotedBrowseFilters, setPromotedBrowseFilters] = useState<Set<SecondaryBrowseFilter>>(new Set());
    const [filterToAutoOpen, setFilterToAutoOpen] = useState<SecondaryBrowseFilter | null>(null);
    const [autoOpenNonce, setAutoOpenNonce] = useState(0);
    const [isModelsExpanded, setIsModelsExpanded] = useState(true);
    const isFirstSortEffectRef = useRef(true);

    const {
        expandedSemanticModelUrns,
        expandedMetricUrns,
        selectedUrn,
        toggleSemanticModel,
        toggleMetric,
        collapseAllExpanded,
        refetchKey,
        entityData,
    } = useMetricsEntityContext();

    const { isSectionExpanded, isExpanding, toggleExpandAll } = useMetricsSectionExpansion(sortSelection);

    const isHomeSelected = !!matchPath(location.pathname, { path: PageRoutes.METRICS, exact: true });

    useDebounce(() => setDebouncedQuery(searchInput), 200, [searchInput]);

    useEffect(() => {
        setPromotedBrowseFilters((prev) =>
            nextPromotedBrowseFilters(prev, {
                tagUrns: selectedTagUrns,
                ownerUrns: selectedOwnerUrns,
            }),
        );
    }, [selectedTagUrns, selectedOwnerUrns]);

    useEffect(() => {
        if (isFirstSortEffectRef.current) {
            isFirstSortEffectRef.current = false;
            return;
        }
        collapseAllExpanded();
    }, [sortSelection, collapseAllExpanded]);

    const demoteBrowseFilter = useCallback((key: SecondaryBrowseFilter) => {
        setPromotedBrowseFilters((prev) => {
            if (!prev.has(key)) return prev;
            const next = new Set(prev);
            next.delete(key);
            return next;
        });
    }, []);

    const promoteBrowseFilter = useCallback((key: SecondaryBrowseFilter) => {
        setPromotedBrowseFilters((prev) => {
            if (prev.has(key)) return prev;
            const next = new Set(prev);
            next.add(key);
            return next;
        });
    }, []);

    const handleAddBrowseFilter = useCallback(
        (value: string) => {
            if (!isSecondaryBrowseFilter(value)) return;
            promoteBrowseFilter(value);
            setFilterToAutoOpen(value);
            setAutoOpenNonce((n) => n + 1);
        },
        [promoteBrowseFilter],
    );

    const handleTagsChange = useCallback(
        (urns: string[]) => {
            setSelectedTagUrns(urns);
            setFilterToAutoOpen(null);
            if (urns.length === 0) demoteBrowseFilter('tag');
        },
        [demoteBrowseFilter],
    );

    const handleOwnersChange = useCallback(
        (urns: string[]) => {
            setSelectedOwnerUrns(urns);
            setFilterToAutoOpen(null);
            if (urns.length === 0) demoteBrowseFilter('owners');
        },
        [demoteBrowseFilter],
    );

    const searchModeInput = useMemo(
        () => ({
            platformUrns: selectedPlatformUrns,
            domainUrns: selectedDomainUrns,
            tagUrns: selectedTagUrns,
            termUrns: selectedTermUrns,
            ownerUrns: selectedOwnerUrns,
        }),
        [selectedPlatformUrns, selectedDomainUrns, selectedTagUrns, selectedTermUrns, selectedOwnerUrns],
    );

    const isSearchActive = isMetricsSidebarSearchActive({
        ...searchModeInput,
        searchInput,
    });
    const shouldFetchSearch = isMetricsSidebarSearchActive({
        ...searchModeInput,
        searchInput: debouncedQuery,
    });

    const { platformOptions, domainOptions, tagOptions, termOptions, ownerOptions } = useMetricsSidebarFacetOptions({
        searchQuery: debouncedQuery,
        platformUrns: selectedPlatformUrns,
        domainUrns: selectedDomainUrns,
        tagUrns: selectedTagUrns,
        termUrns: selectedTermUrns,
        ownerUrns: selectedOwnerUrns,
        includeTagFacets: promotedBrowseFilters.has('tag') || selectedTagUrns.length > 0,
        includeOwnerFacets: promotedBrowseFilters.has('owners') || selectedOwnerUrns.length > 0,
    });

    const {
        metrics: searchResults,
        total: searchTotal,
        loading: searchLoading,
        isRefreshing: searchRefreshing,
        scrollRef: searchScrollRef,
    } = useMetricsSidebarSearch({
        searchQuery: debouncedQuery,
        platformUrns: selectedPlatformUrns,
        domainUrns: selectedDomainUrns,
        tagUrns: selectedTagUrns,
        termUrns: selectedTermUrns,
        ownerUrns: selectedOwnerUrns,
        sort: sortSelection,
        skip: !shouldFetchSearch,
    });

    const searchResultsLoading =
        (isSearchActive && !shouldFetchSearch) || (searchLoading && searchResults.length === 0);

    const {
        data: rootModels,
        scrollRef: rootScrollRef,
        refetch: refetchModels,
    } = useSemanticModelRoots(sortSelection, isSearchActive);

    const missingModelUrn =
        entityData?.entityType === EntityType.Metric && entityData.semanticModel?.urn
            ? entityData.semanticModel.urn
            : null;
    const isMissingFromRoots = missingModelUrn != null && !rootModels.some((m) => m.urn === missingModelUrn);

    const { data: fallbackData } = useScrollSemanticModelsQuery({
        skip: !isMissingFromRoots || missingModelUrn == null || isSearchActive,
        variables: {
            input: {
                query: '*',
                types: [EntityType.SemanticModel],
                count: 1,
                orFilters: [{ and: [{ field: 'urn', condition: 'EQUAL' as any, values: [missingModelUrn ?? ''] }] }],
            },
        },
    });

    const allModels: SemanticModel[] = useMemo(() => {
        if (!isMissingFromRoots) return rootModels;
        const fallbackModels = (fallbackData?.scrollAcrossEntities?.searchResults ?? [])
            .map((r) => r.entity)
            .filter((e): e is SemanticModel => e?.__typename === 'SemanticModel');
        if (fallbackModels.length === 0) return rootModels;
        const existingUrns = new Set(rootModels.map((m) => m.urn));
        const newModels = fallbackModels.filter((m) => !existingUrns.has(m.urn));
        return newModels.length > 0 ? [...newModels, ...rootModels] : rootModels;
    }, [rootModels, fallbackData, isMissingFromRoots]);

    useEffect(() => {
        if (refetchKey > 0) {
            refetchModels();
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [refetchKey]);

    const handleToggleExpandAll = useCallback(() => {
        // Opening nested rows is useless if the section itself is collapsed.
        if (!isSectionExpanded) {
            setIsModelsExpanded(true);
        }
        toggleExpandAll(allModels);
    }, [isSectionExpanded, toggleExpandAll, allModels]);

    const handleClearSearch = useCallback(() => {
        setSearchInput('');
        setDebouncedQuery('');
        setSelectedPlatformUrns([]);
        setSelectedDomainUrns([]);
        setSelectedTagUrns([]);
        setSelectedTermUrns([]);
        setSelectedOwnerUrns([]);
        setPromotedBrowseFilters(new Set());
        setFilterToAutoOpen(null);
    }, []);

    const sortOptions = useMemo(
        () => [
            { value: METRICS_SIDEBAR_SORT.NAME_ASC, label: t('sidebarSort.nameAtoZ') },
            { value: METRICS_SIDEBAR_SORT.NAME_DESC, label: t('sidebarSort.nameZtoA') },
            { value: METRICS_SIDEBAR_SORT.LAST_MODIFIED_DESC, label: t('sidebarSort.lastModified') },
        ],
        [t],
    );

    const addFilterOptions = useMemo(() => {
        const labels: Record<SecondaryBrowseFilter, string> = {
            tag: t('context.tagFilter.label'),
            owners: t('metrics.ownersFilter.label'),
        };
        return SECONDARY_BROWSE_FILTERS.filter((key) => !promotedBrowseFilters.has(key)).map((key) => ({
            value: key,
            label: labels[key],
        }));
    }, [promotedBrowseFilters, t]);

    return (
        <HierarchicalBrowseSidebar
            title={t('metrics.sidebarTitle')}
            isCollapsed={isCollapsed}
            onToggleCollapsed={onToggleCollapsed}
            onExpandSidebar={onExpandSidebar}
            dataTestId="metrics-sidebar"
            collapseButtonTestId="metrics-sidebar-collapse-button"
            collapsedSearchAriaLabel={t('metrics.searchAriaLabel')}
            collapsedSearchTestId="metrics-sidebar-search-icon"
            search={
                <SearchBar
                    placeholder={t('metrics.searchPlaceholder')}
                    value={searchInput}
                    onChange={setSearchInput}
                    data-testid="metrics-sidebar-search-input"
                />
            }
            sort={
                <SidebarSortSelect
                    options={sortOptions}
                    value={sortSelection}
                    onChange={(next) => setSortSelection(next as MetricsSidebarSortValue)}
                    dataTestId="metrics-sidebar-sort"
                />
            }
            filters={
                <>
                    <MetricsSidebarSearchFilters
                        selectedPlatformUrns={selectedPlatformUrns}
                        selectedDomainUrns={selectedDomainUrns}
                        selectedTagUrns={selectedTagUrns}
                        selectedTermUrns={selectedTermUrns}
                        platformOptions={platformOptions}
                        domainOptions={domainOptions}
                        tagOptions={tagOptions}
                        termOptions={termOptions}
                        onPlatformsChange={setSelectedPlatformUrns}
                        onDomainsChange={setSelectedDomainUrns}
                        onTagsChange={handleTagsChange}
                        onTermsChange={setSelectedTermUrns}
                        showTagFilter={promotedBrowseFilters.has('tag')}
                        defaultOpenTagFilter={filterToAutoOpen === 'tag'}
                        tagFilterKey={filterToAutoOpen === 'tag' ? `tag-${autoOpenNonce}` : 'tag'}
                    />
                    {promotedBrowseFilters.has('owners') && (
                        <SimpleSelect
                            key={filterToAutoOpen === 'owners' ? `owners-${autoOpenNonce}` : 'owners'}
                            size="sm"
                            width="fit-content"
                            isMultiSelect
                            showSearch
                            filterResultsByQuery
                            defaultOpen={filterToAutoOpen === 'owners'}
                            isDisabled={
                                ownerOptions.length === 0 &&
                                selectedOwnerUrns.length === 0 &&
                                filterToAutoOpen !== 'owners'
                            }
                            placeholder={t('metrics.ownersFilter.placeholder')}
                            selectLabelProps={{ variant: 'labeled', label: t('metrics.ownersFilter.label') }}
                            options={ownerOptions}
                            values={selectedOwnerUrns}
                            onUpdate={handleOwnersChange}
                            renderCustomOptionText={(option) => {
                                const { creator } = option as (typeof ownerOptions)[number];
                                return (
                                    <OwnerOptionRow>
                                        <Avatar
                                            name={creator.displayName}
                                            imageUrl={creator.pictureLink ?? undefined}
                                            type={
                                                creator.type === EntityType.CorpGroup
                                                    ? AvatarType.group
                                                    : AvatarType.user
                                            }
                                            showInPill
                                            size="sm"
                                        />
                                    </OwnerOptionRow>
                                );
                            }}
                            dataTestId="metrics-sidebar-owners-filter"
                        />
                    )}
                    <SidebarAddFilter
                        options={addFilterOptions}
                        onAdd={handleAddBrowseFilter}
                        dataTestId="metrics-sidebar-add-filter"
                    />
                </>
            }
            homeNav={
                <SidebarHomeNavLink
                    to={PageRoutes.METRICS}
                    isSelected={isHomeSelected}
                    label={t('metrics.allMetrics')}
                    data-testid="metrics-sidebar-home"
                />
            }
        >
            {isSearchActive ? (
                <MetricsSidebarSearchResults
                    metrics={searchResults}
                    total={searchTotal}
                    loading={searchResultsLoading}
                    isRefreshing={searchRefreshing}
                    selectedUrn={selectedUrn}
                    scrollRef={searchScrollRef}
                    onClear={handleClearSearch}
                />
            ) : (
                <div data-testid="metrics-sidebar-tree" key={sortSelection}>
                    <TreeSectionHeader
                        level={0}
                        label={t('metrics.semanticModelsSection')}
                        isExpanded={isModelsExpanded}
                        onToggle={() => setIsModelsExpanded((v) => !v)}
                        onToggleExpandAll={handleToggleExpandAll}
                        isAllExpanded={isSectionExpanded}
                        expandAllLoading={isExpanding}
                        expandAllLabel={t('metrics.expandAll')}
                        collapseAllLabel={t('metrics.collapseAll')}
                        testId="metrics-sidebar-models-section"
                    />
                    {isModelsExpanded && (
                        <>
                            {allModels.length === 0 && (
                                <EmptyStateWrapper>
                                    <EmptyState
                                        icon={Sigma}
                                        title={t('metrics.emptyTreeTitle')}
                                        description={t('metrics.emptyTreeDescription')}
                                        size="sm"
                                    />
                                </EmptyStateWrapper>
                            )}
                            {allModels.map((model) => (
                                <SemanticModelRow
                                    key={model.urn}
                                    model={model}
                                    isExpanded={expandedSemanticModelUrns.has(model.urn)}
                                    isSelected={selectedUrn === model.urn}
                                    expandedMetricUrns={expandedMetricUrns}
                                    selectedUrn={selectedUrn}
                                    sort={sortSelection}
                                    onToggle={() => toggleSemanticModel(model.urn)}
                                    onToggleMetric={toggleMetric}
                                />
                            ))}
                            <div ref={rootScrollRef} style={{ height: 1 }} />
                        </>
                    )}
                </div>
            )}
        </HierarchicalBrowseSidebar>
    );
}
