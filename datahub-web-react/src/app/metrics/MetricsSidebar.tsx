import { EmptyState } from '@components';
import { Sigma } from '@phosphor-icons/react/dist/csr/Sigma';
import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { matchPath, useLocation } from 'react-router-dom';
import styled from 'styled-components';

import { SimpleSelect } from '@components/components/Select/SimpleSelect';

import MetricsSearch from '@app/metrics/MetricsSearch';
import { SemanticModelRow } from '@app/metrics/SemanticModelRow';
import { useMetricsEntityContext } from '@app/metrics/context/MetricsEntityContext';
import { SemanticModel } from '@app/metrics/metricsTypes';
import useSemanticModelRoots from '@app/metrics/useSemanticModelRoots';
import PlatformIcon from '@app/sharedV2/icons/PlatformIcon';
import HierarchicalBrowseSidebar from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/HierarchicalBrowseSidebar';
import SidebarHomeNavLink from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/SidebarHomeNavLink';
import { TreeSectionHeader } from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/TreeSectionHeader';
import { PageRoutes } from '@conf/Global';

import { useScrollSemanticModelsQuery } from '@graphql/metricsBrowse.generated';
import { DataPlatform, EntityType } from '@types';

const EmptyStateWrapper = styled.div`
    flex: 1;
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 24px 12px;
`;

const ALL_OPTION = '__all__';

type Props = {
    isCollapsed: boolean;
    onToggleCollapsed: () => void;
    onExpandSidebar: () => void;
};

export default function MetricsSidebar({ isCollapsed, onToggleCollapsed, onExpandSidebar }: Props) {
    const { t } = useTranslation('misc');
    const location = useLocation();
    const [platformFilter, setPlatformFilter] = useState(ALL_OPTION);
    const [isModelsExpanded, setIsModelsExpanded] = useState(true);

    const {
        expandedSemanticModelUrns,
        expandedMetricUrns,
        selectedUrn,
        toggleSemanticModel,
        toggleMetric,
        expandAllSemanticModels,
        collapseAllExpanded,
        refetchKey,
        entityData,
    } = useMetricsEntityContext();

    const isHomeSelected = !!matchPath(location.pathname, { path: PageRoutes.METRICS, exact: true });

    const { data: rootModels, scrollRef: rootScrollRef, refetch: refetchModels } = useSemanticModelRoots();

    const missingModelUrn =
        entityData?.entityType === EntityType.Metric && entityData.semanticModel?.urn
            ? entityData.semanticModel.urn
            : null;
    const isMissingFromRoots = missingModelUrn != null && !rootModels.some((m) => m.urn === missingModelUrn);

    const { data: fallbackData } = useScrollSemanticModelsQuery({
        skip: !isMissingFromRoots || missingModelUrn == null,
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

    const visibleModels = useMemo(() => {
        if (platformFilter === ALL_OPTION) return allModels;
        return allModels.filter((m) => m.platform?.urn === platformFilter);
    }, [allModels, platformFilter]);

    const isSectionExpanded = expandedSemanticModelUrns.size > 0 || expandedMetricUrns.size > 0;
    const handleToggleExpandAll = useCallback(() => {
        if (isSectionExpanded) {
            collapseAllExpanded();
            return;
        }
        setIsModelsExpanded(true);
        const expandable = visibleModels.filter((m) => (m.metrics?.total ?? 0) > 0).map((m) => m.urn);
        expandAllSemanticModels(expandable);
    }, [isSectionExpanded, visibleModels, collapseAllExpanded, expandAllSemanticModels]);

    const platformFilterControl =
        platformOptions.length > 1 ? (
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
        ) : null;

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
            search={<MetricsSearch />}
            filters={platformFilterControl}
            homeNav={
                <SidebarHomeNavLink
                    to={PageRoutes.METRICS}
                    isSelected={isHomeSelected}
                    label={t('metrics.allMetrics')}
                    data-testid="metrics-sidebar-home"
                />
            }
        >
            <div data-testid="metrics-sidebar-tree">
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
                        {visibleModels.map((model) => (
                            <SemanticModelRow
                                key={model.urn}
                                model={model}
                                isExpanded={expandedSemanticModelUrns.has(model.urn)}
                                isSelected={selectedUrn === model.urn}
                                expandedMetricUrns={expandedMetricUrns}
                                selectedUrn={selectedUrn}
                                onToggle={() => toggleSemanticModel(model.urn)}
                                onToggleMetric={toggleMetric}
                            />
                        ))}
                        <div ref={rootScrollRef} style={{ height: 1 }} />
                    </>
                )}
            </div>
        </HierarchicalBrowseSidebar>
    );
}
