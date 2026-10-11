import { useApolloClient } from '@apollo/client';
import { useCallback, useState } from 'react';

import { useMetricsEntityContext } from '@app/metrics/context/MetricsEntityContext';
import { MetricEntity } from '@app/metrics/metricsTypes';
import { METRIC_CHILDREN_COUNT } from '@app/metrics/useMetricChildren';
import {
    DEFAULT_METRICS_SIDEBAR_SORT,
    MetricsSidebarSortValue,
    metricsSidebarSortToCriterion,
} from '@app/metrics/utils/metricsSidebarSort';
import {
    MetricsExpandParent,
    MetricsExpandableNode,
    expandAllMetricsTree,
} from '@app/metrics/utils/metricsTreeExpansion';

import { ScrollMetricsDocument, ScrollMetricsQuery } from '@graphql/metricsBrowse.generated';
import { EntityType } from '@types';

type ModelRoot = {
    urn: string;
    metrics?: { total?: number | null } | null;
};

/**
 * Section expand-all / collapse-all for the metrics sidebar.
 * Mirrors Documents `useSectionExpansion`: capped BFS with loading state.
 */
export default function useMetricsSectionExpansion(sort: MetricsSidebarSortValue = DEFAULT_METRICS_SIDEBAR_SORT) {
    const client = useApolloClient();
    const {
        expandedSemanticModelUrns,
        expandedMetricUrns,
        expandAllSemanticModels,
        expandAllMetrics,
        collapseAllExpanded,
    } = useMetricsEntityContext();
    const [isExpanding, setIsExpanding] = useState(false);

    const isSectionExpanded = expandedSemanticModelUrns.size > 0 || expandedMetricUrns.size > 0;

    const loadChildren = useCallback(
        async (parent: MetricsExpandParent): Promise<MetricsExpandableNode[]> => {
            const orFilters =
                parent.kind === 'model'
                    ? [
                          {
                              and: [
                                  { field: 'semanticModel', values: [parent.urn] },
                                  { field: 'hasParentMetric', values: ['false'] },
                              ],
                          },
                      ]
                    : [{ and: [{ field: 'parentMetric', values: [parent.urn] }] }];

            const { data } = await client.query<ScrollMetricsQuery>({
                query: ScrollMetricsDocument,
                variables: {
                    input: {
                        query: '*',
                        types: [EntityType.Metric],
                        count: METRIC_CHILDREN_COUNT,
                        orFilters,
                        sortInput: {
                            sortCriteria: [metricsSidebarSortToCriterion(sort)],
                        },
                        searchFlags: { skipCache: true },
                    },
                },
                fetchPolicy: 'network-only',
            });

            return (data?.scrollAcrossEntities?.searchResults ?? [])
                .map((r) => r.entity)
                .filter((e): e is MetricEntity => e?.__typename === 'Metric')
                .map((metric) => ({
                    urn: metric.urn,
                    hasChildren: (metric.childMetrics?.total ?? 0) > 0,
                }));
        },
        [client, sort],
    );

    const toggleExpandAll = useCallback(
        async (modelRoots: ModelRoot[]) => {
            if (isSectionExpanded) {
                collapseAllExpanded();
                return;
            }

            const roots: MetricsExpandableNode[] = modelRoots.map((m) => ({
                urn: m.urn,
                hasChildren: (m.metrics?.total ?? 0) > 0,
            }));

            if (roots.every((r) => !r.hasChildren)) {
                return;
            }

            setIsExpanding(true);
            try {
                await expandAllMetricsTree({
                    modelRoots: roots,
                    loadChildren,
                    onExpandModels: (urns) => expandAllSemanticModels(urns),
                    onExpandMetrics: (urns) => expandAllMetrics(urns),
                });
            } catch {
                // Expand-all is best-effort; failed Apollo page loads must not
                // surface as unhandled rejections from the click handler.
            } finally {
                setIsExpanding(false);
            }
        },
        [isSectionExpanded, collapseAllExpanded, loadChildren, expandAllSemanticModels, expandAllMetrics],
    );

    return {
        isSectionExpanded,
        isExpanding,
        toggleExpandAll,
    };
}
