import React, { createContext, useCallback, useContext, useMemo, useRef, useState } from 'react';
import { matchPath, useLocation } from 'react-router-dom';

import { PageRoutes } from '@conf/Global';

import { GetSemanticModelMetricsQuery } from '@graphql/metricsBrowse.generated';

type SearchResultEntity = NonNullable<
    NonNullable<
        NonNullable<GetSemanticModelMetricsQuery['semanticModel']>['metrics']
    >['searchResults'][number]['entity']
>;
type MetricSearchResult = Extract<SearchResultEntity, { __typename?: 'Metric' }>;

type MetricsEntityContextType = {
    expandedSemanticModelUrns: Set<string>;
    expandedMetricUrns: Set<string>;
    selectedUrn: string | null;
    /** Cached per-semantic-model metric fetch results, keyed by model URN. */
    childMetricsByModelUrn: Record<string, MetricSearchResult[]>;
    /** Cached per-metric child metrics fetch results, keyed by parent metric URN. */
    childMetricsByParentUrn: Record<string, MetricSearchResult[]>;
    toggleSemanticModel: (urn: string) => void;
    toggleMetric: (urn: string) => void;
    setChildMetricsForModel: (modelUrn: string, metrics: MetricSearchResult[]) => void;
    setChildMetricsForParent: (parentUrn: string, metrics: MetricSearchResult[]) => void;
    /** Signal the sidebar to refetch root + all expanded children. */
    refetchTree: () => void;
    /** Incremented each time refetchTree() is called; consumers subscribe via useEffect. */
    refetchKey: number;
};

const MetricsEntityContext = createContext<MetricsEntityContextType>({
    expandedSemanticModelUrns: new Set(),
    expandedMetricUrns: new Set(),
    selectedUrn: null,
    childMetricsByModelUrn: {},
    childMetricsByParentUrn: {},
    toggleSemanticModel: () => {},
    toggleMetric: () => {},
    setChildMetricsForModel: () => {},
    setChildMetricsForParent: () => {},
    refetchTree: () => {},
    refetchKey: 0,
});

type Props = {
    children: React.ReactNode;
};

function toggleInSet(prev: Set<string>, value: string): Set<string> {
    const next = new Set(prev);
    if (next.has(value)) {
        next.delete(value);
    } else {
        next.add(value);
    }
    return next;
}

export function MetricsEntityContextProvider({ children }: Props) {
    const location = useLocation();
    const [expandedSemanticModelUrns, setExpandedSemanticModelUrns] = useState<Set<string>>(new Set());
    const [expandedMetricUrns, setExpandedMetricUrns] = useState<Set<string>>(new Set());
    const [childMetricsByModelUrn, setChildMetricsByModelUrn] = useState<Record<string, MetricSearchResult[]>>({});
    const [childMetricsByParentUrn, setChildMetricsByParentUrn] = useState<Record<string, MetricSearchResult[]>>({});
    const [refetchKey, setRefetchKey] = useState(0);

    // Derive selectedUrn from the current route without extra renders.
    const selectedUrn = useMemo(() => {
        const metricMatch = matchPath<{ urn: string }>(location.pathname, {
            path: `${PageRoutes.METRIC_ENTITY}/:urn`,
        });
        if (metricMatch) return decodeURIComponent(metricMatch.params.urn);

        const modelMatch = matchPath<{ urn: string }>(location.pathname, {
            path: `${PageRoutes.SEMANTIC_MODEL_ENTITY}/:urn`,
        });
        if (modelMatch) return decodeURIComponent(modelMatch.params.urn);

        return null;
    }, [location.pathname]);

    const toggleSemanticModel = useCallback((urn: string) => {
        setExpandedSemanticModelUrns((prev) => toggleInSet(prev, urn));
    }, []);

    const toggleMetric = useCallback((urn: string) => {
        setExpandedMetricUrns((prev) => toggleInSet(prev, urn));
    }, []);

    const setChildMetricsForModel = useCallback((modelUrn: string, metrics: MetricSearchResult[]) => {
        setChildMetricsByModelUrn((prev) => ({ ...prev, [modelUrn]: metrics }));
    }, []);

    const setChildMetricsForParent = useCallback((parentUrn: string, metrics: MetricSearchResult[]) => {
        setChildMetricsByParentUrn((prev) => ({ ...prev, [parentUrn]: metrics }));
    }, []);

    // Use a ref to avoid stale-closure captures in the callback.
    const refetchKeyRef = useRef(0);
    const refetchTree = useCallback(() => {
        refetchKeyRef.current += 1;
        setRefetchKey(refetchKeyRef.current);
        setChildMetricsByModelUrn({});
        setChildMetricsByParentUrn({});
    }, []);

    const value = useMemo(
        () => ({
            expandedSemanticModelUrns,
            expandedMetricUrns,
            selectedUrn,
            childMetricsByModelUrn,
            childMetricsByParentUrn,
            toggleSemanticModel,
            toggleMetric,
            setChildMetricsForModel,
            setChildMetricsForParent,
            refetchTree,
            refetchKey,
        }),
        [
            expandedSemanticModelUrns,
            expandedMetricUrns,
            selectedUrn,
            childMetricsByModelUrn,
            childMetricsByParentUrn,
            toggleSemanticModel,
            toggleMetric,
            setChildMetricsForModel,
            setChildMetricsForParent,
            refetchTree,
            refetchKey,
        ],
    );

    return <MetricsEntityContext.Provider value={value}>{children}</MetricsEntityContext.Provider>;
}

export function useMetricsEntityContext(): MetricsEntityContextType {
    return useContext(MetricsEntityContext);
}
