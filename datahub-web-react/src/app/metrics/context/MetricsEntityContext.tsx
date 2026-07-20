import React, { createContext, useCallback, useContext, useMemo, useRef, useState } from 'react';
import { matchPath, useLocation } from 'react-router-dom';

import { PageRoutes } from '@conf/Global';

import { EntityType } from '@types';

/** Minimal entity data needed by the sidebar to auto-expand the tree to the active entity. */
export type MetricsEntityData = {
    urn: string;
    entityType: EntityType;
    /** The semantic model that owns this metric (present when entityType === Metric). */
    semanticModel?: { urn: string } | null;
    /** Ancestor metrics from immediate parent to root, nearest first. */
    parentMetrics?: Array<{ urn: string }> | null;
};

type MetricsEntityContextType = {
    expandedSemanticModelUrns: Set<string>;
    expandedMetricUrns: Set<string>;
    selectedUrn: string | null;
    toggleSemanticModel: (urn: string) => void;
    toggleMetric: (urn: string) => void;
    /** Expand every semantic model in `urns` (union with current). */
    expandAllSemanticModels: (urns: string[]) => void;
    /** Collapse every expanded semantic model and metric. */
    collapseAllExpanded: () => void;
    /** Signal the sidebar to refetch root + all expanded children. */
    refetchTree: () => void;
    /** Incremented each time refetchTree() is called; consumers subscribe via useEffect. */
    refetchKey: number;
    /** Entity currently viewed in the profile pane; null on the /metrics home page. */
    entityData: MetricsEntityData | null;
    setEntityData: (data: MetricsEntityData | null) => void;
};

const MetricsEntityContext = createContext<MetricsEntityContextType>({
    expandedSemanticModelUrns: new Set(),
    expandedMetricUrns: new Set(),
    selectedUrn: null,
    toggleSemanticModel: () => {},
    toggleMetric: () => {},
    expandAllSemanticModels: () => {},
    collapseAllExpanded: () => {},
    refetchTree: () => {},
    refetchKey: 0,
    entityData: null,
    setEntityData: () => {},
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
    const [refetchKey, setRefetchKey] = useState(0);
    const [entityData, setEntityData] = useState<MetricsEntityData | null>(null);

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

    const expandAllSemanticModels = useCallback((urns: string[]) => {
        setExpandedSemanticModelUrns((prev) => {
            const next = new Set(prev);
            urns.forEach((urn) => next.add(urn));
            return next;
        });
    }, []);

    const collapseAllExpanded = useCallback(() => {
        setExpandedSemanticModelUrns(new Set());
        setExpandedMetricUrns(new Set());
    }, []);

    const refetchKeyRef = useRef(0);
    const refetchTree = useCallback(() => {
        refetchKeyRef.current += 1;
        setRefetchKey(refetchKeyRef.current);
    }, []);

    const value = useMemo(
        () => ({
            expandedSemanticModelUrns,
            expandedMetricUrns,
            selectedUrn,
            toggleSemanticModel,
            toggleMetric,
            expandAllSemanticModels,
            collapseAllExpanded,
            refetchTree,
            refetchKey,
            entityData,
            setEntityData,
        }),
        [
            expandedSemanticModelUrns,
            expandedMetricUrns,
            selectedUrn,
            toggleSemanticModel,
            toggleMetric,
            expandAllSemanticModels,
            collapseAllExpanded,
            refetchTree,
            refetchKey,
            entityData,
        ],
    );

    return <MetricsEntityContext.Provider value={value}>{children}</MetricsEntityContext.Provider>;
}

export function useMetricsEntityContext(): MetricsEntityContextType {
    return useContext(MetricsEntityContext);
}
