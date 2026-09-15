import React, { createContext, useCallback, useContext, useMemo, useRef, useState } from 'react';

type ExpandableApi = {
    expand: () => void;
    collapse: () => void;
};

type TreeExpansionRegistryValue = {
    hasAnyExpanded: boolean;
    expandAll: () => void;
    collapseAll: () => void;
    register: (urn: string, api: ExpandableApi) => void;
    unregister: (urn: string, api?: ExpandableApi) => void;
    reportExpanded: (urn: string, isExpanded: boolean) => void;
};

const TreeExpansionRegistryContext = createContext<TreeExpansionRegistryValue | null>(null);

export function TreeExpansionRegistryProvider({ children }: { children: React.ReactNode }) {
    const apisRef = useRef<Map<string, ExpandableApi>>(new Map());
    const expandedRef = useRef<Set<string>>(new Set());
    const [hasAnyExpanded, setHasAnyExpanded] = useState(false);

    const syncHasAnyExpanded = useCallback(() => {
        setHasAnyExpanded(expandedRef.current.size > 0);
    }, []);

    const register = useCallback((urn: string, api: ExpandableApi) => {
        apisRef.current.set(urn, api);
    }, []);

    // Optional api arg: ignore stale cleanups when a newer register replaced the entry
    // (Strict Mode remount / rapid expand can otherwise drop a live registration).
    const unregister = useCallback(
        (urn: string, api?: ExpandableApi) => {
            const current = apisRef.current.get(urn);
            if (api != null && current != null && current !== api) {
                return;
            }
            apisRef.current.delete(urn);
            expandedRef.current.delete(urn);
            syncHasAnyExpanded();
        },
        [syncHasAnyExpanded],
    );

    const reportExpanded = useCallback(
        (urn: string, isExpanded: boolean) => {
            if (isExpanded) {
                expandedRef.current.add(urn);
            } else {
                expandedRef.current.delete(urn);
            }
            syncHasAnyExpanded();
        },
        [syncHasAnyExpanded],
    );

    const expandAll = useCallback(() => {
        // Snapshot so registers from newly mounted children during expand()
        // don't mutate the iteration set mid-pass (still not recursive — by design).
        Array.from(apisRef.current.values()).forEach((api) => api.expand());
    }, []);

    const collapseAll = useCallback(() => {
        Array.from(apisRef.current.values()).forEach((api) => api.collapse());
        expandedRef.current.clear();
        setHasAnyExpanded(false);
    }, []);

    const value = useMemo(
        () => ({
            hasAnyExpanded,
            expandAll,
            collapseAll,
            register,
            unregister,
            reportExpanded,
        }),
        [hasAnyExpanded, expandAll, collapseAll, register, unregister, reportExpanded],
    );

    return <TreeExpansionRegistryContext.Provider value={value}>{children}</TreeExpansionRegistryContext.Provider>;
}

// Hook lives alongside the provider (same pattern as DocumentTreeContext).
// eslint-disable-next-line react-refresh/only-export-components
export function useTreeExpansionRegistry(): TreeExpansionRegistryValue | null {
    return useContext(TreeExpansionRegistryContext);
}
