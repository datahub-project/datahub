import React, { createContext, useCallback, useContext, useEffect, useMemo, useState } from 'react';

import { useUserContext } from '@app/context/useUserContext';
import { SCHEMA_TARGET } from '@app/entityV2/columnView/types';

import { useGetColumnViewQuery, useGetGlobalColumnViewsSettingsQuery } from '@graphql/columnView.generated';
import { DataHubColumnView, DataHubColumnViewDefinition } from '@types';

/**
 * Column View state for the Schema tab.
 *
 * Selection persistence mirrors Views' localStorage tri-state (UserContextProvider /
 * LocalState.selectedViewUrn), keyed by target in `LocalState.selectedColumnViewUrns`:
 *   undefined -> apply personal -> org default once
 *   null      -> user chose Default; do not re-apply
 *   urn       -> sticky
 * Ad hoc modifications live in React state only: they survive navigating between datasets while
 * the provider stays mounted and reset on reload.
 */
interface ColumnViewContextValue {
    /** The saved view currently selected, or undefined for the built-in Default. */
    selectedColumnView?: DataHubColumnView;
    /** The saved default (personal -> org) for this target, if any. */
    defaultColumnView?: DataHubColumnView;
    /**
     * The definition that drives the table: the ad hoc definition if the user modified columns,
     * else the selected view's definition, else undefined (= built-in behavior).
     */
    activeDefinition?: DataHubColumnViewDefinition;
    /** True when the table shows an ad hoc modification of the selected view / Default. */
    isAdHocModified: boolean;
    /** Persisted selection: undefined = follow default, null = Default, urn = sticky. */
    selectedUrn: string | null | undefined;
    setSelectedUrn: (urn: string | null | undefined) => void;
    /** Ad hoc edits; pass undefined to reset to the selected view. */
    setAdHocDefinition: (definition: DataHubColumnViewDefinition | undefined) => void;
    loading: boolean;
}

const ColumnViewContext = createContext<ColumnViewContextValue>({
    isAdHocModified: false,
    selectedUrn: undefined,
    setSelectedUrn: () => undefined,
    setAdHocDefinition: () => undefined,
    loading: false,
});

export const useColumnViewContext = () => useContext(ColumnViewContext);
/** Convenience for SchemaTable: the definition to render, or undefined for built-in behavior. */
export const useActiveColumnViewDefinition = () => useContext(ColumnViewContext).activeDefinition;

export function ColumnViewProvider({
    children,
    target = SCHEMA_TARGET,
}: {
    children: React.ReactNode;
    target?: string;
}) {
    const userContext = useUserContext();
    const { localState, updateLocalState } = userContext;
    const selectedUrn = localState.selectedColumnViewUrns?.[target];
    const [adHocDefinition, setAdHocDefinition] = useState<DataHubColumnViewDefinition | undefined>(undefined);

    const persistSelection = useCallback(
        (urn: string | null | undefined) =>
            updateLocalState({
                ...localState,
                selectedColumnViewUrns: { ...(localState.selectedColumnViewUrns || {}), [target]: urn },
            }),
        [localState, updateLocalState, target],
    );

    const setSelectedUrn = useCallback(
        (urn: string | null | undefined) => {
            persistSelection(urn);
            setAdHocDefinition(undefined);
        },
        [persistSelection],
    );

    // Personal default for this target, read off the already-loaded user settings.
    const personalDefaultUrn = (userContext.user?.settings?.columnViews?.defaults || []).find(
        (d) => d.target === target,
    )?.view?.urn;

    const { data: globalData, loading: globalLoading } = useGetGlobalColumnViewsSettingsQuery({
        skip: Boolean(personalDefaultUrn),
        fetchPolicy: 'cache-first',
    });
    const globalDefaultUrn = globalData?.globalColumnViewsSettings?.defaults?.find((d) => d.target === target)?.view;
    const defaultUrn = personalDefaultUrn ?? globalDefaultUrn;
    const defaultsLoaded = Boolean(userContext.user) && (Boolean(personalDefaultUrn) || !globalLoading);

    // Apply the default exactly once: only while the persisted selection is still undefined.
    useEffect(() => {
        if (selectedUrn === undefined && defaultsLoaded && defaultUrn) persistSelection(defaultUrn);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [selectedUrn, defaultsLoaded, defaultUrn]);

    const effectiveUrn = selectedUrn === undefined ? defaultUrn : selectedUrn;

    // Caller-context fetch of the chosen view; pure projection, no system context anywhere.
    const { data: viewData, loading: viewLoading } = useGetColumnViewQuery({
        variables: { urn: effectiveUrn as string },
        skip: !effectiveUrn,
        fetchPolicy: 'cache-first',
    });
    const { data: defaultViewData } = useGetColumnViewQuery({
        variables: { urn: defaultUrn as string },
        skip: !defaultUrn || defaultUrn === effectiveUrn,
        fetchPolicy: 'cache-first',
    });

    const value = useMemo<ColumnViewContextValue>(() => {
        const selectedColumnView = effectiveUrn ? (viewData?.columnView as DataHubColumnView | undefined) : undefined;
        return {
            selectedColumnView,
            defaultColumnView: defaultUrn
                ? ((defaultUrn === effectiveUrn ? viewData?.columnView : defaultViewData?.columnView) as
                      | DataHubColumnView
                      | undefined)
                : undefined,
            activeDefinition: adHocDefinition ?? selectedColumnView?.definition,
            isAdHocModified: adHocDefinition !== undefined,
            selectedUrn,
            setSelectedUrn,
            setAdHocDefinition,
            loading: globalLoading || viewLoading,
        };
    }, [
        effectiveUrn,
        defaultUrn,
        viewData,
        defaultViewData,
        adHocDefinition,
        selectedUrn,
        setSelectedUrn,
        globalLoading,
        viewLoading,
    ]);

    return <ColumnViewContext.Provider value={value}>{children}</ColumnViewContext.Provider>;
}
