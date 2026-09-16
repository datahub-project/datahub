import { ApolloClient, ApolloError, DocumentNode, useApolloClient } from '@apollo/client';
import { cloneDeep } from 'lodash';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { combineEntityDataWithSiblings } from '@app/entity/shared/siblingUtils';
import { useIsSeparateSiblingsMode } from '@app/entityV2/shared/useIsSeparateSiblingsMode';

import {
    GetDatasetSchemaDocument,
    GetDatasetSchemaQuery,
    GetDatasetSchemaStructuralDocument,
    GetDatasetSchemaStructuralQuery,
} from '@graphql/dataset.generated';
import { EntityType } from '@types';

// Whether to dynamically load the schema from the backend.
const shouldLoadSchema = (entityType, entityData) => {
    return entityType === EntityType.Dataset && !entityData?.schemaMetadata;
};

type StructuralDataset = NonNullable<GetDatasetSchemaStructuralQuery['dataset']>;
type FullDataset = NonNullable<GetDatasetSchemaQuery['dataset']>;
/** The one schema copy this hook owns: structural fields first, full metadata merged in later. */
type SchemaDataset = StructuralDataset | FullDataset;

interface SchemaState {
    /** Dataset the copy belongs to; outputs are withheld while it differs from the current urn. */
    urn: string | null;
    dataset: SchemaDataset | null;
    /** dataset carries at least field paths and types (Phase 1). */
    hasStructural: boolean;
    /** dataset carries descriptions, tags, terms, editable metadata (Phase 2). */
    hasFull: boolean;
    structuralLoading: boolean;
    fullLoading: boolean;
    structuralError?: ApolloError;
    fullError?: ApolloError;
}

const EMPTY_STATE: SchemaState = {
    urn: null,
    dataset: null,
    hasStructural: false,
    hasFull: false,
    structuralLoading: false,
    fullLoading: false,
};

interface Reload {
    token: number;
    /** Re-run only Phase 2 when full metadata is already present (an edit was saved). */
    fullOnly: boolean;
}

/**
 * Run one schema query imperatively. Unlike useQuery, nothing retains the response once
 * this hook has copied what it needs out of it, so a 20 MB result is garbage as soon as
 * the next phase has been merged.
 */
async function runSchemaQuery<T>(
    client: ApolloClient<object>,
    query: DocumentNode,
    urn: string,
): Promise<{ data?: T; error?: ApolloError }> {
    try {
        const result = await client.query<T>({
            query,
            variables: { urn },
            fetchPolicy: 'no-cache',
            errorPolicy: 'all',
        });
        const dataset = (result.data as { dataset?: unknown } | undefined)?.dataset;
        if (!dataset) {
            return {
                error: new ApolloError({
                    graphQLErrors: result.errors ?? [],
                    errorMessage: result.errors?.length ? undefined : 'Dataset not found',
                }),
            };
        }
        return { data: result.data };
    } catch (e) {
        return { error: e instanceof ApolloError ? e : new ApolloError({ errorMessage: String(e) }) };
    }
}

/** Take ownership of a response's dataset, combined with its siblings unless they are shown separately. */
function adoptDataset<T extends { dataset?: SchemaDataset | null }>(data: T, isHideSiblingMode: boolean) {
    // combineEntityDataWithSiblings walks and rebuilds the tree; give it a private copy so a
    // frozen or shared response can never be mutated. The original is dropped right after.
    const combined = isHideSiblingMode ? data : combineEntityDataWithSiblings(cloneDeep(data));
    return combined.dataset as SchemaDataset;
}

/**
 * Merge the full-metadata dataset into the structural one, field by field, producing new
 * field objects (memoised row renderers compare by reference). The structural copy becomes
 * garbage once the caller replaces its state with the result.
 */
function mergeFullIntoStructural(structural: SchemaDataset | null, full: SchemaDataset): SchemaDataset {
    if (!structural?.schemaMetadata || !full.schemaMetadata) return full;
    const structuralByPath = new Map(structural.schemaMetadata.fields.map((f) => [f.fieldPath, f] as const));
    const fields = full.schemaMetadata.fields.map((field) => {
        const base = structuralByPath.get(field.fieldPath);
        return base ? { ...base, ...field } : field;
    });
    return {
        ...structural,
        ...full,
        schemaMetadata: { ...structural.schemaMetadata, ...full.schemaMetadata, fields },
    } as SchemaDataset;
}

// structuralOnly: when true, only the lean structural query fires and the full metadata
// query is skipped. Use this for callers that only need field paths or counts (e.g. the
// tab badge) and do not need tags, glossary terms, or descriptions. Avoids a duplicate
// full-metadata network request when multiple consumers of this hook are mounted at once.
// structuralFirst: when true, `loading` reports only the structural phase so the caller can
// render rows from structuralSchemaMetadata while full metadata is still in flight (SchemaTab).
// Every other caller reads entityWithSchema, which only carries full metadata, so for them
// `loading` stays true until the full query has settled -- same contract as before the
// two-phase split.
//
// Memory model: the hook holds exactly one copy of the schema. Phase 1 (structural query)
// creates it, Phase 2 (full metadata) is merged into it and the structural version is
// released. The query responses themselves are not retained -- both queries run through
// client.query rather than useQuery, whose observables would keep their last result alive
// for the life of the tab. On a 2000-column dataset that is the difference between one
// ~20 MB payload on the heap and three or four.
export const useGetEntityWithSchema = (skip?: boolean, structuralOnly?: boolean, structuralFirst?: boolean) => {
    const { urn, entityData, entityType } = useEntityData();
    const shouldLoad = !skip && !!urn && shouldLoadSchema(entityType, entityData);
    const isHideSiblingMode = useIsSeparateSiblingsMode();
    const client = useApolloClient();

    const [state, setState] = useState<SchemaState>(EMPTY_STATE);
    const stateRef = useRef(state);
    stateRef.current = state;

    const [reload, setReload] = useState<Reload>({ token: 0, fullOnly: false });
    // refetch() promises waiting for the run they triggered to finish.
    const pendingRefetches = useRef<Array<() => void>>([]);

    useEffect(() => {
        const settlePending = () => {
            const pending = pendingRefetches.current;
            pendingRefetches.current = [];
            pending.forEach((resolve) => resolve());
        };

        if (!shouldLoad || !urn) {
            setState(EMPTY_STATE);
            settlePending();
            return undefined;
        }

        // Flipped by the cleanup when the urn (or another input) changes underneath an
        // in-flight run: late responses for the previous dataset are dropped, never merged.
        let cancelled = false;

        const run = async () => {
            const { current } = stateRef;
            const fullOnly = reload.fullOnly && current.urn === urn && current.hasFull;

            if (!fullOnly) {
                // Phase 1: field paths, types, nullability. Evicts whatever was shown before.
                setState({ ...EMPTY_STATE, urn, structuralLoading: true });
                const structural = await runSchemaQuery<GetDatasetSchemaStructuralQuery>(
                    client,
                    GetDatasetSchemaStructuralDocument,
                    urn,
                );
                if (cancelled) return;
                if (!structural.data) {
                    setState((s) => ({ ...s, structuralLoading: false, structuralError: structural.error }));
                    return;
                }
                const dataset = adoptDataset(structural.data, isHideSiblingMode);
                setState((s) => ({
                    ...s,
                    dataset,
                    hasStructural: true,
                    structuralLoading: false,
                    structuralError: undefined,
                    fullLoading: !structuralOnly,
                }));
                if (structuralOnly) return;
            } else {
                setState((s) => ({ ...s, fullLoading: true, fullError: undefined }));
            }

            // Phase 2: descriptions, tags, terms, editable metadata, merged into the copy.
            const full = await runSchemaQuery<GetDatasetSchemaQuery>(client, GetDatasetSchemaDocument, urn);
            if (cancelled) return;
            if (!full.data) {
                setState((s) => ({ ...s, fullLoading: false, fullError: full.error }));
                return;
            }
            const fullDataset = adoptDataset(full.data, isHideSiblingMode);
            setState((s) => ({
                ...s,
                dataset: mergeFullIntoStructural(s.dataset, fullDataset),
                hasFull: true,
                fullLoading: false,
                fullError: undefined,
            }));
        };

        run().finally(settlePending);
        return () => {
            cancelled = true;
        };
    }, [client, urn, shouldLoad, structuralOnly, isHideSiblingMode, reload]);

    // Resolves once the reload it triggered has settled (or been superseded), so callers
    // that await it, or Promise.all it with other refetches, behave as before.
    const refetch = useCallback(
        () =>
            new Promise<void>((resolve) => {
                pendingRefetches.current.push(resolve);
                setReload((r) => ({ token: r.token + 1, fullOnly: true }));
            }),
        [],
    );

    const isCurrent = shouldLoad && state.urn === urn;
    // True after Phase 1 resolves but before Phase 2 completes. SchemaTable shows skeleton
    // placeholders in metadata columns. Not raised again by a fullOnly reload, which keeps
    // the existing metadata on screen until the fresh copy replaces it.
    const fullMetadataLoading = isCurrent && state.hasStructural && !state.hasFull && state.fullLoading;

    // Once hasFull is set the copy has been merged from the full query, so it is a FullDataset.
    const entityWithSchema = useMemo(
        () => (isCurrent && state.hasFull ? (state.dataset as FullDataset) : entityData),
        [isCurrent, state.hasFull, state.dataset, entityData],
    );

    return {
        // structuralFirst callers: true only while the structural query is in flight
        // (SchemaTab shows a spinner, then rows with skeleton metadata cells).
        // Everyone else: true until entityWithSchema is final, i.e. the full query has
        // produced data or an error.
        loading: structuralFirst ? state.structuralLoading : state.structuralLoading || fullMetadataLoading,
        fullMetadataLoading,
        // Set when the full metadata query fails. SchemaTab shows an inline error banner.
        fullMetadataError: isCurrent ? state.fullError : undefined,
        // Set when the structural query fails. SchemaTab shows an error instead of a
        // misleading empty table.
        structuralSchemaError: isCurrent ? state.structuralError : undefined,
        // The schema with full metadata when Phase 2 has landed, otherwise the entity
        // context data. All consumers other than SchemaTab should use this -- it never
        // exposes structural-only data, so components that read tags, terms, or
        // schemaFieldEntity will not see missing fields.
        entityWithSchema,
        // The structural-only copy, exposed while Phase 2 is pending (or failed) so SchemaTab
        // can render field paths and types immediately. Null once full metadata has been
        // merged in: there is only one copy, and entityWithSchema is it.
        structuralSchemaMetadata:
            isCurrent && state.hasStructural && !state.hasFull ? (state.dataset?.schemaMetadata ?? null) : null,
        refetch,
    };
};
