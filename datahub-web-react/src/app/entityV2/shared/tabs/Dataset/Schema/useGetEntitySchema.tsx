import { cloneDeep } from 'lodash';
import { useCallback, useMemo } from 'react';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { combineEntityDataWithSiblings } from '@app/entity/shared/siblingUtils';
import { useIsSeparateSiblingsMode } from '@app/entityV2/shared/useIsSeparateSiblingsMode';

import { useGetDatasetSchemaQuery, useGetDatasetSchemaStructuralQuery } from '@graphql/dataset.generated';
import { EntityType } from '@types';

// Whether to dynamically load the schema from the backend.
const shouldLoadSchema = (entityType, entityData) => {
    return entityType === EntityType.Dataset && !entityData?.schemaMetadata;
};

// structuralOnly: when true, only the lean structural query fires and the full metadata
// query is skipped. Use this for callers that only need field paths or counts (e.g. the
// tab badge) and do not need tags, glossary terms, or descriptions. Avoids a duplicate
// full-metadata network request when multiple consumers of this hook are mounted at once.
// structuralFirst: when true, `loading` reports only the structural phase so the caller can
// render rows from structuralSchemaMetadata while full metadata is still in flight (SchemaTab).
// Every other caller reads entityWithSchema, which only carries full metadata, so for them
// `loading` stays true until the full query has settled -- same contract as before the
// two-phase split.
export const useGetEntityWithSchema = (skip?: boolean, structuralOnly?: boolean, structuralFirst?: boolean) => {
    const { urn, entityData, entityType } = useEntityData();
    const shouldLoad = !skip && !!urn && shouldLoadSchema(entityType, entityData);
    const isHideSiblingMode = useIsSeparateSiblingsMode();

    // Structural query: lean fetch of field paths, types, and nullability only.
    // Fires immediately so the table has rows to render before the heavier full
    // metadata query completes.
    // no-cache: structural data is temporary -- once the full metadata query arrives
    // it is superseded. Keeping it out of the Apollo cache avoids holding two large
    // payloads in memory simultaneously, which was crashing the tab on large datasets.
    const {
        data: structuralData,
        loading: structuralLoading,
        error: structuralError,
        refetch: refetchStructural,
    } = useGetDatasetSchemaStructuralQuery({
        variables: { urn },
        skip: !shouldLoad,
        fetchPolicy: 'no-cache',
        errorPolicy: 'all',
    });

    // Gate the full metadata query on the structural result itself rather than on
    // !structuralLoading: on the very first render Apollo initialises loading=false
    // synchronously before the network tick, so !structuralLoading would be true
    // immediately and both queries would fire at once, defeating the sequencing.
    // The result must name the current urn: after navigation Apollo can still hand back
    // the previous dataset's data for a render, and treating that as "loaded" would start
    // the full query against the NEW urn before its own structural query has completed.
    // Derived synchronously (no state + effect) so there is no frame in which structural
    // data is visible but fullMetadataLoading has not yet flipped to true.
    const structuralDataLoaded = !!urn && !structuralLoading && structuralData?.dataset?.urn === urn;

    // Full metadata query: tags, glossary terms, descriptions, editable metadata.
    // Skipped until the structural query has delivered data so the table is visible first.
    // no-cache: cache-first triggered Apollo's normalization walk over 2220 deeply-nested
    // field objects, spiking the heap before GC could run and crashing the tab.
    // errorPolicy:'all' returns partial data instead of throwing to an error boundary;
    // failures surface via fullMetadataError so SchemaTab can warn inline.
    const {
        data: fullData,
        loading: fullLoading,
        error: fullError,
        refetch: refetchFull,
    } = useGetDatasetSchemaQuery({
        variables: { urn },
        skip: !shouldLoad || !structuralDataLoaded || !!structuralOnly,
        fetchPolicy: 'no-cache',
        errorPolicy: 'all',
    });

    const mergedStructuralData = useMemo(
        () =>
            structuralData && !isHideSiblingMode
                ? combineEntityDataWithSiblings(cloneDeep(structuralData))
                : structuralData,
        [structuralData, isHideSiblingMode],
    );

    const mergedFullData = useMemo(
        () => (fullData && !isHideSiblingMode ? combineEntityDataWithSiblings(cloneDeep(fullData)) : fullData),
        [fullData, isHideSiblingMode],
    );

    // Chain refetches sequentially so the full metadata query starts only after the
    // structural query completes, matching the initial-load sequencing.
    // Guard on structuralDataLoaded: calling refetch on a query that was mounted with
    // skip:true and has never fired throws an Apollo error.
    const refetch = useCallback(async () => {
        await refetchStructural();
        if (shouldLoad && structuralDataLoaded && !structuralOnly) await refetchFull();
    }, [refetchStructural, refetchFull, shouldLoad, structuralDataLoaded, structuralOnly]);

    // True after Phase 1 resolves but before Phase 2 (tags/terms/descriptions)
    // completes. SchemaTable shows skeleton placeholders in metadata columns.
    // Includes the render gap between Phase 1 resolving and Phase 2 mounting
    // (fullLoading is still false there): pending until the full query has
    // actually produced data or an error, so SchemaTab never sees a spurious
    // "metadata done" frame and clears valid metadata filters.
    const fullMetadataLoading =
        shouldLoad && !structuralOnly && structuralDataLoaded && !fullData && !fullError
            ? true
            : structuralDataLoaded && fullLoading;

    return {
        // structuralFirst callers: true only while the structural query is in flight
        // (SchemaTab shows a spinner, then rows with skeleton metadata cells).
        // Everyone else: true until entityWithSchema is final, i.e. the full query has
        // produced data or an error.
        loading: structuralFirst ? structuralLoading : structuralLoading || fullMetadataLoading,
        fullMetadataLoading,
        // Set when the full metadata query fails. SchemaTab shows an inline error banner.
        fullMetadataError: fullError,
        // Set when the structural query fails. SchemaTab shows an error instead of a
        // misleading empty table.
        structuralSchemaError: structuralError,
        // Full entity data: full metadata query result when ready, otherwise falls back
        // to entity context data. All consumers other than SchemaTab should use this --
        // it never exposes structural-only data, so components that read tags, terms, or
        // schemaFieldEntity will not see missing fields.
        entityWithSchema: shouldLoad ? (mergedFullData?.dataset ?? entityData) : entityData,
        // Structural schema metadata, exposed separately so SchemaTab can render field
        // paths and types immediately while the full metadata query is still in-flight.
        structuralSchemaMetadata: mergedStructuralData?.dataset?.schemaMetadata ?? null,
        refetch,
    };
};
