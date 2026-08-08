import { cloneDeep } from 'lodash';
import { useCallback, useEffect, useMemo, useState } from 'react';

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
export const useGetEntityWithSchema = (skip?: boolean, structuralOnly?: boolean) => {
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

    // Gate the full metadata query on this flag rather than on !structuralLoading.
    // On the very first render, Apollo initialises loading=false synchronously before
    // the network tick, so !structuralLoading would be true immediately and both
    // queries would fire at the same time, defeating the intended sequencing.
    const [structuralDataLoaded, setStructuralDataLoaded] = useState(false);

    // Reset when the entity changes so the full metadata query does not fire immediately
    // on navigation -- the new dataset's structural query must complete first.
    useEffect(() => {
        setStructuralDataLoaded(false);
    }, [urn]);

    useEffect(() => {
        if (!structuralLoading && structuralData) {
            setStructuralDataLoaded(true);
        }
    }, [structuralLoading, structuralData]);

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

    return {
        // True while the structural query is in-flight. SchemaTab shows a spinner.
        loading: structuralLoading,
        // True after Phase 1 resolves but before Phase 2 (tags/terms/descriptions)
        // completes. SchemaTable shows skeleton placeholders in metadata columns.
        fullMetadataLoading: structuralDataLoaded && fullLoading,
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
