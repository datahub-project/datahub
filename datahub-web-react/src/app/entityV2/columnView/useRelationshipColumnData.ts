import { useApolloClient } from '@apollo/client';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';

import { ColumnLike, displayFor, isGraphColumn } from '@app/entityV2/columnView/columnKinds';
import { useAppConfig } from '@app/useAppConfig';

import { GetColumnViewRelationshipsDocument } from '@graphql/columnViewRelationships.generated';
import { DataHubColumnViewColumnType, EntityType } from '@types';

export interface RelatedItem {
    urn: string;
    entityType: EntityType;
    fieldPath?: string | null;
    datasetUrn?: string | null;
    datasetName?: string | null;
    platformUrn?: string | null;
}

export interface RelationshipCell {
    total: number;
    totalIsCapped: boolean;
    related: RelatedItem[];
    /** Items requested when this cell was fetched; `related.length < total` means more exist. */
    count: number;
}

/** kind -> fieldUrn -> cell */
export type RelationshipColumnData = Map<DataHubColumnViewColumnType, Map<string, RelationshipCell>>;

const DEBOUNCE_MS = 150;
const MAX_URNS_PER_CALL = 100;
const FALLBACK_LIMIT = 5;

export interface UseRelationshipColumnDataOptions {
    /** Saved view the active definition came from; lets the server use the column's own display.maxItems. */
    columnViewUrn?: string;
}

/**
 * Viewport-driven fetching of relationship (GRAPH) column data, under the caller's context.
 *
 * Rows report visibility (useVisibleRowUrns); newly visible field urns that are not yet cached
 * are debounced into ONE `columnViewRelationships` call per GRAPH column. Results live in a Map
 * keyed by (kind, fieldUrn) for the life of the table, so scrolling back never refetches.
 * `loadMore` re-requests one cell with a larger count (capped by the server) for the popover.
 */
export function useRelationshipColumnData(
    visibleFieldUrns: string[],
    graphColumns: ColumnLike[],
    { columnViewUrn }: UseRelationshipColumnDataOptions = {},
): { data: RelationshipColumnData; loading: boolean; loadMore: (kind: DataHubColumnViewColumnType, fieldUrn: string, count: number) => Promise<RelationshipCell | undefined>; limit: number } {
    const client = useApolloClient();
    const { config } = useAppConfig();
    const limit = (config as any)?.columnViewsConfig?.relationshipPreviewLimit ?? FALLBACK_LIMIT;

    const cacheRef = useRef<RelationshipColumnData>(new Map());
    const inflightRef = useRef<Map<DataHubColumnViewColumnType, Set<string>>>(new Map());
    const [version, setVersion] = useState(0);
    const [inflightCount, setInflightCount] = useState(0);

    const columns = useMemo(() => graphColumns.filter(isGraphColumn), [graphColumns]);
    // Changing a column's maxItems invalidates its cache (different page size).
    const columnsKey = columns.map((c) => `${c.type}:${displayFor(c).maxItems ?? ''}`).join('|');
    const lastColumnsKey = useRef(columnsKey);
    if (lastColumnsKey.current !== columnsKey) {
        lastColumnsKey.current = columnsKey;
        cacheRef.current = new Map();
        inflightRef.current = new Map();
    }

    const fetchCells = useCallback(
        async (column: ColumnLike, fieldUrns: string[], count: number): Promise<Map<string, RelationshipCell>> => {
            const kind = column.type;
            const result = new Map<string, RelationshipCell>();
            for (let i = 0; i < fieldUrns.length; i += MAX_URNS_PER_CALL) {
                const chunk = fieldUrns.slice(i, i + MAX_URNS_PER_CALL);
                // eslint-disable-next-line no-await-in-loop
                const { data } = await client.query({
                    query: GetColumnViewRelationshipsDocument,
                    variables: { input: { fieldUrns: chunk, column: kind, count, columnViewUrn } },
                    fetchPolicy: 'no-cache',
                });
                (data?.columnViewRelationships || []).forEach((p: any) => {
                    result.set(p.fieldUrn, {
                        total: Number(p.total ?? 0),
                        totalIsCapped: Boolean(p.totalIsCapped),
                        related: p.related || [],
                        count,
                    });
                });
                // Fields the server omitted have nothing related.
                chunk.forEach((u) => {
                    if (!result.has(u)) result.set(u, { total: 0, totalIsCapped: false, related: [], count });
                });
            }
            return result;
        },
        [client, columnViewUrn],
    );

    useEffect(() => {
        if (columns.length === 0 || visibleFieldUrns.length === 0) return undefined;
        const timer = setTimeout(() => {
            columns.forEach((column) => {
                const kind = column.type;
                const cached = cacheRef.current.get(kind) ?? new Map<string, RelationshipCell>();
                cacheRef.current.set(kind, cached);
                const inflight = inflightRef.current.get(kind) ?? new Set<string>();
                inflightRef.current.set(kind, inflight);
                const missing = visibleFieldUrns.filter((u) => !cached.has(u) && !inflight.has(u));
                if (missing.length === 0) return;
                missing.forEach((u) => inflight.add(u));
                const count = Math.min(displayFor(column).maxItems ?? limit, limit);
                setInflightCount((n) => n + 1);
                fetchCells(column, missing, count)
                    .then((cells) => cells.forEach((cell, urn) => cached.set(urn, cell)))
                    .catch((e) => console.error('columnViewRelationships failed', e))
                    .finally(() => {
                        missing.forEach((u) => inflight.delete(u));
                        setInflightCount((n) => n - 1);
                        setVersion((v) => v + 1);
                    });
            });
        }, DEBOUNCE_MS);
        return () => clearTimeout(timer);
    }, [visibleFieldUrns, columns, columnsKey, fetchCells, limit]);

    const loadMore = useCallback(
        async (kind: DataHubColumnViewColumnType, fieldUrn: string, count: number) => {
            const column = columns.find((c) => c.type === kind);
            if (!column) return undefined;
            const cells = await fetchCells(column, [fieldUrn], Math.min(count, limit));
            const cell = cells.get(fieldUrn);
            if (cell) {
                const cached = cacheRef.current.get(kind) ?? new Map<string, RelationshipCell>();
                cached.set(fieldUrn, cell);
                cacheRef.current.set(kind, cached);
                setVersion((v) => v + 1);
            }
            return cell;
        },
        [columns, fetchCells, limit],
    );

    // `version` is the only thing that changes identity; the Map itself is mutated in place.
    // eslint-disable-next-line react-hooks/exhaustive-deps
    const data = useMemo(() => cacheRef.current, [version, columnsKey]);
    return { data, loading: inflightCount > 0, loadMore, limit };
}
