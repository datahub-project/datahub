import { useContext, useMemo, useRef } from 'react';

import {
    FineGrainedLineage,
    LineageDisplayContext,
    ShownRelatedColumns,
    ShownRelatedCounts,
    createColumnRef,
} from '@app/lineageV3/common';
import { NUM_COLUMNS_PER_PAGE } from '@app/lineageV3/constants';
import { ColumnAsset, FetchedEntityV2, LineageAssetType } from '@app/lineageV3/types';

import { LineageDirection, SchemaFieldDataType } from '@types';

type FieldPath = string;

export interface LineageDisplayColumn {
    fieldPath: FieldPath;
    highlighted: boolean;
    hasLineage: boolean;
    connectedToHomeNode: boolean;
    type?: SchemaFieldDataType;
    nativeDataType?: string | null;
    lineageAsset: ColumnAsset;
    /** Undefined unless the column is part of the current column lineage traversal. */
    shownRelated?: ShownRelatedCounts;
}

interface Arguments {
    urn: string;
    entity?: FetchedEntityV2;
    showColumns: boolean;
    pageIndex: number;
    filterText: string;
    onlyWithLineage: boolean;
}

export interface DisplayedColumns {
    paginatedColumns: LineageDisplayColumn[];
    extraHighlightedColumns: LineageDisplayColumn[];
    numFilteredColumns: number;
    numColumnsWithLineage: number;
    numColumnsTotal: number;
}

interface NormalizedReturn {
    plainColumns: string[];
    highlightedColumns: string[];
    extraHighlightedColumns: string[];
    numFilteredColumns: number;
    numColumnsWithLineage: number;
    numColumnsTotal: number;
}

/**
 * Whether a column has any lineage: counts fetched for it, or column lineage already on the graph.
 * `lineageAsset` is written to in place as counts resolve, so compute this where it is read rather
 * than passing the result around, which goes stale.
 */
export function columnHasLineage(lineageAsset: ColumnAsset, connectedToHomeNode: boolean): boolean {
    return !!lineageAsset.numUpstream || !!lineageAsset.numDownstream || connectedToHomeNode;
}

/** Identifies a column by everything about it that is rendered, so that changed counts re-render. */
function describe(column: LineageDisplayColumn): string {
    const { shownRelated, lineageAsset } = column;
    return [
        column.fieldPath,
        shownRelated?.[LineageDirection.Upstream],
        shownRelated?.[LineageDirection.Downstream],
        // Written onto the asset in place as counts resolve, so they are not visible as a change
        // to any of the values this memo is computed from
        lineageAsset.numUpstream,
        lineageAsset.numDownstream,
    ].join('␟');
}

export default function useDisplayedColumns(args: Arguments): DisplayedColumns {
    // Prevent unnecessary NodeContents rerenders
    const oldVals = useRef<DisplayedColumns>({
        paginatedColumns: [],
        extraHighlightedColumns: [],
        numFilteredColumns: 0,
        numColumnsWithLineage: 0,
        numColumnsTotal: 0,
    });
    const vals = useComputeValues(args);
    // TODO: Consider using comparison method instead
    if (JSON.stringify(normalize(oldVals.current)) !== JSON.stringify(normalize(vals))) {
        oldVals.current = vals;
    }
    return oldVals.current;
}

function normalize(val: DisplayedColumns): NormalizedReturn {
    return {
        plainColumns: val.paginatedColumns.filter((col) => !col.highlighted).map(describe),
        highlightedColumns: val.paginatedColumns.filter((col) => col.highlighted).map(describe),
        extraHighlightedColumns: val.extraHighlightedColumns.map(describe),
        numFilteredColumns: val.numFilteredColumns,
        numColumnsWithLineage: val.numColumnsWithLineage,
        numColumnsTotal: val.numColumnsTotal,
    };
}

function useComputeValues({
    urn,
    entity,
    showColumns,
    pageIndex,
    filterText,
    onlyWithLineage,
}: Arguments): DisplayedColumns {
    const { highlightedColumns, shownRelatedColumns, fineGrainedLineage } = useContext(LineageDisplayContext);

    return useMemo(() => {
        if (!entity) {
            return {
                paginatedColumns: [],
                extraHighlightedColumns: [],
                numFilteredColumns: 0,
                numColumnsWithLineage: 0,
                numColumnsTotal: 0,
            };
        }

        const columnHighlights = highlightedColumns.get(urn) || new Set<string>();
        const columns = getDisplayColumns(urn, entity, columnHighlights, shownRelatedColumns, fineGrainedLineage);

        const columnsWithLineage = columns.filter((field) => field.hasLineage);
        const filteredColumns = filterColumnsByText(onlyWithLineage ? columnsWithLineage : columns, filterText);
        const paginatedColumns = filteredColumns.slice(
            pageIndex * NUM_COLUMNS_PER_PAGE,
            pageIndex * NUM_COLUMNS_PER_PAGE + NUM_COLUMNS_PER_PAGE,
        );
        const paginatedFields = new Set(paginatedColumns.map((column) => column.fieldPath));
        const extraHighlightedColumns = columns.filter(
            (column) =>
                columnHighlights.has(column.fieldPath) && (!showColumns || !paginatedFields.has(column.fieldPath)),
        );
        return {
            paginatedColumns,
            extraHighlightedColumns,
            numFilteredColumns: filteredColumns.length,
            numColumnsTotal: columns.length,
            numColumnsWithLineage: columnsWithLineage.length,
        };
    }, [
        urn,
        entity,
        showColumns,
        pageIndex,
        filterText,
        highlightedColumns,
        shownRelatedColumns,
        onlyWithLineage,
        fineGrainedLineage,
    ]);
}

function getDisplayColumns(
    urn: string,
    entity: FetchedEntityV2,
    columnHighlights: Set<string>,
    shownRelatedColumns: ShownRelatedColumns,
    fineGrainedLineage: FineGrainedLineage,
): LineageDisplayColumn[] {
    if (!entity.lineageAssets) return [];

    return Array.from(entity.lineageAssets.values())
        .filter((asset): asset is ColumnAsset => asset.type === LineageAssetType.Column)
        .map((columnAsset) => {
            const columnRef = createColumnRef(urn, columnAsset.name);
            const connectedToHomeNode =
                fineGrainedLineage.upstream.has(columnRef) || fineGrainedLineage.downstream.has(columnRef);

            return {
                fieldPath: columnAsset.name,
                type: columnAsset.dataType,
                nativeDataType: columnAsset.nativeDataType,
                highlighted: columnHighlights.has(columnAsset.name),
                hasLineage: columnHasLineage(columnAsset, connectedToHomeNode),
                connectedToHomeNode,
                lineageAsset: columnAsset,
                shownRelated: shownRelatedColumns.get(columnRef),
            };
        });
}

function filterColumnsByText(fields: LineageDisplayColumn[], filterText: string): LineageDisplayColumn[] {
    const formattedFilterText = filterText.toLocaleLowerCase();
    return fields?.filter((field) => field.fieldPath.toLocaleLowerCase().includes(formattedFilterText));
}
