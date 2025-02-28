import { useContext, useMemo, useRef } from 'react';
import { SchemaFieldDataType } from '@types';
import { createColumnRef, FineGrainedLineage, LineageDisplayContext } from '../common';
import { NUM_COLUMNS_PER_PAGE } from '../constants';
import { ColumnAsset, FetchedEntityV2, LineageAssetType } from '../types';

export const LINEAGE_NODE_WIDTH = 240;
export const LINEAGE_NODE_HEIGHT = 80;

type FieldPath = string;

export interface LineageDisplayColumn {
    fieldPath: FieldPath;
    highlighted: boolean;
    hasLineage: boolean;
    connectedToHomeNode: boolean;
    type?: SchemaFieldDataType;
    nativeDataType?: string | null;
    lineageAsset: ColumnAsset;
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
        plainColumns: val.paginatedColumns.filter((col) => !col.highlighted).map((col) => col.fieldPath),
        highlightedColumns: val.paginatedColumns.filter((col) => col.highlighted).map((col) => col.fieldPath),
        extraHighlightedColumns: val.extraHighlightedColumns.map((col) => col.fieldPath),
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
    const { highlightedColumns, fineGrainedLineage } = useContext(LineageDisplayContext);

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
        const columns = getDisplayColumns(urn, entity, columnHighlights, fineGrainedLineage);

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
    }, [urn, entity, showColumns, pageIndex, filterText, highlightedColumns, onlyWithLineage, fineGrainedLineage]);
}

function getDisplayColumns(
    urn: string,
    entity: FetchedEntityV2,
    columnHighlights: Set<string>,
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
                hasLineage: !!columnAsset.numUpstream || !!columnAsset.numDownstream || connectedToHomeNode,
                connectedToHomeNode,
                lineageAsset: columnAsset,
            };
        });
}

function filterColumnsByText(fields: LineageDisplayColumn[], filterText: string): LineageDisplayColumn[] {
    const formattedFilterText = filterText.toLocaleLowerCase();
    return fields?.filter((field) => field.fieldPath.toLocaleLowerCase().includes(formattedFilterText));
}
