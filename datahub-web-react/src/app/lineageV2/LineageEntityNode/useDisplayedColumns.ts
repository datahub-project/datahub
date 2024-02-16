import { useContext, useMemo, useRef } from 'react';
import { SchemaFieldDataType } from '../../../types.generated';
import { ColumnHighlight, createColumnRef, FineGrainedLineage, LineageDisplayContext } from '../common';
import { NUM_COLUMNS_PER_PAGE } from '../constants';
import { ColumnAsset, FetchedEntityV2, LineageAssetType } from '../types';

export const LINEAGE_NODE_WIDTH = 200;
export const LINEAGE_NODE_HEIGHT = 70;

type FieldPath = string;

export interface LineageDisplayColumn {
    fieldPath: FieldPath;
    highlighted: boolean;
    fromSelect?: boolean;
    type?: SchemaFieldDataType;
    nativeDataType?: string | null;
}

interface Arguments {
    urn: string;
    entity?: FetchedEntityV2;
    showAllColumns: boolean;
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
    const oldVals = useRef<DisplayedColumns>({
        paginatedColumns: [],
        extraHighlightedColumns: [],
        numFilteredColumns: 0,
        numColumnsWithLineage: 0,
        numColumnsTotal: 0,
    });
    const vals = useComputeValues(args);
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
    showAllColumns,
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

        const fieldMap = new Map<string, ColumnAsset>();
        entity.lineageAssets?.forEach((asset) => {
            if (asset.type === LineageAssetType.Column) {
                fieldMap.set(asset.name, asset);
            }
        });
        const columnHighlights = highlightedColumns.get(urn) || new Map<string, ColumnHighlight>();

        function makeLineageDisplayColumn(fieldPath: string): LineageDisplayColumn {
            return {
                fieldPath,
                highlighted: columnHighlights.has(fieldPath),
                fromSelect: columnHighlights.get(fieldPath)?.fromSelect,
                type: fieldMap.get(fieldPath)?.dataType,
                nativeDataType: fieldMap.get(fieldPath)?.nativeDataType,
            };
        }

        const fields = entity.lineageAssets?.map((asset) => asset.name) || [];
        const withLineageFields = filterColumnsByLineage(fields, urn, fineGrainedLineage);
        const filteredFields = filterColumnsByText(onlyWithLineage ? withLineageFields : fields, filterText);
        const paginatedFields = filteredFields.slice(
            pageIndex * NUM_COLUMNS_PER_PAGE,
            pageIndex * NUM_COLUMNS_PER_PAGE + NUM_COLUMNS_PER_PAGE,
        );
        const missingHighlightedFields = Array.from(columnHighlights)
            .sort(
                // Highlights from selection before highlights from hover
                ([_fieldA, detailsA], [_fieldB, detailsB]) => Number(detailsB.fromSelect) - Number(detailsA.fromSelect),
            )
            .filter(([field]) => !showAllColumns || !paginatedFields.includes(field));

        return {
            paginatedColumns: paginatedFields.map(makeLineageDisplayColumn),
            extraHighlightedColumns: missingHighlightedFields.map(([field]) => field).map(makeLineageDisplayColumn),
            numFilteredColumns: filteredFields.length,
            numColumnsTotal: fields.length,
            numColumnsWithLineage: withLineageFields.length,
        };
    }, [urn, entity, showAllColumns, pageIndex, filterText, highlightedColumns, onlyWithLineage, fineGrainedLineage]);
}

function filterColumnsByText(fields: FieldPath[], filterText: string): FieldPath[] {
    const formattedFilterText = filterText.toLocaleLowerCase();
    return fields?.filter((field) => field.toLocaleLowerCase().includes(formattedFilterText));
}

function filterColumnsByLineage(fields: FieldPath[], urn: string, fineGrainedLineage: FineGrainedLineage): FieldPath[] {
    return fields.filter((fieldPath) => {
        const columnRef = createColumnRef(urn, fieldPath);
        return fineGrainedLineage.forward.has(columnRef) || fineGrainedLineage.backward.has(columnRef);
    });
}
