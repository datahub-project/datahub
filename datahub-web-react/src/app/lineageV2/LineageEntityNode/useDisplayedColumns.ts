import { useContext, useMemo } from 'react';
import { SchemaFieldDataType } from '../../../types.generated';
import { ColumnHighlight, createColumnRef, FineGrainedLineage, LineageDisplayContext } from '../common';
import { NUM_COLUMNS_PER_PAGE } from '../constants';
import { ColumnAsset, FetchedEntityV2, LineageAssetType } from '../types';

export const LINEAGE_NODE_WIDTH = 200;
export const LINEAGE_NODE_HEIGHT = 70;
export const TRANSITION_DURATION_MS = 200;

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

interface Return {
    paginatedColumns: LineageDisplayColumn[];
    extraHighlightedColumns: LineageDisplayColumn[];
    numFilteredColumns: number;
    numColumnsWithLineage: number;
    numColumnsTotal: number;
}

export default function useDisplayedColumns({
    urn,
    entity,
    showAllColumns,
    pageIndex,
    filterText,
    onlyWithLineage,
}: Arguments): Return {
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
