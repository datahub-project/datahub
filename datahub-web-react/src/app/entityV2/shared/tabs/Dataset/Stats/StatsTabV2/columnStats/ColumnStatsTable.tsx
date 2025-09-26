import { Table, Text } from '@components';
import React, { useEffect, useMemo, useRef, useState } from 'react';
import styled from 'styled-components';

import SchemaFieldDrawer from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/SchemaFieldDrawer';
import { useGetEntityWithSchema } from '@app/entityV2/shared/tabs/Dataset/Schema/useGetEntitySchema';
import useKeyboardControls from '@app/entityV2/shared/tabs/Dataset/Schema/useKeyboardControls';
import { decimalToPercentStr } from '@app/entityV2/shared/tabs/Dataset/Schema/utils/statsUtil';
import { useGetColumnStatsColumns } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/columnStats/useGetColumnStatsColumns';
import { isPresent } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/utils';
import { downgradeV2FieldPath, groupByFieldPath } from '@src/app/entityV2/dataset/profile/schema/utils/utils';
import { DatasetFieldProfile } from '@src/types.generated';

/**
 * Determines if a field is nullable based on column statistics.
 * Uses null count or proportion data when available, defaults to nullable.
 */
function isFieldNullable(stat: DatasetFieldProfile): boolean {
    if (stat.nullCount != null) {
        return stat.nullCount > 0;
    }

    if (stat.nullProportion != null) {
        return stat.nullProportion > 0;
    }

    return true; // Default to nullable when data unavailable
}

/**
 * Creates a stats-only field object for fields that exist in column stats but not in schema.
 */
function createStatsOnlyField(stat: DatasetFieldProfile) {
    return {
        fieldPath: stat.fieldPath,
        type: null,
        nativeDataType: null,
        schemaFieldEntity: null,
        nullable: isFieldNullable(stat),
        recursive: false,
        description: null,
    };
}

/**
 * Flattens nested field hierarchies to enable drawer field path matching.
 */
function flattenFields(fieldList: any[]): any[] {
    const result: any[] = [];
    fieldList.forEach((field) => {
        result.push(field);
        if (field.children) {
            result.push(...flattenFields(field.children));
        }
    });
    return result;
}

/**
 * Handles scroll adjustment when a row is selected to ensure it's visible.
 */
function handleRowScrollIntoView(row: HTMLTableRowElement | undefined, header: HTMLTableSectionElement | null) {
    if (!row || !header) return;

    const rowRect = row.getBoundingClientRect();
    const headerRect = header.getBoundingClientRect();
    const rowTop = rowRect.top;
    const headerBottom = headerRect.bottom;
    const scrollContainer = row.closest('table')?.parentElement;

    if (scrollContainer && rowTop < headerBottom) {
        const scrollAmount = headerBottom - rowTop;
        scrollContainer.scrollTop -= scrollAmount;
    }
}

/**
 * Filters column stats data based on search query.
 */
function filterColumnStatsByQuery(data: any[], query: string) {
    if (!query.trim()) return data;

    const lowercaseQuery = query.toLowerCase();
    return data.filter((columnStat) => columnStat.column?.toLowerCase().includes(lowercaseQuery));
}

const EmptyContainer = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    height: 100%;
    width: 100%;
    height: 150px;
`;

interface Props {
    columnStats: DatasetFieldProfile[];
    searchQuery: string;
}

function ColumnStatsTable({ columnStats, searchQuery }: Props) {
    const { entityWithSchema } = useGetEntityWithSchema();
    const fields = entityWithSchema?.schemaMetadata?.fields;

    const columnStatsTableData = useMemo(
        () =>
            columnStats.map((stat) => ({
                column: downgradeV2FieldPath(stat.fieldPath),
                originalFieldPath: stat.fieldPath,
                type: (fields as any[])?.find((field) => field.fieldPath === stat.fieldPath)?.type,
                nullPercentage: isPresent(stat.nullProportion) && decimalToPercentStr(stat.nullProportion, 2),
                uniqueValues: isPresent(stat.uniqueCount) && stat.uniqueCount.toString(),
                min: stat.min,
                max: stat.max,
            })) || [],
        [columnStats, fields],
    );

    const [expandedDrawerFieldPath, setExpandedDrawerFieldPath] = useState<string | null>(null);

    const rows = useMemo(() => {
        const schemaFields = fields || [];

        // Add fields from column stats that don't exist in schema
        const statsOnlyFields = columnStats
            .filter((stat) => !(schemaFields as any[]).find((field) => field.fieldPath === stat.fieldPath))
            .map(createStatsOnlyField);

        const combinedFields = [...schemaFields, ...statsOnlyFields];
        const groupedFields = groupByFieldPath(combinedFields as any[]);

        return flattenFields(groupedFields);
    }, [fields, columnStats]);

    const filteredData = filterColumnStatsByQuery(columnStatsTableData, searchQuery);

    const columnStatsColumns = useGetColumnStatsColumns({
        tableData: columnStatsTableData,
        searchQuery,
        setExpandedDrawerFieldPath,
    });

    const { selectPreviousField, selectNextField } = useKeyboardControls(
        rows,
        expandedDrawerFieldPath,
        setExpandedDrawerFieldPath,
    );

    const rowRefs = useRef<HTMLTableRowElement[]>([]);
    const headerRef = useRef<HTMLTableSectionElement | null>(null);

    useEffect(() => {
        if (expandedDrawerFieldPath) {
            const selectedIndex = rows.findIndex((row) => row.fieldPath === expandedDrawerFieldPath);
            const row = rowRefs.current[selectedIndex];
            const header = headerRef.current;

            // To bring the selected row into view
            if (selectedIndex !== -1 && row) {
                row.scrollIntoView({
                    behavior: 'smooth',
                    block: 'nearest',
                });
            }
            // Adjust scroll position to account for fixed header
            setTimeout(() => {
                handleRowScrollIntoView(row, header);
            }, 100);
        }
    }, [expandedDrawerFieldPath, rows]);

    if (filteredData.length === 0) {
        return (
            <EmptyContainer>
                <Text color="gray" weight="bold">
                    No search results!
                </Text>
            </EmptyContainer>
        );
    }

    const getRowClassName = (record) => {
        return expandedDrawerFieldPath === record.originalFieldPath ? 'selected-row' : '';
    };

    const onRowClick = (record) => {
        setExpandedDrawerFieldPath(
            expandedDrawerFieldPath === record.originalFieldPath ? null : record.originalFieldPath,
        );
    };

    return (
        <>
            <Table
                columns={columnStatsColumns}
                data={filteredData}
                isScrollable
                maxHeight="475px"
                onRowClick={onRowClick}
                rowClassName={getRowClassName}
                rowRefs={rowRefs}
                headerRef={headerRef}
            />
            {!!fields && (
                <SchemaFieldDrawer
                    schemaFields={fields as any[]}
                    expandedDrawerFieldPath={expandedDrawerFieldPath}
                    editableSchemaMetadata={entityWithSchema?.editableSchemaMetadata as any}
                    setExpandedDrawerFieldPath={setExpandedDrawerFieldPath}
                    displayedRows={rows}
                    defaultSelectedTabName="Statistics"
                    selectPreviousField={selectPreviousField}
                    selectNextField={selectNextField}
                />
            )}
        </>
    );
}

export default ColumnStatsTable;
