import { Table, Text } from '@components';
import React, { useEffect, useMemo, useRef, useState } from 'react';
import styled from 'styled-components';

import { ExtendedSchemaFields } from '@app/entityV2/dataset/profile/schema/utils/types';
import SchemaFieldDrawer from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/SchemaFieldDrawer';
import { useGetEntityWithSchema } from '@app/entityV2/shared/tabs/Dataset/Schema/useGetEntitySchema';
import useKeyboardControls from '@app/entityV2/shared/tabs/Dataset/Schema/useKeyboardControls';
import { decimalToPercentStr } from '@app/entityV2/shared/tabs/Dataset/Schema/utils/statsUtil';
import {
    createStatsOnlyField,
    filterColumnStatsByQuery,
    flattenFields,
    handleRowScrollIntoView,
    mapToSchemaFields,
} from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/columnStats/ColumnStatsTable.utils';
import { useGetColumnStatsColumns } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/columnStats/useGetColumnStatsColumns';
import { isPresent } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/utils';
import { downgradeV2FieldPath, groupByFieldPath } from '@src/app/entityV2/dataset/profile/schema/utils/utils';

// Local type definitions since generated types aren't available
interface DatasetFieldProfile {
    fieldPath: string;
    nullCount?: number | null;
    nullProportion?: number | null;
    uniqueCount?: number | null;
    min?: string | null;
    max?: string | null;
}

// Extended type that includes the fieldPath property we know exists
interface ExtendedSchemaFieldsWithFieldPath extends ExtendedSchemaFields {
    fieldPath: string;
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
    const rawFields = entityWithSchema?.schemaMetadata?.fields;

    const fields = useMemo(() => {
        return rawFields ? mapToSchemaFields(rawFields) : [];
    }, [rawFields]);

    const columnStatsTableData = useMemo(
        () =>
            columnStats.map((stat) => ({
                column: downgradeV2FieldPath(stat.fieldPath),
                originalFieldPath: stat.fieldPath,
                type: fields.find((field) => field.fieldPath === stat.fieldPath)?.type,
                nullPercentage: isPresent(stat.nullProportion) && decimalToPercentStr(stat.nullProportion, 2),
                uniqueValues: isPresent(stat.uniqueCount) && stat.uniqueCount.toString(),
                min: stat.min,
                max: stat.max,
            })) || [],
        [columnStats, fields],
    );

    const [expandedDrawerFieldPath, setExpandedDrawerFieldPath] = useState<string | null>(null);

    const rows = useMemo(() => {
        const schemaFields = fields;

        // Add fields from column stats that don't exist in schema
        const statsOnlyFields = columnStats
            .filter((stat) => !schemaFields.find((field) => field.fieldPath === stat.fieldPath))
            .map(createStatsOnlyField);

        const combinedFields = [...schemaFields, ...statsOnlyFields];
        const groupedFields = groupByFieldPath(combinedFields as any);

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
            const selectedIndex = rows.findIndex(
                (row) => (row as ExtendedSchemaFieldsWithFieldPath).fieldPath === expandedDrawerFieldPath,
            );
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
            {fields.length > 0 && (
                <SchemaFieldDrawer
                    schemaFields={fields as any}
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
