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
    columnStats: Array<DatasetFieldProfile>;
    searchQuery: string;
}

const ColumnStatsTable = ({ columnStats, searchQuery }: Props) => {
    const { entityWithSchema } = useGetEntityWithSchema();
    const schemaMetadata: any = entityWithSchema?.schemaMetadata || undefined;
    const editableSchemaMetadata: any = entityWithSchema?.editableSchemaMetadata || undefined;
    const fields = schemaMetadata?.fields;

    const columnStatsTableData = useMemo(
        () =>
            columnStats.map((doc) => ({
                column: downgradeV2FieldPath(doc.fieldPath),
                type: fields?.find((field) => field.fieldPath === doc.fieldPath)?.type,
                nullPercentage: isPresent(doc.nullProportion) && decimalToPercentStr(doc.nullProportion, 2),
                uniqueValues: isPresent(doc.uniqueCount) && doc.uniqueCount.toString(),
                min: doc.min,
                max: doc.max,
            })) || [],
        [columnStats, fields],
    );

    const [expandedDrawerFieldPath, setExpandedDrawerFieldPath] = useState<string | null>(null);

    const rows = useMemo(() => {
        return groupByFieldPath(fields);
    }, [fields]);

    const filteredData = columnStatsTableData.filter((columnStat) =>
        columnStat.column?.toLowerCase().includes(searchQuery.toLowerCase()),
    );

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
            // To bring the row hidden behind the fixed header into view fully
            setTimeout(() => {
                if (row && header) {
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
        return expandedDrawerFieldPath === record.column ? 'selected-row' : '';
    };

    const onRowClick = (record) => {
        setExpandedDrawerFieldPath(expandedDrawerFieldPath === record.column ? null : record.column);
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
                    schemaFields={fields}
                    expandedDrawerFieldPath={expandedDrawerFieldPath}
                    editableSchemaMetadata={editableSchemaMetadata}
                    setExpandedDrawerFieldPath={setExpandedDrawerFieldPath}
                    displayedRows={rows}
                    defaultSelectedTabName="Statistics"
                    selectPreviousField={selectPreviousField}
                    selectNextField={selectNextField}
                />
            )}
        </>
    );
};

export default ColumnStatsTable;
