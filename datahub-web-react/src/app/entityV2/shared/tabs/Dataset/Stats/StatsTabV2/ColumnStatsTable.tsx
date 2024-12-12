import { Table, Text } from '@components';
import { groupByFieldPath } from '@src/app/entityV2/dataset/profile/schema/utils/utils';
import { DatasetFieldProfile } from '@src/types.generated';
import React, { useMemo, useState } from 'react';
import styled from 'styled-components';
import SchemaFieldDrawer from '../../Schema/components/SchemaFieldDrawer/SchemaFieldDrawer';
import { useGetEntityWithSchema } from '../../Schema/useGetEntitySchema';
import { decimalToPercentStr } from '../../Schema/utils/statsUtil';
import { useGetColumnStatsColumns } from './useGetColumnStatsColumns';
import { isPresent } from './utils';

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
                column: doc.fieldPath,
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
        columnStat.column.toLowerCase().includes(searchQuery.toLowerCase()),
    );

    const columnStatsColumns = useGetColumnStatsColumns({
        tableData: columnStatsTableData,
        searchQuery,
        setExpandedDrawerFieldPath,
    });

    if (filteredData.length === 0) {
        return (
            <EmptyContainer>
                <Text color="gray" weight="bold">
                    No search results!
                </Text>
            </EmptyContainer>
        );
    }

    return (
        <>
            <Table columns={columnStatsColumns} data={filteredData} isScrollable />
            {!!fields && (
                <SchemaFieldDrawer
                    schemaFields={fields}
                    expandedDrawerFieldPath={expandedDrawerFieldPath}
                    editableSchemaMetadata={editableSchemaMetadata}
                    setExpandedDrawerFieldPath={setExpandedDrawerFieldPath}
                    displayedRows={rows}
                    defaultSelectedTabName="Statistics"
                />
            )}
        </>
    );
};

export default ColumnStatsTable;
