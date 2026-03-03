import React, { useState } from 'react';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { StyledTable as Table } from '@app/entity/shared/components/styled/StyledTable';
import ExpandIcon from '@app/entity/shared/tabs/Dataset/Schema/components/ExpandIcon';
import { EditColumn } from '@app/entity/shared/tabs/Properties/Edit/EditColumn';
import NameColumn from '@app/entity/shared/tabs/Properties/NameColumn';
import TabHeader from '@app/entity/shared/tabs/Properties/TabHeader';
import ValuesColumn from '@app/entity/shared/tabs/Properties/ValuesColumn';
import { PropertyRow } from '@app/entity/shared/tabs/Properties/types';
import useStructuredProperties from '@app/entity/shared/tabs/Properties/useStructuredProperties';
import useUpdateExpandedRowsFromFilter from '@app/entity/shared/tabs/Properties/useUpdateExpandedRowsFromFilter';
import {
    getFilteredCustomProperties,
    mapCustomPropertiesToPropertyRows,
} from '@app/entity/shared/tabs/Properties/utils';
import { useEntityRegistry } from '@app/useEntityRegistry';

const StyledTable = styled(Table)`
    &&& .ant-table-cell-with-append {
        padding: 4px;
    }
` as typeof Table;

export const PropertiesTab = () => {
    const [filterText, setFilterText] = useState('');
    const { entityData } = useEntityData();
    const entityRegistry = useEntityRegistry();

    const propertyTableColumns = [
        {
            width: '40%',
            title: 'Name',
            defaultSortOrder: 'ascend',
            render: (propertyRow: PropertyRow) => <NameColumn propertyRow={propertyRow} filterText={filterText} />,
        },
        {
            title: 'Value',
            render: (propertyRow: PropertyRow) => <ValuesColumn propertyRow={propertyRow} filterText={filterText} />,
        },
    ];

    if (entityData?.privileges?.canEditProperties) {
        propertyTableColumns.push({
            title: '',
            width: '10%',
            render: (propertyRow: PropertyRow) => (
                <EditColumn
                    structuredProperty={propertyRow.structuredProperty}
                    associatedUrn={propertyRow.associatedUrn}
                    values={propertyRow.values?.map((v) => v.value) || []}
                />
            ),
        } as any);
    }

    const { structuredPropertyRows, expandedRowsFromFilter } = useStructuredProperties(entityRegistry, filterText);
    const filteredStructuredPropertyRows = structuredPropertyRows.filter(
        (row) => !row.structuredProperty?.settings?.isHidden,
    );
    const customProperties = getFilteredCustomProperties(filterText, entityData) || [];
    const customPropertyRows = mapCustomPropertiesToPropertyRows(customProperties);
    const dataSource: PropertyRow[] = filteredStructuredPropertyRows.concat(customPropertyRows);

    const [expandedRows, setExpandedRows] = useState<Set<string>>(new Set());

    useUpdateExpandedRowsFromFilter({ expandedRowsFromFilter, setExpandedRows });

    return (
        <>
            <TabHeader setFilterText={setFilterText} />
            <StyledTable
                pagination={false}
                // typescript is complaining that default sort order is not a valid column field- overriding this here
                // eslint-disable-next-line @typescript-eslint/ban-ts-comment
                // @ts-ignore
                columns={propertyTableColumns}
                dataSource={dataSource}
                rowKey="qualifiedName"
                expandable={{
                    expandedRowKeys: [...Array.from(expandedRows)],
                    defaultExpandAllRows: false,
                    expandRowByClick: false,
                    expandIcon: ExpandIcon,
                    onExpand: (expanded, record) => {
                        if (expanded) {
                            setExpandedRows((previousRows) => new Set(previousRows.add(record.qualifiedName)));
                        } else {
                            setExpandedRows((previousRows) => {
                                previousRows.delete(record.qualifiedName);
                                return new Set(previousRows);
                            });
                        }
                    },
                    indentSize: 0,
                }}
            />
        </>
    );
};
