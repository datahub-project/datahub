import { Empty, Table } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import TabHeader from '@app/entity/shared/tabs/Properties/TabHeader';
import { PropertyRow } from '@app/entity/shared/tabs/Properties/types';
import useUpdateExpandedRowsFromFilter from '@app/entity/shared/tabs/Properties/useUpdateExpandedRowsFromFilter';
import {
    getFilteredCustomProperties,
    mapCustomPropertiesToPropertyRows,
} from '@app/entity/shared/tabs/Properties/utils';
import ExpandIcon from '@app/entityV2/shared/tabs/Dataset/Schema/components/ExpandIcon';
import NameColumn from '@app/entityV2/shared/tabs/Properties/NameColumn';
import ValuesColumn from '@app/entityV2/shared/tabs/Properties/ValuesColumn';
import { useGetProposedProperties } from '@app/entityV2/shared/tabs/Properties/useGetProposedProperties';
import { useHydratedEntityMap } from '@app/entityV2/shared/tabs/Properties/useHydratedEntityMap';
import useStructuredProperties from '@app/entityV2/shared/tabs/Properties/useStructuredProperties';
import { filterStructuredProperties } from '@app/entityV2/shared/tabs/Properties/utils';
import { TabRenderType } from '@app/entityV2/shared/types';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';
import { EditColumn } from '@src/app/entity/shared/tabs/Properties/Edit/EditColumn';
import { ActionRequest, Maybe, SchemaFieldEntity, StructuredProperties } from '@src/types.generated';

const StyledTable = styled(Table)`
    &&& .ant-table-cell-with-append {
        padding: 16px;
    }
    &&& .ant-table-tbody > tr > td {
        border: none;
    }
    &&& .row-icon-container {
        margin-bottom: 4px;
    }
` as typeof Table;

const EmptyText = styled(Empty)`
    font-size: 14px;
`;

interface Props {
    properties?: {
        fieldPath?: string;
        fieldUrn?: string;
        fieldProperties?: Maybe<StructuredProperties>;
        fieldEntity?: Maybe<SchemaFieldEntity>;
        refetch?: () => void;
        disableEdit?: boolean;
        disableSearch?: boolean;
    };
    renderType?: TabRenderType;
}

export const PropertiesTab = ({ renderType = TabRenderType.DEFAULT, properties }: Props) => {
    const fieldPath = properties?.fieldPath;
    const fieldUrn = properties?.fieldUrn;
    const fieldProperties = properties?.fieldProperties;
    const fieldEntity = properties?.fieldEntity;
    const refetch = properties?.refetch;
    const [filterText, setFilterText] = useState('');
    const { entityData } = useEntityData();
    const entityRegistry = useEntityRegistryV2();

    const { structuredPropertyRows, expandedRowsFromFilter, structuredPropertyRowsRaw } = useStructuredProperties(
        entityRegistry,
        fieldPath || null,
        filterText,
    );

    // only show entity custom properties on entity level, not on field level
    const customProperties = !fieldPath ? getFilteredCustomProperties(filterText, entityData) || [] : [];
    const customPropertyRows = mapCustomPropertiesToPropertyRows(customProperties);

    const { proposedRows } = useGetProposedProperties({ fieldPath });
    const { filteredRows: filteredProposedRows } = filterStructuredProperties(entityRegistry, proposedRows, filterText);

    const dataSource: PropertyRow[] = structuredPropertyRows
        .concat(customPropertyRows)
        .filter((row) => !row.structuredProperty?.settings?.isHidden);

    const finalDataSource: (PropertyRow & { request?: ActionRequest })[] = [...dataSource, ...filteredProposedRows];

    const [expandedRows, setExpandedRows] = useState<Set<string>>(new Set());

    useUpdateExpandedRowsFromFilter({ expandedRowsFromFilter, setExpandedRows });

    const entityUrnsToHydrate =
        structuredPropertyRowsRaw.flatMap((row) => row?.values?.map((v) => v.entity?.urn)).filter(Boolean) ?? [];
    const proposedEntityUrns =
        proposedRows.flatMap((row) => row?.values?.map((v) => v.entity?.urn)).filter(Boolean) ?? [];

    const hydratedEntityMap = useHydratedEntityMap(entityUrnsToHydrate.concat(proposedEntityUrns));

    const propertyTableColumns = [
        {
            width: '40%',
            title: 'Name',
            render: (propertyRow: PropertyRow) => <NameColumn propertyRow={propertyRow} filterText={filterText} />,
        },
        {
            title: 'Value',
            ellipsis: true,
            render: (propertyRow: PropertyRow & { request?: ActionRequest }) => (
                <ValuesColumn
                    propertyRow={propertyRow}
                    filterText={filterText}
                    hydratedEntityMap={hydratedEntityMap}
                    renderType={renderType}
                    isProposed={propertyRow.isProposed}
                />
            ),
        },
    ];

    const canEditProperties =
        (entityData?.parent?.privileges?.canEditProperties || entityData?.privileges?.canEditProperties) &&
        !properties?.disableEdit;

    if (canEditProperties) {
        propertyTableColumns.push({
            title: '',
            width: '10%',
            render: (propertyRow: PropertyRow) => {
                if (propertyRow.isProposed) return null;
                return (
                    <EditColumn
                        structuredProperty={propertyRow?.structuredProperty}
                        associatedUrn={propertyRow?.associatedUrn}
                        values={propertyRow?.values?.map((v) => v.value) || []}
                        refetch={refetch}
                        fieldEntity={fieldEntity}
                    />
                );
            },
        } as any);
    }

    return (
        <>
            {!properties?.disableSearch && (
                <TabHeader
                    setFilterText={setFilterText}
                    fieldUrn={fieldUrn}
                    fieldProperties={fieldProperties}
                    refetch={refetch}
                />
            )}
            <StyledTable
                pagination={false}
                // typescript is complaining that default sort order is not a valid column field- overriding this here
                // eslint-disable-next-line @typescript-eslint/ban-ts-comment
                // @ts-ignore
                columns={propertyTableColumns}
                dataSource={finalDataSource}
                locale={{
                    emptyText: <EmptyText description="No properties found" image={Empty.PRESENTED_IMAGE_SIMPLE} />,
                }}
                rowKey="qualifiedName"
                expandable={{
                    expandedRowKeys: [...Array.from(expandedRows)],
                    defaultExpandAllRows: false,
                    expandRowByClick: false,
                    expandIcon: (props) => <ExpandIcon {...props} isCompact />,
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
                    indentSize: 16,
                }}
            />
        </>
    );
};
