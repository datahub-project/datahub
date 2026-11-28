import { EmptyState, Table } from '@components';
import { ListBullets } from '@phosphor-icons/react/dist/csr/ListBullets';
import React, { useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import type { Column, TableTreeDataProps } from '@components/components/Table/types';

import { useEntityData } from '@app/entityV2/shared/EntityContext';
import NameColumn from '@app/entityV2/shared/tabs/Properties/NameColumn';
import TabHeader from '@app/entityV2/shared/tabs/Properties/TabHeader';
import ValuesColumn from '@app/entityV2/shared/tabs/Properties/ValuesColumn';
import { PropertyRow } from '@app/entityV2/shared/tabs/Properties/types';
import useStructuredProperties from '@app/entityV2/shared/tabs/Properties/useStructuredProperties';
import useUpdateExpandedRowsFromFilter from '@app/entityV2/shared/tabs/Properties/useUpdateExpandedRowsFromFilter';
import {
    filterHiddenProperties,
    getFilteredCustomProperties,
    getPropertyRowKey,
    mapCustomPropertiesToPropertyRows,
} from '@app/entityV2/shared/tabs/Properties/utils';
import { TabRenderType } from '@app/entityV2/shared/types';
import Loading from '@app/sharedV2/Loading';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { EditColumn } from '@src/app/entityV2/shared/tabs/Properties/Edit/EditColumn';
import { canEditDisplayedStructuredProperties } from '@src/app/entityV2/shared/tabs/Properties/structuredPropertyPrivileges';
import { Maybe, SchemaFieldEntity } from '@src/types.generated';

// Same inset as the search bar above, like the Columns tab.
const TableContainer = styled.div`
    padding: 0 16px 16px 16px;
`;

const EmptyStateContainer = styled.div`
    padding: 48px 20px;
`;

interface Props {
    properties?: {
        fieldPath?: string;
        fieldUrn?: string;
        fieldEntity?: Maybe<SchemaFieldEntity>;
        refetch?: () => void;
        disableEdit?: boolean;
        disableSearch?: boolean;
    };
    renderType?: TabRenderType;
}

export const PropertiesTab = ({ renderType = TabRenderType.DEFAULT, properties }: Props) => {
    const { t } = useTranslation('entity.profile.tabs');
    const { t: tc } = useTranslation('common.labels');
    const fieldPath = properties?.fieldPath;
    const fieldUrn = properties?.fieldUrn;
    const fieldEntity = properties?.fieldEntity;
    const fieldProperties = fieldEntity?.structuredProperties;
    const refetch = properties?.refetch;
    const [filterText, setFilterText] = useState('');
    const { entityData } = useEntityData();
    const entityRegistry = useEntityRegistry();

    const {
        structuredPropertyRows,
        expandedRowsFromFilter,
        loading: structuredPropertiesLoading,
    } = useStructuredProperties(entityRegistry, fieldPath || null, filterText, fieldEntity);

    // avoid flashing an empty table when revisiting this tab before the parent drawer's
    // field entity has loaded
    const isLoadingFieldProperties = !!fieldPath && !fieldEntity && structuredPropertiesLoading;

    // only show entity custom properties on entity level, not on field level
    const customProperties = !fieldPath ? getFilteredCustomProperties(filterText, entityData) || [] : [];
    const customPropertyRows = mapCustomPropertiesToPropertyRows(customProperties);
    const dataSource: PropertyRow[] = filterHiddenProperties(structuredPropertyRows)
        .concat(customPropertyRows)
        .filter((row) => !row.structuredProperty?.settings?.isHidden);

    const [expandedRows, setExpandedRows] = useState<Set<string>>(new Set());

    useUpdateExpandedRowsFromFilter({ expandedRowsFromFilter, setExpandedRows });

    const canEditProperties = canEditDisplayedStructuredProperties(entityData, !!fieldPath) && !properties?.disableEdit;

    const propertyTableColumns: Column<PropertyRow>[] = [
        {
            width: '40%',
            title: tc('name'),
            key: 'name',
            render: (propertyRow) => <NameColumn propertyRow={propertyRow} filterText={filterText} />,
        },
        {
            title: tc('value'),
            key: 'value',
            render: (propertyRow) => (
                <ValuesColumn
                    propertyRow={propertyRow}
                    filterText={filterText}
                    renderType={renderType}
                    scopeKey={fieldPath ? `${entityData?.urn}:${fieldPath}` : (entityData?.urn ?? '')}
                />
            ),
        },
    ];

    if (canEditProperties) {
        propertyTableColumns.push({
            title: '',
            key: 'edit',
            width: '10%',
            render: (propertyRow) => (
                <EditColumn
                    structuredProperty={propertyRow.structuredProperty}
                    associatedUrn={propertyRow.associatedUrn}
                    values={propertyRow.values?.map((v) => v.value) || []}
                    refetch={refetch}
                />
            ),
        });
    }

    const treeData = useMemo(
        (): TableTreeDataProps<PropertyRow> => ({
            getChildren: (row) => row.children,
            expandedRowKeys: Array.from(expandedRows),
            onExpandedRowsChange: (keys) => setExpandedRows(new Set(keys)),
        }),
        [expandedRows],
    );

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
            <TableContainer data-testid="entity-properties-table">
                {isLoadingFieldProperties && <Loading />}
                {!isLoadingFieldProperties && dataSource.length === 0 && (
                    <EmptyStateContainer>
                        <EmptyState icon={ListBullets} title={t('properties.empty')} size="sm" />
                    </EmptyStateContainer>
                )}
                {!isLoadingFieldProperties && dataSource.length > 0 && (
                    <Table<PropertyRow>
                        columns={propertyTableColumns}
                        data={dataSource}
                        rowKey={getPropertyRowKey}
                        treeData={treeData}
                    />
                )}
            </TableContainer>
        </>
    );
};
