import React, { useCallback, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import TabHeader from '@app/entity/shared/tabs/Properties/TabHeader';
import { PropertyRow } from '@app/entity/shared/tabs/Properties/types';
import useUpdateExpandedRowsFromFilter from '@app/entity/shared/tabs/Properties/useUpdateExpandedRowsFromFilter';
import {
    getFilteredCustomProperties,
    mapCustomPropertiesToPropertyRows,
} from '@app/entity/shared/tabs/Properties/utils';
import NameColumn from '@app/entityV2/shared/tabs/Properties/NameColumn';
import ValuesColumn from '@app/entityV2/shared/tabs/Properties/ValuesColumn';
import { useHydratedEntityMap } from '@app/entityV2/shared/tabs/Properties/useHydratedEntityMap';
import useStructuredProperties from '@app/entityV2/shared/tabs/Properties/useStructuredProperties';
import { TabRenderType } from '@app/entityV2/shared/types';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';
import { EmptyState, Table } from '@src/alchemy-components';
import { Column, ExpandableProps } from '@src/alchemy-components/components/Table/types';
import { EditColumn } from '@src/app/entity/shared/tabs/Properties/Edit/EditColumn';
import { Maybe, StructuredProperties } from '@src/types.generated';

const TableWrapper = styled.div`
    width: 100%;
    padding: 16px;
`;

const EmptyWrapper = styled.div`
    padding: 40px 0;
`;

function isRowHidden(row: PropertyRow): boolean {
    return !!row.structuredProperty?.settings?.isHidden;
}

// Add a `name` alias (== qualifiedName) recursively so the alchemy Table can key expansion off `name`.
function withExpansionKeys(rows: PropertyRow[]): PropertyRow[] {
    return rows.map((row) => ({
        ...row,
        name: row.qualifiedName,
        children: row.children ? withExpansionKeys(row.children) : row.children,
    }));
}

interface Props {
    properties?: {
        fieldPath?: string;
        fieldUrn?: string;
        fieldProperties?: Maybe<StructuredProperties>;
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
    const fieldProperties = properties?.fieldProperties;
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

    const dataSource: PropertyRow[] = useMemo(
        () => withExpansionKeys(structuredPropertyRows.concat(customPropertyRows).filter((row) => !isRowHidden(row))),
        [structuredPropertyRows, customPropertyRows],
    );

    const [expandedRows, setExpandedRows] = useState<Set<string>>(new Set());

    useUpdateExpandedRowsFromFilter({ expandedRowsFromFilter, setExpandedRows });

    const toggleExpanded = useCallback((qualifiedName: string) => {
        setExpandedRows((previousRows) => {
            const next = new Set(previousRows);
            if (next.has(qualifiedName)) {
                next.delete(qualifiedName);
            } else {
                next.add(qualifiedName);
            }
            return next;
        });
    }, []);

    const entityUrnsToHydrate = structuredPropertyRowsRaw
        .flatMap((row) => row?.values?.map((v) => (typeof v?.value === 'string' ? v.value : null)))
        .filter(Boolean);

    const hydratedEntityMap = useHydratedEntityMap(entityUrnsToHydrate);

    const canEditProperties =
        (entityData?.parent?.privileges?.canEditProperties || entityData?.privileges?.canEditProperties) &&
        !properties?.disableEdit;

    const columns = useMemo<Column<PropertyRow>[]>(() => {
        const cols: Column<PropertyRow>[] = [
            {
                title: tc('name'),
                key: 'name',
                width: '40%',
                render: (propertyRow: PropertyRow) => (
                    <NameColumn
                        propertyRow={propertyRow}
                        filterText={filterText}
                        isExpanded={expandedRows.has(propertyRow.qualifiedName)}
                        onToggleExpand={() => toggleExpanded(propertyRow.qualifiedName)}
                    />
                ),
            },
            {
                title: tc('value'),
                key: 'value',
                render: (propertyRow: PropertyRow) => (
                    <ValuesColumn
                        propertyRow={propertyRow}
                        filterText={filterText}
                        hydratedEntityMap={hydratedEntityMap}
                        renderType={renderType}
                    />
                ),
            },
        ];

        if (canEditProperties) {
            cols.push({
                title: '',
                key: 'actions',
                width: '10%',
                render: (propertyRow: PropertyRow) => (
                    <EditColumn
                        structuredProperty={propertyRow.structuredProperty}
                        associatedUrn={propertyRow.associatedUrn}
                        values={propertyRow.values?.map((v) => v.value) || []}
                        refetch={refetch}
                    />
                ),
            });
        }

        return cols;
    }, [tc, filterText, expandedRows, toggleExpanded, hydratedEntityMap, renderType, canEditProperties, refetch]);

    // Recursive expandable config: each expanded parent renders an inner alchemy Table for its
    // (non-hidden) children, which in turn reuses this same config for arbitrarily deep nesting.
    const expandable = useMemo<ExpandableProps<PropertyRow>>(() => {
        const config: ExpandableProps<PropertyRow> = {
            expandedGroupIds: Array.from(expandedRows),
            expandedRowRender: (record: PropertyRow) => {
                const childRows = (record.children || []).filter((row) => !isRowHidden(row));
                return (
                    <Table
                        columns={columns}
                        data={childRows}
                        showHeader={false}
                        isBorderless
                        isExpandedInnerTable
                        expandable={config}
                    />
                );
            },
        };
        return config;
    }, [columns, expandedRows]);

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
            <TableWrapper data-testid="entity-properties-table">
                {dataSource.length === 0 ? (
                    <EmptyWrapper>
                        <EmptyState title={t('properties.empty')} />
                    </EmptyWrapper>
                ) : (
                    <Table columns={columns} data={dataSource} expandable={expandable} />
                )}
            </TableWrapper>
        </>
    );
};
