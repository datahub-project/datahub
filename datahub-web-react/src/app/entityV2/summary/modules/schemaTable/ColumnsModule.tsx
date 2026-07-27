import { Database } from '@phosphor-icons/react/dist/csr/Database';
import React, { useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useHistory } from 'react-router-dom';
import styled from 'styled-components';

import { useBaseEntity, useEntityData } from '@app/entity/shared/EntityContext';
import { groupByFieldPath } from '@app/entityV2/dataset/profile/schema/utils/utils';
import SchemaContext from '@app/entityV2/shared/tabs/Dataset/Schema/SchemaContext';
import SchemaTable from '@app/entityV2/shared/tabs/Dataset/Schema/SchemaTable';
import { SchemaFilterType, filterSchemaRows } from '@app/entityV2/shared/tabs/Dataset/Schema/utils/filterSchemaRows';
import EmptyContent from '@app/homeV3/module/components/EmptyContent';
import LargeModule from '@app/homeV3/module/components/LargeModule';
import { useModuleContext } from '@app/homeV3/module/context/ModuleContext';
import { ModuleProps, ModuleSize } from '@app/homeV3/module/types';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { GetDatasetQuery, useGetDatasetSchemaQuery } from '@graphql/dataset.generated';
import { EditableSchemaMetadata, EntityType, SchemaMetadata } from '@types';

const SCHEMA_TABLE_FIELDS = ['fieldPath', 'type', 'description'];

const Wrapper = styled.div`
    padding-top: 8px;
    height: 100%;
`;

export default function ColumnsModule(props: ModuleProps) {
    const { t } = useTranslation('modules');
    const { urn: entityUrn } = useEntityData();
    const [expandedDrawerFieldPath, setExpandedDrawerFieldPath] = useState<string | null>(null);
    const entityRegistry = useEntityRegistry();
    const baseEntity = useBaseEntity<GetDatasetQuery>();
    const history = useHistory();
    const { size } = useModuleContext();

    const navigateToSchemaTab = () => {
        const baseUrl = entityRegistry.getEntityUrl(EntityType.Dataset, entityUrn);
        history.push(`${baseUrl}/Columns`);
    };

    const { data, loading, error, refetch } = useGetDatasetSchemaQuery({
        variables: {
            urn: entityUrn,
        },
        skip: !entityUrn,
    });

    const schemaMetadata = data?.dataset?.schemaMetadata as SchemaMetadata | null | undefined;
    const editableSchemaMetadata = data?.dataset?.editableSchemaMetadata as EditableSchemaMetadata | null | undefined;

    const usageStats = baseEntity?.dataset?.usageStats;

    const { filteredRows, expandedRowsFromFilter } = filterSchemaRows(
        schemaMetadata?.fields ?? [],
        editableSchemaMetadata,
        '', // No filter text for module
        [SchemaFilterType.FieldPath, SchemaFilterType.Documentation, SchemaFilterType.Tags, SchemaFilterType.Terms],
        expandedDrawerFieldPath,
        entityRegistry,
        false,
    );

    const rows = useMemo(() => {
        return groupByFieldPath(filteredRows, { showKeySchema: undefined });
    }, [filteredRows]);

    if (error) {
        return (
            <LargeModule {...props} loading={false} dataTestId="schema-table-module">
                <EmptyContent
                    icon={Database}
                    title={t('columns.errorTitle')}
                    description={t('columns.errorDescription')}
                />
            </LargeModule>
        );
    }

    if (!schemaMetadata || !schemaMetadata?.fields || schemaMetadata.fields.length === 0) {
        return (
            <LargeModule {...props} loading={loading} dataTestId="schema-table-module">
                <EmptyContent
                    icon={Database}
                    title={t('columns.emptyTitle')}
                    description={t('columns.emptyDescription')}
                />
            </LargeModule>
        );
    }

    return (
        <SchemaContext.Provider value={{ refetch }}>
            <LargeModule
                {...props}
                loading={loading}
                dataTestId="columns-module"
                onClickViewAll={navigateToSchemaTab}
                viewAllText={t('columns.viewAll')}
            >
                <Wrapper>
                    <SchemaTable
                        rows={rows}
                        schemaMetadata={schemaMetadata}
                        editableSchemaMetadata={editableSchemaMetadata}
                        usageStats={usageStats}
                        expandedRowsFromFilter={expandedRowsFromFilter}
                        filterText=""
                        expandedDrawerFieldPath={expandedDrawerFieldPath}
                        setExpandedDrawerFieldPath={setExpandedDrawerFieldPath}
                        openTimelineDrawer={false}
                        refetch={refetch}
                        visibleColumns={size === ModuleSize.FULL ? undefined : SCHEMA_TABLE_FIELDS}
                    />
                </Wrapper>
            </LargeModule>
        </SchemaContext.Provider>
    );
}
