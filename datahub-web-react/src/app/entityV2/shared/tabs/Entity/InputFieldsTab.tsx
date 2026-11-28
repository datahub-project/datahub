import { EmptyState } from '@components';
import { Columns } from '@phosphor-icons/react/dist/csr/Columns';
import React, { useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useLocation } from 'react-router';
import styled from 'styled-components';

import { groupByFieldPath } from '@app/entityV2/dataset/profile/schema/utils/utils';
import { useEntityData } from '@app/entityV2/shared/EntityContext';
import SchemaTable from '@app/entityV2/shared/tabs/Dataset/Schema/SchemaTable';
import getExpandedDrawerFieldPath from '@app/entityV2/shared/tabs/Dataset/Schema/utils/getExpandedDrawerFieldPath';
import SchemaEditableContext from '@app/sharedV2/SchemaEditableContext';

import { SchemaField } from '@types';

const NoSchemaContainer = styled.div`
    padding-top: 60px;
`;

const SchemaTableContainer = styled.div`
    overflow: auto;
    height: 100%;
`;
export const InputFieldsTab = () => {
    const { t } = useTranslation('entity.profile.tabs');
    const { entityData } = useEntityData();
    const inputFields = entityData?.inputFields || undefined;
    const ungroupedRows = inputFields?.fields?.map((field) => field?.schemaField) as SchemaField[];

    const rows = useMemo(() => {
        return groupByFieldPath(ungroupedRows, { showKeySchema: false });
    }, [ungroupedRows]);
    const location = useLocation();

    const expandedDrawerFieldPathFromUrl = getExpandedDrawerFieldPath(location);

    const [expandedDrawerFieldPath, setExpandedDrawerFieldPath] = useState<string | null>(
        expandedDrawerFieldPathFromUrl,
    );

    return (
        <>
            <SchemaTableContainer>
                {rows && rows.length > 0 ? (
                    <>
                        <SchemaEditableContext.Provider value={false}>
                            <SchemaTable
                                schemaMetadata={null}
                                rows={rows}
                                editableSchemaMetadata={null}
                                usageStats={null}
                                expandedDrawerFieldPath={expandedDrawerFieldPath}
                                setExpandedDrawerFieldPath={setExpandedDrawerFieldPath}
                                inputFields={ungroupedRows}
                            />
                        </SchemaEditableContext.Provider>
                    </>
                ) : (
                    <NoSchemaContainer>
                        <EmptyState icon={Columns} title={t('inputFields.empty')} />
                    </NoSchemaContainer>
                )}
            </SchemaTableContainer>
        </>
    );
};
