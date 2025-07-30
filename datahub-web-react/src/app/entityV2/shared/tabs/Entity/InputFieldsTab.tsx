import { Empty } from 'antd';
import React, { useMemo, useState } from 'react';
import { useLocation } from 'react-router';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { groupByFieldPath } from '@app/entityV2/dataset/profile/schema/utils/utils';
import { ANTD_GRAY } from '@app/entityV2/shared/constants';
import SchemaTable from '@app/entityV2/shared/tabs/Dataset/Schema/SchemaTable';
import getExpandedDrawerFieldPath from '@app/entityV2/shared/tabs/Dataset/Schema/utils/getExpandedDrawerFieldPath';
import SchemaEditableContext from '@app/shared/SchemaEditableContext';

import { SchemaField } from '@types';

const NoSchema = styled(Empty)`
    color: ${ANTD_GRAY[6]};
    padding-top: 60px;
`;

const SchemaTableContainer = styled.div`
    overflow: auto;
    height: 100%;
`;
export const InputFieldsTab = () => {
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
                    <NoSchema />
                )}
            </SchemaTableContainer>
        </>
    );
};
