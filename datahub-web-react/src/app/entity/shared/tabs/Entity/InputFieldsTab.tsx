/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Empty } from 'antd';
import React, { useMemo } from 'react';
import styled from 'styled-components';

import { groupByFieldPath } from '@app/entity/dataset/profile/schema/utils/utils';
import { useEntityData } from '@app/entity/shared/EntityContext';
import { ANTD_GRAY } from '@app/entity/shared/constants';
import SchemaTable from '@app/entity/shared/tabs/Dataset/Schema/SchemaTable';
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
                                schemaFieldBlameList={null}
                                showSchemaAuditView={false}
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
