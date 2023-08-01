import React from 'react';
import { FormInstance } from 'antd';
import { GetSearchResultsDocument } from '../../../../../../graphql/search.generated';
import { EntityType } from '../../../../../../types.generated';

export const EditableContext = React.createContext<FormInstance<any> | null>(null);
export interface JoinDataType {
    key: React.Key;
    field1Name: string;
    field2Name: string;
}
export const checkDuplicateJoin = async (client, name): Promise<boolean> => {
    const { data } = await client.query({
        query: GetSearchResultsDocument,
        variables: {
            input: {
                type: EntityType.Join,
                query: '',
                orFilters: [
                    {
                        and: [
                            {
                                field: 'name',
                                values: [name],
                            },
                        ],
                    },
                ],
                start: 0,
                count: 1000,
            },
        },
    });
    return data && data.search && data.search.total > 0;
};
const validateTableData = (fieldMappingData: JoinDataType) => {
    if (fieldMappingData.field1Name !== '' && fieldMappingData.field2Name !== '') {
        return true;
    }
    return false;
};
export const validateJoin = async (nameField: string, tableSchema: JoinDataType[], editFlag, client) => {
    const errors: string[] = [];
    const bDuplicateName = await checkDuplicateJoin(client, nameField?.trim()).then((result) => result);
    if (nameField === '') {
        errors.push('Join name is required.');
    }
    if (bDuplicateName && !editFlag) {
        errors.push('This join name already exists. A unique name for each join is required.');
    }
    const faultyRows = tableSchema.filter((item) => validateTableData(item) !== true);
    if (faultyRows.length > 0) {
        errors.push('Fields should be selected');
    }
    return errors;
};

export function getDatasetName(datainput: any): string {
    return (
        datainput?.editableProperties?.name ||
        datainput?.properties?.name ||
        datainput?.name ||
        datainput?.urn.split(',').at(1)
    );
}
