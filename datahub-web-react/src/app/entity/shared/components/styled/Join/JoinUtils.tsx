import React from 'react';
import { FormInstance } from 'antd';
import { EntityType } from '../../../../../../types.generated';

export const EditableContext = React.createContext<FormInstance<any> | null>(null);
export interface JoinDataType {
    key: React.Key;
    field1Name: string;
    field2Name: string;
}

export const checkDuplicateJoin = async (getSearchResultsJoins, name): Promise<boolean> => {
    const { data: searchResults } = await getSearchResultsJoins({
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
    });
    const recordExists = searchResults && searchResults.search && searchResults.search.total > 0;
    return recordExists || false;
};
const validateTableData = (fieldMappingData: JoinDataType) => {
    if (fieldMappingData.field1Name !== '' && fieldMappingData.field2Name !== '') {
        return true;
    }
    return false;
};
export const validateJoin = async (nameField: string, tableSchema: JoinDataType[], editFlag, getSearchResultsJoins) => {
    const errors: string[] = [];
    const bDuplicateName = await checkDuplicateJoin(getSearchResultsJoins, nameField?.trim()).then((result) => result);
    if (nameField === '') {
        errors.push('Join name is required');
    }
    if (bDuplicateName && !editFlag) {
        errors.push('This join name already exists. A unique name for each join is required');
    }
    const faultyRows = tableSchema.filter((item) => validateTableData(item) !== true);
    if (faultyRows.length > 0) {
        errors.push('Please fill out or remove all empty Join fields');
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
