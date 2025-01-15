import React from 'react';
import { FormInstance } from 'antd';
import { EntityType } from '../../../../../../types.generated';

export const EditableContext = React.createContext<FormInstance<any> | null>(null);
export interface ERModelRelationDataType {
    key: React.Key;
    field1Name: string;
    field2Name: string;
}

export const checkDuplicateERModelRelation = async (getSearchResultsERModelRelations, name): Promise<boolean> => {
    const { data: searchResults } = await getSearchResultsERModelRelations({
        input: {
            type: EntityType.ErModelRelationship,
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
const validateTableData = (fieldMappingData: ERModelRelationDataType) => {
    if (fieldMappingData.field1Name !== '' && fieldMappingData.field2Name !== '') {
        return true;
    }
    return false;
};
export const validateERModelRelation = async (
    nameField: string,
    tableSchema: ERModelRelationDataType[],
    editFlag,
    getSearchResultsERModelRelations,
) => {
    const errors: string[] = [];
    const bDuplicateName = await checkDuplicateERModelRelation(
        getSearchResultsERModelRelations,
        nameField?.trim(),
    ).then((result) => result);
    if (nameField === '') {
        errors.push('ER-Model-Relationship name is required');
    }
    if (bDuplicateName && !editFlag) {
        errors.push(
            'This ER-Model-Relationship name already exists. A unique name for each ER-Model-Relationship is required',
        );
    }
    const faultyRows = tableSchema.filter((item) => validateTableData(item) !== true);
    if (faultyRows.length > 0) {
        errors.push('Please fill out or remove all empty ER-Model-Relationship fields');
    }
    return errors;
};

export function getDatasetName(datainput: any): string {
    return (
        datainput?.editableProperties?.name ||
        datainput?.properties?.name ||
        datainput?.name ||
        datainput?.urn?.split(',')?.at(1)
    );
}
