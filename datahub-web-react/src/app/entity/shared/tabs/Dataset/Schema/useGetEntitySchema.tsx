import { cloneDeep } from 'lodash';
import { EntityType } from '../../../../../../types.generated';
import { useEntityData } from '../../../EntityContext';
import { useGetDatasetSchemaQuery } from '../../../../../../graphql/dataset.generated';
import { combineEntityDataWithSiblings, useIsSeparateSiblingsMode } from '../../../siblingUtils';

export const useGetEntityWithSchema = () => {
    const { urn, entityData, entityType } = useEntityData();
    // Load the dataset schema lazily.
    const { data: rawData, loading } = useGetDatasetSchemaQuery({
        variables: {
            urn,
        },
        skip: entityType !== EntityType.Dataset,
        fetchPolicy: 'cache-first',
    });
    const isHideSiblingMode = useIsSeparateSiblingsMode();
    // Merge with sibling information as required.
    const combinedData = rawData && !isHideSiblingMode ? combineEntityDataWithSiblings(cloneDeep(rawData)) : rawData;
    return {
        entityWithSchema: entityType === EntityType.Dataset ? combinedData?.dataset : entityData,
        loading,
    };
};
