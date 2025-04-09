import { cloneDeep } from 'lodash';
import { EntityType } from '../../../../../../types.generated';
import { useEntityData } from '../../../../../entity/shared/EntityContext';
import { useGetDatasetSchemaQuery } from '../../../../../../graphql/dataset.generated';
import { combineEntityDataWithSiblings } from '../../../../../entity/shared/siblingUtils';
import { useIsSeparateSiblingsMode } from '../../../useIsSeparateSiblingsMode';

// Whether to dynamically load the schema from the backend.
const shouldLoadSchema = (entityType, entityData) => {
    return entityType === EntityType.Dataset && !entityData?.schemaMetadata;
};

export const useGetEntityWithSchema = (skip?: boolean) => {
    const { urn, entityData, entityType } = useEntityData();
    // Load the dataset schema lazily.
    const {
        data: rawData,
        loading,
        refetch,
    } = useGetDatasetSchemaQuery({
        variables: {
            urn,
        },
        skip: skip || !urn || !shouldLoadSchema(entityType, entityData),
        fetchPolicy: 'cache-first',
    });
    const isHideSiblingMode = useIsSeparateSiblingsMode();
    // Merge with sibling information as required.
    const combinedData = rawData && !isHideSiblingMode ? combineEntityDataWithSiblings(cloneDeep(rawData)) : rawData;
    return {
        loading,
        entityWithSchema: shouldLoadSchema(entityType, entityData) ? combinedData?.dataset : entityData,
        refetch,
    };
};
