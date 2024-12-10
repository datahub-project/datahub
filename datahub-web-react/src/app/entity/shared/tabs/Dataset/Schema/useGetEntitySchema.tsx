import { cloneDeep } from 'lodash';
import { EntityType } from '../../../../../../types.generated';
import { useEntityData } from '../../../EntityContext';
import { GetDatasetSchemaQuery, useGetDatasetSchemaQuery } from '../../../../../../graphql/dataset.generated';
import { combineEntityDataWithSiblings, useIsSeparateSiblingsMode } from '../../../siblingUtils';

// Whether to dynamically load the schema from the backend.
const shouldLoadSchema = (entityType, entityData) => {
    return entityType === EntityType.Dataset && !entityData?.schemaMetadata;
};

export const useGetEntityWithSchema = (
    skip?: boolean,
    onCompleted?: ((data: GetDatasetSchemaQuery) => void) | undefined,
) => {
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
        skip: skip || !shouldLoadSchema(entityType, entityData),
        fetchPolicy: 'cache-first',
        onCompleted,
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
