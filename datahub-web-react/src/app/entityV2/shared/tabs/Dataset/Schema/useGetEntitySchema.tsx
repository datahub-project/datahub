import { cloneDeep } from 'lodash';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { combineEntityDataWithSiblings } from '@app/entity/shared/siblingUtils';
import { useActiveColumnViewDefinition } from '@app/entityV2/columnView/ColumnViewContext';
import { schemaQueryIncludesFor } from '@app/entityV2/columnView/columnKinds';
import { useIsSeparateSiblingsMode } from '@app/entityV2/shared/useIsSeparateSiblingsMode';

import { useGetDatasetSchemaQuery } from '@graphql/dataset.generated';
import { EntityType } from '@types';

// Whether to dynamically load the schema from the backend.
const shouldLoadSchema = (entityType, entityData) => {
    return entityType === EntityType.Dataset && !entityData?.schemaMetadata;
};

export const useGetEntityWithSchema = (skip?: boolean) => {
    const { urn, entityData, entityType } = useEntityData();
    // Column Views: only select the optional per-field aspects the active definition needs.
    // Outside a ColumnViewProvider the definition is undefined and every gate defaults to true.
    const activeDefinition = useActiveColumnViewDefinition();
    // Load the dataset schema lazily.
    const {
        data: rawData,
        loading,
        refetch,
    } = useGetDatasetSchemaQuery({
        variables: {
            urn,
            ...schemaQueryIncludesFor(activeDefinition),
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
