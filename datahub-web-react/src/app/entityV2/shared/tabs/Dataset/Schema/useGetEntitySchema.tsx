/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { cloneDeep } from 'lodash';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { combineEntityDataWithSiblings } from '@app/entity/shared/siblingUtils';
import { useIsSeparateSiblingsMode } from '@app/entityV2/shared/useIsSeparateSiblingsMode';

import { useGetDatasetSchemaQuery } from '@graphql/dataset.generated';
import { EntityType } from '@types';

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
