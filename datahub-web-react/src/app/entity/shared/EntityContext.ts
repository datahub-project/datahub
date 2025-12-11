/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React, { useContext } from 'react';

import { shouldEntityBeTreatedAsPrimary, useIsSeparateSiblingsMode } from '@app/entity/shared/siblingUtils';
import { EntityContextType, UpdateEntityType } from '@app/entity/shared/types';

import { EntityType } from '@types';

export const EntityContext = React.createContext<EntityContextType>({
    urn: '',
    entityType: EntityType.Dataset,
    entityData: null,
    loading: true,
    baseEntity: null,
    updateEntity: () => Promise.resolve({}),
    routeToTab: () => {},
    refetch: () => Promise.resolve({}),
    lineage: undefined,
    dataNotCombinedWithSiblings: null,
    entityState: { shouldRefetchContents: false, setShouldRefetchContents: () => {} },
});

export default EntityContext;

export function useEntityContext() {
    return useContext(EntityContext);
}

export const useBaseEntity = <T>(): T => {
    const { baseEntity } = useContext(EntityContext);
    return baseEntity as T;
};

export const useDataNotCombinedWithSiblings = <T>(): T => {
    const { dataNotCombinedWithSiblings } = useContext(EntityContext);
    return dataNotCombinedWithSiblings as T;
};

export const useEntityUpdate = <U>(): UpdateEntityType<U> | null | undefined => {
    const { updateEntity } = useContext(EntityContext);
    return updateEntity;
};

export const useEntityData = () => {
    const { urn, entityType, entityData, loading } = useContext(EntityContext);
    return { urn, entityType, entityData, loading };
};

export const useRouteToTab = () => {
    const { routeToTab } = useContext(EntityContext);
    return routeToTab;
};

export const useRefetch = () => {
    const { refetch } = useContext(EntityContext);
    return refetch;
};

export const useLineageData = () => {
    const { lineage } = useContext(EntityContext);
    return lineage;
};

export const useMutationUrn = () => {
    const { urn, entityData } = useContext(EntityContext);
    const isHideSiblingMode = useIsSeparateSiblingsMode();
    if (!entityData?.siblingsSearch?.searchResults || shouldEntityBeTreatedAsPrimary(entityData) || isHideSiblingMode) {
        return urn;
    }
    return entityData?.siblingsSearch?.searchResults?.[0].entity.urn || urn;
};
