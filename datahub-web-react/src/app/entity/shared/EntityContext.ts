import React, { useContext } from 'react';
import { EntityType } from '../../../types.generated';
import { shouldEntityBeTreatedAsPrimary, useIsSeparateSiblingsMode } from './siblingUtils';
import { EntityContextType, UpdateEntityType } from './types';

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
