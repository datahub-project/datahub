import React, { useContext } from 'react';
import { GenericEntityProperties } from '../entity/shared/types';

export interface DomainsContextType {
    entityData: GenericEntityProperties | null;
    setEntityData: (entityData: GenericEntityProperties | null) => void;
    parentDomainsToUpdate: string[];
    setParentDomainsToUpdate: (values: string[]) => void;
}

export const DomainsContext = React.createContext<DomainsContextType>({
    entityData: null,
    setEntityData: () => {},
    parentDomainsToUpdate: [], // used to tell domains to refetch their children count after updates (create, move, delete)
    setParentDomainsToUpdate: () => {},
});

export const useDomainsContext = () => {
    const { entityData, setEntityData, parentDomainsToUpdate, setParentDomainsToUpdate } = useContext(DomainsContext);
    return { entityData, setEntityData, parentDomainsToUpdate, setParentDomainsToUpdate };
};
