import React, { useContext } from 'react';
import { GenericEntityProperties } from '../entity/shared/types';

export interface DomainsContextType {
    entityData: GenericEntityProperties | null;
    setEntityData: (entityData: GenericEntityProperties | null) => void;
    parentDomainsToUpate: string[];
    setParentDomainsToUpdate: (values: string[]) => void;
}

export const DomainsContext = React.createContext<DomainsContextType>({
    entityData: null,
    setEntityData: () => {},
    parentDomainsToUpate: [], // used to tell domains to refetch their children count after updates (create, move, delete)
    setParentDomainsToUpdate: () => {},
});

export const useDomainsContext = () => {
    const { entityData, setEntityData, parentDomainsToUpate, setParentDomainsToUpdate } = useContext(DomainsContext);
    return { entityData, setEntityData, parentDomainsToUpate, setParentDomainsToUpdate };
};
