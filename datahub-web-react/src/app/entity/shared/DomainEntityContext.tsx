import React, { useContext } from 'react';
import { GenericEntityProperties } from './types';

export interface DomainEntityContextType {
    isInDomainContext: boolean;
    entityData: GenericEntityProperties | null;
    setEntityData: (entityData: GenericEntityProperties | null) => void;
    // Since we have domain data in the profile and in the sidebar browser, we need to communicate to the
    // sidebar when to refetch for a given domain or sub-domain. This will happen when you edit a name,
    // move a domain/sub-domain, create a new domain/sub-domain, and delete a domain/sub-domain
    urnsToUpdate: string[];
    setUrnsToUpdate: (updatdUrns: string[]) => void;
}

export const DomainEntityContext = React.createContext<DomainEntityContextType>({
    isInDomainContext: false,
    entityData: null,
    setEntityData: () => {},
    urnsToUpdate: [],
    setUrnsToUpdate: () => {},
});

export const useDomainEntityData = () => {
    const { isInDomainContext, entityData, setEntityData, urnsToUpdate, setUrnsToUpdate } =
        useContext(DomainEntityContext);
    return { isInDomainContext, entityData, setEntityData, urnsToUpdate, setUrnsToUpdate };
};
