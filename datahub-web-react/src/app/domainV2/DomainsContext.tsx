import React, { useContext } from 'react';
import { GenericEntityProperties } from '../entity/shared/types';

export interface DomainsContextType {
    entityData: GenericEntityProperties | null;
    setEntityData: (entityData: GenericEntityProperties | null) => void;
}

export const DomainsContext = React.createContext<DomainsContextType>({
    entityData: null,
    setEntityData: () => {},
});

export const useDomainsContext = () => {
    const { entityData, setEntityData } = useContext(DomainsContext);
    return { entityData, setEntityData };
};
