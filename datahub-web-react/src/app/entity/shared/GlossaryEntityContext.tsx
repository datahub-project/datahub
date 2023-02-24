import React, { useContext } from 'react';
import { GenericEntityProperties } from './types';

export interface GlossaryEntityContextType {
    isInGlossaryContext: boolean;
    entityData: GenericEntityProperties | null;
    setEntityData: (entityData: GenericEntityProperties | null) => void;
    updatedUrns: string[];
    setUpdatedUrns: (updatdUrns: string[]) => void;
}

export const GlossaryEntityContext = React.createContext<GlossaryEntityContextType>({
    isInGlossaryContext: false,
    entityData: null,
    setEntityData: () => {},
    updatedUrns: [],
    setUpdatedUrns: () => {},
});

export const useGlossaryEntityData = () => {
    const { isInGlossaryContext, entityData, setEntityData, updatedUrns, setUpdatedUrns } =
        useContext(GlossaryEntityContext);
    return { isInGlossaryContext, entityData, setEntityData, updatedUrns, setUpdatedUrns };
};
