import React, { useContext } from 'react';
import { Entity } from '../../../../types.generated';
import { EntityAndType, GenericEntityProperties } from '../types';

export enum FormView {
    BY_ENTITY,
}

export type EntityFormContextType = {
    formUrn: string;
    isInFormContext: boolean;
    entityData: GenericEntityProperties | undefined;
    loading: boolean;
    selectedEntity: Entity | undefined;
    selectedPromptId: string | null;
    formView: FormView;
    selectedEntities: EntityAndType[];
    setSelectedEntities: (entities: EntityAndType[]) => void;
    setFormView: (formView: FormView) => void;
    refetch: () => Promise<any>;
    setSelectedEntity: (sortOption: Entity) => void;
    setSelectedPromptId: (promptId: string) => void;
    shouldRefetchSearchResults: boolean;
    setShouldRefetchSearchResults: (shouldRefetch: boolean) => void;
    isVerificationType: boolean;
};

export const DEFAULT_CONTEXT = {
    formUrn: '',
    isInFormContext: false,
    entityData: undefined,
    loading: false,
    refetch: () => Promise.resolve({}),
    selectedEntity: undefined,
    setSelectedEntity: (_: Entity) => null,
    selectedEntities: [],
    setSelectedEntities: (_: EntityAndType[]) => null,
    formView: FormView.BY_ENTITY,
    setFormView: (_: FormView) => null,
    selectedPromptId: null,
    setSelectedPromptId: (_: string) => null,
    shouldRefetchSearchResults: false,
    setShouldRefetchSearchResults: () => null,
    isVerificationType: true,
};

export const EntityFormContext = React.createContext<EntityFormContextType>(DEFAULT_CONTEXT);

export function useEntityFormContext() {
    const context = useContext(EntityFormContext);
    if (context === null)
        throw new Error(`${useEntityFormContext.name} must be used under a EntityFormContextProvider`);
    return context;
}
