import { ModulePositionInput } from '@app/homeV3/template/types';

import { PageModuleFragment, PageTemplateFragment } from '@graphql/template.generated';
import { DataHubPageModuleType, PageModuleScope } from '@types';

// Input types for the methods
export interface UpsertModuleInput {
    urn?: string;
    name: string;
    type: DataHubPageModuleType;
    scope?: PageModuleScope;
    params?: any; // Module-specific parameters
    position: ModulePositionInput;
}

export interface AddModuleInput {
    module: PageModuleFragment;
    position: ModulePositionInput;
}

export interface ModuleModalState {
    isOpen: boolean;
    moduleType: DataHubPageModuleType | null;
    position: ModulePositionInput | null;
    open: (moduleType: DataHubPageModuleType, position: ModulePositionInput) => void;
    close: () => void;
    isEditing: boolean;
    initialState: PageModuleFragment | null;
    openToEdit: (moduleType: DataHubPageModuleType, currentData: PageModuleFragment) => void;
}

// Context state shape
export type PageTemplateContextState = {
    personalTemplate: PageTemplateFragment | null;
    globalTemplate: PageTemplateFragment | null;
    template: PageTemplateFragment | null;
    isEditingGlobalTemplate: boolean;
    setIsEditingGlobalTemplate: (val: boolean) => void;
    setPersonalTemplate: (template: PageTemplateFragment | null) => void;
    setGlobalTemplate: (template: PageTemplateFragment | null) => void;
    setTemplate: (template: PageTemplateFragment | null) => void;
    addModule: (input: AddModuleInput) => void;
    upsertModule: (input: UpsertModuleInput) => void;
    moduleModalState: ModuleModalState;
};
