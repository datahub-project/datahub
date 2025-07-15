import React, { ReactNode, createContext, useContext, useMemo } from 'react';

import { useCreateModuleModalState } from '@app/homeV3/context/hooks/useCreateModuleModalState';
import { useModuleOperations } from '@app/homeV3/context/hooks/useModuleOperations';
import { useTemplateOperations } from '@app/homeV3/context/hooks/useTemplateOperations';
import { useTemplateState } from '@app/homeV3/context/hooks/useTemplateState';
import { PageTemplateContextState } from '@app/homeV3/context/types';

import { PageTemplateFragment } from '@graphql/template.generated';

const PageTemplateContext = createContext<PageTemplateContextState | undefined>(undefined);

export const PageTemplateProvider = ({
    personalTemplate: initialPersonalTemplate,
    globalTemplate: initialGlobalTemplate,
    children,
}: {
    personalTemplate: PageTemplateFragment | null | undefined;
    globalTemplate: PageTemplateFragment | null | undefined;
    children: ReactNode;
}) => {
    // Template state management
    const {
        personalTemplate,
        globalTemplate,
        template,
        isEditingGlobalTemplate,
        setIsEditingGlobalTemplate,
        setPersonalTemplate,
        setGlobalTemplate,
        setTemplate,
    } = useTemplateState(initialPersonalTemplate, initialGlobalTemplate);

    // Template operations
    const { updateTemplateWithModule, removeModuleFromTemplate, upsertTemplate } = useTemplateOperations();

    // Module operations
    const { addModule, removeModule, createModule } = useModuleOperations(
        isEditingGlobalTemplate,
        personalTemplate,
        globalTemplate,
        setPersonalTemplate,
        setGlobalTemplate,
        updateTemplateWithModule,
        removeModuleFromTemplate,
        upsertTemplate,
    );

    const createModuleModalState = useCreateModuleModalState();

    const value = useMemo(
        () => ({
            personalTemplate,
            globalTemplate,
            template,
            isEditingGlobalTemplate,
            setIsEditingGlobalTemplate,
            setPersonalTemplate,
            setGlobalTemplate,
            setTemplate,
            addModule,
            removeModule,
            createModule,
            createModuleModalState,
        }),
        [
            personalTemplate,
            globalTemplate,
            template,
            isEditingGlobalTemplate,
            setIsEditingGlobalTemplate,
            setPersonalTemplate,
            setGlobalTemplate,
            setTemplate,
            addModule,
            removeModule,
            createModule,
            createModuleModalState,
        ],
    );

    return <PageTemplateContext.Provider value={value}>{children}</PageTemplateContext.Provider>;
};

export function usePageTemplateContext() {
    const context = useContext(PageTemplateContext);
    if (!context) {
        throw new Error('usePageTemplateContext must be used within a PageTemplateProvider');
    }
    return context;
}

// Re-export types for convenience
export type { CreateModuleInput, AddModuleInput, RemoveModuleInput } from './types';
