import React, { ReactNode, createContext, useContext, useMemo } from 'react';

import { PageTemplateFragment } from '@graphql/template.generated';

import { useModuleOperations } from './hooks/useModuleOperations';
import { useTemplateOperations } from './hooks/useTemplateOperations';
import { useTemplateState } from './hooks/useTemplateState';
import { PageTemplateContextState } from './types';

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
    const { updateTemplateWithModule, upsertTemplate } = useTemplateOperations();

    // Module operations
    const { addModule, createModule } = useModuleOperations(
        isEditingGlobalTemplate,
        personalTemplate,
        globalTemplate,
        setPersonalTemplate,
        setGlobalTemplate,
        updateTemplateWithModule,
        upsertTemplate,
    );

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
            createModule,
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
            createModule,
        ],
    );

    return <PageTemplateContext.Provider value={value}>{children}</PageTemplateContext.Provider>;
};

export function usePageTemplateContext() {
    const ctx = useContext(PageTemplateContext);
    if (!ctx) throw new Error('usePageTemplateContext must be used within a PageTemplateProvider');
    return ctx;
}

// Re-export types for convenience
export type { CreateModuleInput, AddModuleInput } from './types';
