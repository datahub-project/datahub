import React, { ReactNode, createContext, useContext, useMemo } from 'react';

import { useModuleModalState } from '@app/homeV3/context/hooks/useModuleModalState';
import { useModuleOperations } from '@app/homeV3/context/hooks/useModuleOperations';
import { useTemplateOperations } from '@app/homeV3/context/hooks/useTemplateOperations';
import { useTemplateState } from '@app/homeV3/context/hooks/useTemplateState';
import { PageTemplateContextState } from '@app/homeV3/context/types';

const PageTemplateContext = createContext<PageTemplateContextState | undefined>(undefined);

export const PageTemplateProvider = ({ children }: { children: ReactNode }) => {
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
    } = useTemplateState();

    // Template operations
    const { updateTemplateWithModule, removeModuleFromTemplate, upsertTemplate, resetTemplateToDefault } =
        useTemplateOperations(setPersonalTemplate);

    // Modal state
    const moduleModalState = useModuleModalState();

    // Module operations
    const { addModule, removeModule, upsertModule, moveModule } = useModuleOperations(
        isEditingGlobalTemplate,
        personalTemplate,
        globalTemplate,
        setPersonalTemplate,
        setGlobalTemplate,
        updateTemplateWithModule,
        removeModuleFromTemplate,
        upsertTemplate,
        moduleModalState.isEditing,
        moduleModalState.initialState,
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
            removeModule,
            upsertModule,
            moduleModalState,
            moveModule,
            resetTemplateToDefault,
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
            upsertModule,
            moduleModalState,
            moveModule,
            resetTemplateToDefault,
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
