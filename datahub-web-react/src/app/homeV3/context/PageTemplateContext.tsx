import React, { ReactNode, createContext, useContext, useMemo } from 'react';

import { useAssetSummaryOperations } from '@app/homeV3/context/hooks/useAssetSummaryOperations';
import { useModuleModalState } from '@app/homeV3/context/hooks/useModuleModalState';
import { useModuleOperations } from '@app/homeV3/context/hooks/useModuleOperations';
import { useTemplateOperations } from '@app/homeV3/context/hooks/useTemplateOperations';
import { useTemplateState } from '@app/homeV3/context/hooks/useTemplateState';
import { PageTemplateContextState } from '@app/homeV3/context/types';

import { PageTemplateSurfaceType } from '@types';

const PageTemplateContext = createContext<PageTemplateContextState | undefined>(undefined);

interface Props {
    children: ReactNode;
    templateType: PageTemplateSurfaceType;
}

export const PageTemplateProvider = ({ children, templateType }: Props) => {
    const isTemplateEditable = false; // template is not editable in OSS
    // Template state management
    const {
        personalTemplate,
        globalTemplate,
        template,
        isEditingGlobalTemplate,
        summaryElements,
        setIsEditingGlobalTemplate,
        setPersonalTemplate,
        setGlobalTemplate,
        setTemplate,
    } = useTemplateState(templateType);

    // Template operations
    const { updateTemplateWithModule, removeModuleFromTemplate, upsertTemplate, resetTemplateToDefault } =
        useTemplateOperations(setPersonalTemplate, personalTemplate, templateType);

    // Modal state
    const moduleModalState = useModuleModalState(templateType);

    // Module operations
    const { addModule, removeModule, upsertModule, moveModule, moduleContext } = useModuleOperations(
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
        templateType,
    );

    // Asset summary operations
    const { addSummaryElement, removeSummaryElement, replaceSummaryElement } = useAssetSummaryOperations(
        isEditingGlobalTemplate,
        personalTemplate,
        globalTemplate,
        setPersonalTemplate,
        setGlobalTemplate,
        upsertTemplate,
    );

    const value = useMemo(
        () => ({
            isTemplateEditable,
            personalTemplate,
            globalTemplate,
            template,
            templateType,
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
            moduleContext,
            // Asset summary operations
            summaryElements,
            addSummaryElement,
            removeSummaryElement,
            replaceSummaryElement,
        }),
        [
            isTemplateEditable,
            personalTemplate,
            globalTemplate,
            template,
            templateType,
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
            moduleContext,
            // Asset summary operations
            summaryElements,
            addSummaryElement,
            removeSummaryElement,
            replaceSummaryElement,
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
