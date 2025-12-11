/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React, { ReactNode, createContext, useContext, useMemo } from 'react';

import { useAssetSummaryOperations } from '@app/homeV3/context/hooks/useAssetSummaryOperations';
import useIsTemplateEditable from '@app/homeV3/context/hooks/useIsTemplateEditable';
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
    const isTemplateEditable = useIsTemplateEditable(templateType);
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
