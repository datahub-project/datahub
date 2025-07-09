import React, { ReactNode, createContext, useCallback, useContext, useMemo, useState } from 'react';

import { ModuleInfo } from '@app/homeV3/modules/types';
import { AddModuleHandlerInput } from '@app/homeV3/template/types';

import {
    PageModuleFragment,
    PageTemplateFragment,
    useUpsertPageModuleMutation,
    useUpsertPageTemplateMutation,
} from '@graphql/template.generated';
import { useUpdateUserHomePageSettingsMutation } from '@graphql/user.generated';
import { DataHubPageModuleType, EntityType, PageModuleScope, PageTemplateScope, PageTemplateSurfaceType } from '@types';

// Input types for the methods
export interface CreateModuleInput {
    name: string;
    type: DataHubPageModuleType;
    scope?: PageModuleScope;
    params?: any; // Module-specific parameters
    position: AddModuleHandlerInput;
}

export interface AddModuleInput {
    module: PageModuleFragment;
    position: AddModuleHandlerInput;
}

// Context state shape
type PageTemplateContextState = {
    personalTemplate: PageTemplateFragment | null;
    globalTemplate: PageTemplateFragment | null;
    template: PageTemplateFragment | null;
    isEditingGlobalTemplate: boolean;
    setIsEditingGlobalTemplate: (val: boolean) => void;
    setPersonalTemplate: (template: PageTemplateFragment | null) => void;
    setGlobalTemplate: (template: PageTemplateFragment | null) => void;
    setTemplate: (template: PageTemplateFragment | null) => void;
    addModule: (input: AddModuleInput) => void;
    createModule: (input: CreateModuleInput) => void;
};

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
    const [personalTemplate, setPersonalTemplate] = useState<PageTemplateFragment | null>(
        initialPersonalTemplate || null,
    );
    const [globalTemplate, setGlobalTemplate] = useState<PageTemplateFragment | null>(initialGlobalTemplate || null);
    const [isEditingGlobalTemplate, setIsEditingGlobalTemplate] = useState(false);

    const [upsertPageTemplateMutation] = useUpsertPageTemplateMutation();
    const [upsertPageModuleMutation] = useUpsertPageModuleMutation();
    const [updateUserHomePageSettings] = useUpdateUserHomePageSettingsMutation();

    // The current template is personal unless editing global or personal is missing
    const template = isEditingGlobalTemplate ? globalTemplate : personalTemplate || globalTemplate;

    // Helper function to update template state with a new module
    const updateTemplateWithModule = useCallback(
        (
            templateToUpdate: PageTemplateFragment | null,
            module: PageModuleFragment,
            position: AddModuleHandlerInput,
        ): PageTemplateFragment | null => {
            if (!templateToUpdate) return null;

            const newTemplate = { ...templateToUpdate };
            const newRows = [...(newTemplate.properties?.rows || [])];

            if (position.rowIndex === undefined) {
                // Add to new row at the end
                newRows.push({
                    modules: [module],
                });
            } else {
                // Add to existing row
                const rowIndex = position.rowIndex;
                if (rowIndex >= newRows.length) {
                    // Create new row if index is out of bounds
                    newRows.push({
                        modules: [module],
                    });
                } else {
                    const row = { ...newRows[rowIndex] };
                    const newModules = [...(row.modules || [])];

                    if (position.rowSide === 'left') {
                        newModules.unshift(module);
                    } else {
                        newModules.push(module);
                    }

                    row.modules = newModules;
                    newRows[rowIndex] = row;
                }
            }

            newTemplate.properties = {
                ...newTemplate.properties,
                rows: newRows,
            };

            return newTemplate;
        },
        [],
    );

    // Helper function to upsert template
    const upsertTemplate = useCallback(
        (templateToUpsert: PageTemplateFragment | null, isPersonal: boolean) => {
            if (!templateToUpsert) {
                throw new Error('Template is required for upsert');
            }

            const isCreatingPersonalTemplate = isPersonal && !personalTemplate;

            const input = {
                urn: isCreatingPersonalTemplate ? undefined : templateToUpsert.urn || undefined, // undefined for create
                rows:
                    templateToUpsert.properties?.rows?.map((row) => ({
                        modules: row.modules?.map((module) => module.urn) || [],
                    })) || [],
                scope: isPersonal ? PageTemplateScope.Personal : PageTemplateScope.Global,
                surfaceType: PageTemplateSurfaceType.HomePage,
            };

            return upsertPageTemplateMutation({
                variables: { input },
            }).then(({ data }) => {
                if (isCreatingPersonalTemplate && data?.upsertPageTemplate.urn) {
                    updateUserHomePageSettings({ variables: { input: { pageTemplate: data.upsertPageTemplate.urn } } });
                }
            });
        },
        [upsertPageTemplateMutation],
    );

    const addModule = useCallback(
        (input: AddModuleInput) => {
            const { module, position } = input;

            // Determine which template to update
            const isPersonal = !isEditingGlobalTemplate;
            const templateToUpdate = isPersonal ? personalTemplate || globalTemplate : globalTemplate;

            if (!templateToUpdate) {
                throw new Error('No template available to update');
            }

            // Update template state
            const updatedTemplate = updateTemplateWithModule(templateToUpdate, module, position);

            // Update local state immediately for optimistic UI
            if (isPersonal) {
                setPersonalTemplate(updatedTemplate);
            } else {
                setGlobalTemplate(updatedTemplate);
            }

            // Persist changes
            upsertTemplate(updatedTemplate, isPersonal).catch((error) => {
                // Revert on error
                if (isPersonal) {
                    setPersonalTemplate(personalTemplate);
                } else {
                    setGlobalTemplate(globalTemplate);
                }
                console.error('Failed to update template:', error);
                throw error;
            });
        },
        [isEditingGlobalTemplate, personalTemplate, globalTemplate, updateTemplateWithModule, upsertTemplate],
    );

    const createModule = useCallback(
        (input: CreateModuleInput) => {
            const { name, type, scope = PageModuleScope.Personal, params = {}, position } = input;

            // Create the module first
            const moduleInput = {
                name,
                type,
                scope,
                visibility: {
                    scope,
                },
                params,
            };

            upsertPageModuleMutation({
                variables: { input: moduleInput },
            })
                .then((moduleResult) => {
                    const moduleUrn = moduleResult.data?.upsertPageModule?.urn;
                    if (!moduleUrn) {
                        throw new Error('Failed to create module');
                    }

                    // Create a module fragment for optimistic UI
                    const moduleFragment: PageModuleFragment = {
                        urn: moduleUrn,
                        type: EntityType.DatahubPageModule,
                        properties: {
                            name,
                            type,
                            visibility: { scope },
                            params: params || {},
                        },
                    };

                    // Now add the module to the template
                    addModule({
                        module: moduleFragment,
                        position,
                    });
                })
                .catch((error) => {
                    console.error('Failed to create module:', error);
                    throw error;
                });
        },
        [upsertPageModuleMutation, addModule],
    );

    const setTemplate = (t: PageTemplateFragment | null) => {
        if (isEditingGlobalTemplate) {
            setGlobalTemplate(t);
        } else {
            setPersonalTemplate(t);
        }
    };

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
        [personalTemplate, globalTemplate, template, isEditingGlobalTemplate, addModule, createModule],
    );

    return <PageTemplateContext.Provider value={value}>{children}</PageTemplateContext.Provider>;
};

export function usePageTemplateContext() {
    const ctx = useContext(PageTemplateContext);
    if (!ctx) throw new Error('usePageTemplateContext must be used within a PageTemplateProvider');
    return ctx;
}
