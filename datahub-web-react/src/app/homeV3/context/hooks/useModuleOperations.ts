import { message } from 'antd';
import { useCallback } from 'react';

import { ModulePositionInput } from '@app/homeV3/template/types';

import { PageModuleFragment, PageTemplateFragment, useUpsertPageModuleMutation } from '@graphql/template.generated';
import { DataHubPageModuleType, EntityType, PageModuleScope } from '@types';

// Input types for the methods
export interface CreateModuleInput {
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

export interface RemoveModuleInput {
    moduleUrn: string;
    position: ModulePositionInput;
}

export function useModuleOperations(
    isEditingGlobalTemplate: boolean,
    personalTemplate: PageTemplateFragment | null,
    globalTemplate: PageTemplateFragment | null,
    setPersonalTemplate: (template: PageTemplateFragment | null) => void,
    setGlobalTemplate: (template: PageTemplateFragment | null) => void,
    updateTemplateWithModule: (
        templateToUpdate: PageTemplateFragment | null,
        module: PageModuleFragment,
        position: ModulePositionInput,
    ) => PageTemplateFragment | null,
    removeModuleFromTemplate: (
        templateToUpdate: PageTemplateFragment | null,
        moduleUrn: string,
        position: ModulePositionInput,
    ) => PageTemplateFragment | null,
    upsertTemplate: (
        templateToUpsert: PageTemplateFragment | null,
        isPersonal: boolean,
        personalTemplate: PageTemplateFragment | null,
    ) => Promise<any>,
) {
    const [upsertPageModuleMutation] = useUpsertPageModuleMutation();

    // Updates template state with a new module and updates the appropriate template on the backend
    const addModule = useCallback(
        (input: AddModuleInput) => {
            const { module, position } = input;

            // Determine which template to update
            const isPersonal = !isEditingGlobalTemplate;
            const templateToUpdate = isPersonal ? personalTemplate || globalTemplate : globalTemplate;

            if (!templateToUpdate) {
                console.error('No template provided to update');
                return;
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
            upsertTemplate(updatedTemplate, isPersonal, personalTemplate).catch((error) => {
                // Revert on error
                if (isPersonal) {
                    setPersonalTemplate(personalTemplate);
                } else {
                    setGlobalTemplate(globalTemplate);
                }
                console.error('Failed to update template:', error);
                message.error('Failed to update template', error);
            });
        },
        [
            isEditingGlobalTemplate,
            personalTemplate,
            globalTemplate,
            setPersonalTemplate,
            setGlobalTemplate,
            updateTemplateWithModule,
            upsertTemplate,
        ],
    );

    // Removes a module from the template state and updates the appropriate template on the backend
    const removeModule = useCallback(
        (input: RemoveModuleInput) => {
            const { moduleUrn, position } = input;

            // Determine which template to update
            const isPersonal = !isEditingGlobalTemplate;
            const templateToUpdate = isPersonal ? personalTemplate || globalTemplate : globalTemplate;

            if (!templateToUpdate) {
                console.error('No template provided to update');
                return;
            }

            // Update template state
            const updatedTemplate = removeModuleFromTemplate(templateToUpdate, moduleUrn, position);

            // Update local state immediately for optimistic UI
            if (isPersonal) {
                setPersonalTemplate(updatedTemplate);
            } else {
                setGlobalTemplate(updatedTemplate);
            }

            // Persist changes
            upsertTemplate(updatedTemplate, isPersonal, personalTemplate).catch((error) => {
                // Revert on error
                if (isPersonal) {
                    setPersonalTemplate(personalTemplate);
                } else {
                    setGlobalTemplate(globalTemplate);
                }
                console.error('Failed to remove module from template:', error);
                message.error('Failed to remove module from template', error);
            });
        },
        [
            isEditingGlobalTemplate,
            personalTemplate,
            globalTemplate,
            setPersonalTemplate,
            setGlobalTemplate,
            removeModuleFromTemplate,
            upsertTemplate,
        ],
    );

    // Takes input and makes a call to create a module then add that module to the template
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
                        console.error('Failed to create module');
                        message.error('Failed to create module');
                        return;
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
                    message.error('Failed to create module', error);
                });
        },
        [upsertPageModuleMutation, addModule],
    );

    return {
        addModule,
        removeModule,
        createModule,
    };
}
