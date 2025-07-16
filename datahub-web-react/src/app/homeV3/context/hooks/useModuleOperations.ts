import { message } from 'antd';
import { useCallback } from 'react';



import { CreateModuleInput } from '@app/homeV3/context/types';
import { ModulePositionInput } from '@app/homeV3/template/types';

import { PageModuleFragment, PageTemplateFragment, useUpsertPageModuleMutation } from '@graphql/template.generated';
import { EntityType, PageModuleScope } from '@types';

export interface AddModuleInput {
    module: PageModuleFragment;
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
        isEditing: boolean,
    ) => PageTemplateFragment | null,
    upsertTemplate: (
        templateToUpsert: PageTemplateFragment | null,
        isPersonal: boolean,
        personalTemplate: PageTemplateFragment | null,
    ) => Promise<any>,
    isEditingModule: boolean,
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
            const updatedTemplate = updateTemplateWithModule(templateToUpdate, module, position, isEditingModule);

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
            updateTemplateWithModule,
            isEditingModule,
            upsertTemplate,
            setPersonalTemplate,
            setGlobalTemplate,
        ],
    );

    // Takes input and makes a call to create a module then add that module to the template
    const createModule = useCallback(
        (input: CreateModuleInput) => {
            const { name, type, scope = PageModuleScope.Personal, params = {}, position, urn } = input;

            // Create the module first
            const moduleInput = {
                name,
                type,
                scope,
                params,
                urn,
            };

            upsertPageModuleMutation({
                variables: { input: moduleInput },
            })
                .then((moduleResult) => {
                    const moduleUrn = moduleResult.data?.upsertPageModule?.urn;
                    if (!moduleUrn) {
                        console.error(`Failed to ${isEditingModule ? 'update' : 'create'} module:`);
                        message.error(`Failed to ${isEditingModule ? 'update' : 'create'} module`);
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
                    console.error(`Failed to ${isEditingModule ? 'update' : 'create'} module:`, error);
                    message.error(`Failed to ${isEditingModule ? 'update' : 'create'} module`, error);
                });
        },
        [upsertPageModuleMutation, addModule, isEditingModule],
    );

    return {
        addModule,
        createModule,
    };
}