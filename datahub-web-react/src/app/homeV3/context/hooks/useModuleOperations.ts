import { message } from 'antd';
import { useCallback, useMemo } from 'react';

import { UpsertModuleInput } from '@app/homeV3/context/types';
import { ModulePositionInput } from '@app/homeV3/template/types';

import { PageModuleFragment, PageTemplateFragment, useUpsertPageModuleMutation } from '@graphql/template.generated';
import { EntityType, PageModuleScope } from '@types';

export interface AddModuleInput {
    module: PageModuleFragment;
    position: ModulePositionInput;
}

export interface RemoveModuleInput {
    moduleUrn: string;
    position: ModulePositionInput;
}

// Helper types for shared operations
interface TemplateUpdateContext {
    isEditingGlobalTemplate: boolean;
    personalTemplate: PageTemplateFragment | null;
    globalTemplate: PageTemplateFragment | null;
    setPersonalTemplate: (template: PageTemplateFragment | null) => void;
    setGlobalTemplate: (template: PageTemplateFragment | null) => void;
    upsertTemplate: (
        templateToUpsert: PageTemplateFragment | null,
        isPersonal: boolean,
        personalTemplate: PageTemplateFragment | null,
    ) => Promise<any>;
}

// Helper functions for shared template operations
const getTemplateToUpdate = (
    context: TemplateUpdateContext,
): {
    template: PageTemplateFragment | null;
    isPersonal: boolean;
} => {
    const isPersonal = !context.isEditingGlobalTemplate;
    const template = isPersonal ? context.personalTemplate || context.globalTemplate : context.globalTemplate;

    return { template, isPersonal };
};

const updateTemplateStateOptimistically = (
    context: TemplateUpdateContext,
    updatedTemplate: PageTemplateFragment | null,
    isPersonal: boolean,
) => {
    if (isPersonal) {
        context.setPersonalTemplate(updatedTemplate);
    } else {
        context.setGlobalTemplate(updatedTemplate);
    }
};

const persistTemplateChanges = async (
    context: TemplateUpdateContext,
    updatedTemplate: PageTemplateFragment | null,
    isPersonal: boolean,
    operationName: string,
) => {
    try {
        await context.upsertTemplate(updatedTemplate, isPersonal, context.personalTemplate);
    } catch (error) {
        // Revert on error
        if (isPersonal) {
            context.setPersonalTemplate(context.personalTemplate);
        } else {
            context.setGlobalTemplate(context.globalTemplate);
        }
        console.error(`Failed to ${operationName}:`, error);
        message.error(`Failed to ${operationName}`);
    }
};

// Helper functions for input validation
const validateAddModuleInput = (input: AddModuleInput): string | null => {
    if (!input.module?.urn) {
        return 'Module URN is required';
    }
    if (!input.module?.properties?.type) {
        return 'Module type is required';
    }
    if (!input.position) {
        return 'Module position is required';
    }
    return null;
};

const validateRemoveModuleInput = (input: RemoveModuleInput): string | null => {
    if (!input.moduleUrn) {
        return 'Module URN is required for removal';
    }
    if (!input.position) {
        return 'Module position is required for removal';
    }
    if (input.position.rowIndex === undefined || input.position.rowIndex < 0) {
        return 'Valid row index is required for removal';
    }
    return null;
};

const validateUpsertModuleInput = (input: UpsertModuleInput): string | null => {
    if (!input.name?.trim()) {
        return 'Module name is required';
    }
    if (!input.type) {
        return 'Module type is required';
    }
    if (!input.position) {
        return 'Module position is required';
    }
    return null;
};

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
    isEditingModule: boolean,
) {
    const [upsertPageModuleMutation] = useUpsertPageModuleMutation();

    // Create context object to avoid passing many parameters
    const context: TemplateUpdateContext = useMemo(
        () => ({
            isEditingGlobalTemplate,
            personalTemplate,
            globalTemplate,
            setPersonalTemplate,
            setGlobalTemplate,
            upsertTemplate,
        }),
        [
            isEditingGlobalTemplate,
            personalTemplate,
            globalTemplate,
            setPersonalTemplate,
            setGlobalTemplate,
            upsertTemplate,
        ],
    );

    // Updates template state with a new module and updates the appropriate template on the backend
    const addModule = useCallback(
        (input: AddModuleInput) => {
            // Validate input
            const validationError = validateAddModuleInput(input);
            if (validationError) {
                console.error('Invalid addModule input:', validationError);
                message.error(validationError);
                return;
            }

            const { module, position } = input;
            const { template: templateToUpdate, isPersonal } = getTemplateToUpdate(context);

            if (!templateToUpdate) {
                console.error('No template provided to update');
                message.error('No template available to update');
                return;
            }

            // Update template state
            const updatedTemplate = updateTemplateWithModule(templateToUpdate, module, position, isEditingModule);

            // Update local state immediately for optimistic UI
            updateTemplateStateOptimistically(context, updatedTemplate, isPersonal);

            // Persist changes
            persistTemplateChanges(context, updatedTemplate, isPersonal, 'add module');
        },
        [context, isEditingModule, updateTemplateWithModule],
    );

    // Removes a module from the template state and updates the appropriate template on the backend
    const removeModule = useCallback(
        (input: RemoveModuleInput) => {
            // Validate input
            const validationError = validateRemoveModuleInput(input);
            if (validationError) {
                console.error('Invalid removeModule input:', validationError);
                message.error(validationError);
                return;
            }

            const { moduleUrn, position } = input;
            const { template: templateToUpdate, isPersonal } = getTemplateToUpdate(context);

            if (!templateToUpdate) {
                console.error('No template provided to update');
                message.error('No template available to update');
                return;
            }

            // Update template state
            const updatedTemplate = removeModuleFromTemplate(templateToUpdate, moduleUrn, position);

            // Update local state immediately for optimistic UI
            updateTemplateStateOptimistically(context, updatedTemplate, isPersonal);

            // Persist changes
            persistTemplateChanges(context, updatedTemplate, isPersonal, 'remove module');
        },
        [context, removeModuleFromTemplate],
    );

    // Takes input and makes a call to create a module then add that module to the template
    const upsertModule = useCallback(
        (input: UpsertModuleInput) => {
            // Validate input
            const validationError = validateUpsertModuleInput(input);
            if (validationError) {
                console.error('Invalid upsertModule input:', validationError);
                message.error(validationError);
                return;
            }

            const { name, type, scope = PageModuleScope.Personal, params = {}, position, urn } = input;

            // Create the module first
            const moduleInput = {
                name: name.trim(),
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
                        console.error(`Failed to ${isEditingModule ? 'update' : 'create'} module - no URN returned`);
                        message.error(`Failed to ${isEditingModule ? 'update' : 'create'} module`);
                        return;
                    }

                    // Create a module fragment for optimistic UI
                    const moduleFragment: PageModuleFragment = {
                        urn: moduleUrn,
                        type: EntityType.DatahubPageModule,
                        properties: {
                            name: name.trim(),
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
                    message.error(`Failed to ${isEditingModule ? 'update' : 'create'} module`);
                });
        },
        [upsertPageModuleMutation, addModule, isEditingModule],
    );

    return {
        addModule,
        removeModule,
        upsertModule,
    };
}
