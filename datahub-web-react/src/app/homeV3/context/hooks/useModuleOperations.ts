import { message } from 'antd';
import { useCallback, useMemo } from 'react';

import {
    calculateAdjustedRowIndex,
    insertModuleIntoRows,
    removeModuleFromRows,
    validateModuleMoveConstraints,
} from '@app/homeV3/context/hooks/utils/moduleOperationsUtils';
import { AddModuleInput, MoveModuleInput, RemoveModuleInput, UpsertModuleInput } from '@app/homeV3/context/types';
import { DEFAULT_MODULE_URNS } from '@app/homeV3/modules/constants';
import { ModulePositionInput } from '@app/homeV3/template/types';

import {
    PageModuleFragment,
    PageTemplateFragment,
    useDeletePageModuleMutation,
    useUpsertPageModuleMutation,
} from '@graphql/template.generated';
import { EntityType, PageModuleScope } from '@types';

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
    if (!input.module.urn) {
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

const validateMoveModuleInput = (input: MoveModuleInput): string | null => {
    if (!input.module?.urn) {
        return 'Module URN is required for move operation';
    }
    if (!input.fromPosition) {
        return 'From position is required for move operation';
    }
    if (!input.toPosition) {
        return 'To position is required for move operation';
    }
    if (input.fromPosition.rowIndex === undefined || input.fromPosition.moduleIndex === undefined) {
        return 'Valid from position indices are required for move operation';
    }
    if (input.toPosition.rowIndex === undefined) {
        return 'Valid to position row index is required for move operation';
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
    const [deletePageModule] = useDeletePageModuleMutation();

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

    // Simplified helper function to move a module within or between rows in a template
    const moveModuleInTemplate = useCallback(
        (
            template: PageTemplateFragment | null,
            module: PageModuleFragment,
            fromPosition: ModulePositionInput,
            toPosition: ModulePositionInput,
            insertNewRow?: boolean,
        ): PageTemplateFragment | null => {
            if (!template) return null;

            const { rowIndex: toRowIndex } = toPosition;
            if (toRowIndex === undefined) {
                console.error('Target row index is required for move operation');
                return null;
            }

            // Step 1: Remove module from original position
            const { updatedRows, wasRowRemoved } = removeModuleFromRows(template.properties?.rows, fromPosition);

            // Step 2: Calculate adjusted target row index
            const adjustedRowIndex = calculateAdjustedRowIndex(fromPosition, toRowIndex, wasRowRemoved);

            // Step 3: Insert module into new position
            const finalRows = insertModuleIntoRows(updatedRows, module, toPosition, adjustedRowIndex, insertNewRow);

            // Step 4: Return updated template
            return {
                ...template,
                properties: {
                    ...template.properties,
                    rows: finalRows,
                },
            };
        },
        [],
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

            const { module, position } = input;
            const { template: templateToUpdate, isPersonal } = getTemplateToUpdate(context);

            if (!templateToUpdate) {
                console.error('No template provided to update');
                message.error('No template available to update');
                return;
            }

            // Update template state
            const updatedTemplate = removeModuleFromTemplate(templateToUpdate, module.urn, position);

            // Update local state immediately for optimistic UI
            updateTemplateStateOptimistically(context, updatedTemplate, isPersonal);

            // Persist changes
            persistTemplateChanges(context, updatedTemplate, isPersonal, 'remove module');

            // Delete module if necessary
            // Only do not delete a module when removing a global module from personal template OR module is a default
            const shouldNotDeleteModule =
                (!context.isEditingGlobalTemplate && module.properties.visibility.scope === PageModuleScope.Global) ||
                DEFAULT_MODULE_URNS.includes(module.urn);
            if (!shouldNotDeleteModule) {
                deletePageModule({ variables: { input: { urn: module.urn } } });
            }
        },
        [context, removeModuleFromTemplate, deletePageModule],
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

            const defaultScope = isEditingGlobalTemplate ? PageModuleScope.Global : PageModuleScope.Personal;
            const { name, type, scope = defaultScope, params = {}, position, urn } = input;

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
        [upsertPageModuleMutation, addModule, isEditingModule, isEditingGlobalTemplate],
    );

    // Simplified move module function with extracted validation and orchestration
    const moveModule = useCallback(
        (input: MoveModuleInput) => {
            // Validate input
            const validationError = validateMoveModuleInput(input);
            if (validationError) {
                console.error('Invalid moveModule input:', validationError);
                message.error(validationError);
                return;
            }

            const { module, fromPosition, toPosition, insertNewRow } = input;
            const { template: templateToUpdate, isPersonal } = getTemplateToUpdate(context);

            if (!templateToUpdate) {
                console.error('No template provided to update');
                message.error('No template available to update');
                return;
            }

            // Validate move constraints
            const constraintError = validateModuleMoveConstraints(templateToUpdate, fromPosition, toPosition);
            if (constraintError) {
                console.warn(`Move validation failed: ${constraintError}`);
                message.warning(constraintError);
                return;
            }

            // Execute the move operation
            const updatedTemplate = moveModuleInTemplate(
                templateToUpdate,
                module,
                fromPosition,
                toPosition,
                insertNewRow,
            );

            if (!updatedTemplate) {
                console.error('Failed to update template during move operation');
                message.error('Failed to move module');
                return;
            }

            // Update local state immediately for optimistic UI
            updateTemplateStateOptimistically(context, updatedTemplate, isPersonal);

            // Persist changes
            persistTemplateChanges(context, updatedTemplate, isPersonal, 'move module');
        },
        [context, moveModuleInTemplate],
    );

    return {
        addModule,
        removeModule,
        upsertModule,
        moveModule,
    };
}
