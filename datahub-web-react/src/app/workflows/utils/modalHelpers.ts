import { useEntityRegistry } from '@app/useEntityRegistry';

import { ActionWorkflowFragment } from '@graphql/actionWorkflow.generated';
import { ActionWorkflowCategory, EntityType } from '@types';

export enum WorkflowRequestFormModalMode {
    CREATE = 'CREATE',
    REVIEW = 'REVIEW',
    EDIT = 'EDIT', // For future use
}

export interface ModalConfig {
    title: string;
    submitButtonText: string;
    cancelButtonText: string;
}

export interface FormStateForVisibility {
    [fieldId: string]: any[];
}

type EntityRegistry = ReturnType<typeof useEntityRegistry>;

/**
 * Get the appropriate entity selector title based on workflow entity types
 */
export const getEntitySelectorTitle = (workflow: ActionWorkflowFragment, entityRegistry: EntityRegistry): string => {
    if (workflow?.trigger?.form?.entityTypes?.length === 1) {
        return entityRegistry.getEntityName(workflow.trigger?.form?.entityTypes[0] as EntityType) || 'Entity';
    }
    return 'Asset';
};

/**
 * Transform form state into format expected by field visibility checks
 */
export const createFormValuesForVisibility = (
    formState: Record<string, { values?: any[] }>,
): FormStateForVisibility => {
    const values: FormStateForVisibility = {};
    Object.entries(formState).forEach(([fieldId, fieldState]) => {
        values[fieldId] = fieldState?.values || [];
    });
    return values;
};

/**
 * Get modal configuration based on mode and workflow
 */
export const getModalConfig = (mode: WorkflowRequestFormModalMode, workflow: ActionWorkflowFragment): ModalConfig => {
    switch (mode) {
        case WorkflowRequestFormModalMode.REVIEW:
            return {
                title: `Review ${workflow.name} Request`,
                submitButtonText: 'Submit',
                cancelButtonText: 'Close',
            };
        case WorkflowRequestFormModalMode.CREATE:
        default:
            return {
                title: `Create ${workflow.name} Request`,
                submitButtonText: 'Submit',
                cancelButtonText: 'Cancel',
            };
    }
};

/**
 * Determine if workflow should show access-specific fields
 */
export const shouldShowAccessFields = (workflow: ActionWorkflowFragment): boolean => {
    return workflow.category === ActionWorkflowCategory.Access;
};

/**
 * Get appropriate placeholder text based on mode and context
 */
export const getPlaceholderText = (mode: WorkflowRequestFormModalMode, defaultText: string): string => {
    return mode === WorkflowRequestFormModalMode.REVIEW ? 'No value provided' : defaultText;
};

/**
 * Check if entity selection is required and validate selection
 */
export const isEntitySelectionRequired = (workflow: ActionWorkflowFragment, selectedEntityUrn?: string): boolean => {
    const requiresEntitySelection =
        workflow.trigger?.form?.entityTypes && workflow.trigger?.form?.entityTypes?.length > 0;
    if (!requiresEntitySelection) {
        return false;
    }
    return !selectedEntityUrn || selectedEntityUrn.trim() === '';
};
