import { message } from 'antd';
import { PageTemplateFragment } from '@graphql/template.generated';

/**
 * Shared context interface for template operations
 */
export interface TemplateUpdateContext {
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

/**
 * Determines which template to update and whether it's personal
 */
export const getTemplateToUpdate = (
    context: TemplateUpdateContext,
): {
    template: PageTemplateFragment | null;
    isPersonal: boolean;
} => {
    const isPersonal = !context.isEditingGlobalTemplate;
    const template = isPersonal ? context.personalTemplate || context.globalTemplate : context.globalTemplate;

    return { template, isPersonal };
};

/**
 * Updates template state optimistically for immediate UI feedback
 */
export const updateTemplateStateOptimistically = (
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

/**
 * Persists template changes and handles errors with rollback
 */
export const persistTemplateChanges = async (
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

/**
 * Generic validation utility for handling validation errors
 */
export const handleValidationError = (validationError: string | null, operation: string): boolean => {
    if (validationError) {
        console.error(`Invalid ${operation} input:`, validationError);
        message.error(validationError);
        return true;
    }
    return false;
};

/**
 * Generic template availability check
 */
export const validateTemplateAvailability = (template: PageTemplateFragment | null): boolean => {
    if (!template) {
        console.error('No template provided to update');
        message.error('No template available to update');
        return false;
    }
    return true;
};
