import { message } from 'antd';
import i18next from 'i18next';

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

export type TemplateOperation =
    | 'addModule'
    | 'removeModule'
    | 'moveModule'
    | 'createModule'
    | 'updateModule'
    | 'replaceGlobalModule'
    | 'addSummaryElement'
    | 'removeSummaryElement'
    | 'replaceSummaryElement';

export const getOperationErrorMessage = (operation: TemplateOperation): string => {
    switch (operation) {
        case 'addModule':
            return i18next.t('modules:error.failedToAddModule');
        case 'removeModule':
            return i18next.t('modules:error.failedToRemoveModule');
        case 'moveModule':
            return i18next.t('modules:error.failedToMoveModule');
        case 'createModule':
            return i18next.t('modules:error.failedToCreateModule');
        case 'updateModule':
            return i18next.t('modules:error.failedToUpdateModule');
        case 'replaceGlobalModule':
            return i18next.t('modules:error.failedToReplaceGlobalModule');
        case 'addSummaryElement':
            return i18next.t('home.v3:error.failedToAddSummaryElement');
        case 'removeSummaryElement':
            return i18next.t('home.v3:error.failedToRemoveSummaryElement');
        case 'replaceSummaryElement':
            return i18next.t('home.v3:error.failedToReplaceSummaryElement');
        default:
            return i18next.t('modules:error.noTemplateAvailable');
    }
};

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
    operation: TemplateOperation,
) => {
    try {
        await context.upsertTemplate(updatedTemplate, isPersonal, context.personalTemplate);
    } catch (error) {
        if (isPersonal) {
            context.setPersonalTemplate(context.personalTemplate);
        } else {
            context.setGlobalTemplate(context.globalTemplate);
        }
        console.error(`Failed to ${operation}:`, error);
        message.error(getOperationErrorMessage(operation));
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
export const validateTemplateAvailability = (template: PageTemplateFragment | null, errorMessage: string): boolean => {
    if (!template) {
        console.error('No template provided to update');
        message.error(errorMessage);
        return false;
    }
    return true;
};
