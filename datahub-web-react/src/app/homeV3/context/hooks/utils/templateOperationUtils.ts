/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
