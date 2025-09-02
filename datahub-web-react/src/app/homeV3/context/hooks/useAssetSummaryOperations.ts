import { message } from 'antd';
import { useCallback } from 'react';

import { StructuredPropertyFieldsFragment } from '@graphql/fragments.generated';
import { PageTemplateFragment, SummaryElementFragment } from '@graphql/template.generated';
import { SummaryElement, SummaryElementType } from '@types';

// Types for summary element operations
export interface SummaryElementWithId extends SummaryElement {
    id: string; // Unique identifier for React keys and operations
}

export interface AddSummaryElementInput {
    elementType: SummaryElementType;
    structuredProperty?: StructuredPropertyFieldsFragment;
}

export interface ReplaceSummaryElementInput {
    elementType: SummaryElementType;
    structuredProperty?: StructuredPropertyFieldsFragment;
    position: number;
}

// Helper functions for template operations
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

// Helper function to generate unique ID for summary elements
const generateSummaryElementId = (element: SummaryElement, index: number): string => {
    if (element.elementType === SummaryElementType.StructuredProperty && element.structuredProperty?.urn) {
        return `${element.elementType}-${element.structuredProperty.urn}`;
    }
    return `${element.elementType}-${index}`;
};

// Helper function to convert SummaryElement to SummaryElementWithId
const addIdToSummaryElement = (element: SummaryElement, index: number): SummaryElementWithId => ({
    ...element,
    id: generateSummaryElementId(element, index),
});

// Helper function to create a new summary element from input
const createSummaryElementFromInput = (input: AddSummaryElementInput): SummaryElementFragment => ({
    __typename: 'SummaryElement',
    elementType: input.elementType,
    structuredProperty: input.structuredProperty ? input.structuredProperty : undefined,
});

// Validation functions
const validateAddSummaryElementInput = (input: AddSummaryElementInput): string | null => {
    if (!input.elementType) {
        return 'Element type is required';
    }
    if (input.elementType === SummaryElementType.StructuredProperty && !input.structuredProperty?.urn) {
        return 'Structured property URN is required for STRUCTURED_PROPERTY element type';
    }
    return null;
};

const validateReplaceSummaryElementInput = (input: ReplaceSummaryElementInput): string | null => {
    const baseValidation = validateAddSummaryElementInput(input);
    if (baseValidation) return baseValidation;

    if (input.position < 0) {
        return 'Position must be non-negative';
    }
    return null;
};

const validateRemoveSummaryElementInput = (position: number): string | null => {
    if (position < 0) {
        return 'Position must be non-negative';
    }
    return null;
};

export function useAssetSummaryOperations(
    isEditingGlobalTemplate: boolean,
    personalTemplate: PageTemplateFragment | null,
    globalTemplate: PageTemplateFragment | null,
    setPersonalTemplate: (template: PageTemplateFragment | null) => void,
    setGlobalTemplate: (template: PageTemplateFragment | null) => void,
    upsertTemplate: (
        templateToUpsert: PageTemplateFragment | null,
        isPersonal: boolean,
        personalTemplate: PageTemplateFragment | null,
    ) => Promise<any>,
) {
    // Create context object to avoid passing many parameters
    const context: TemplateUpdateContext = {
        isEditingGlobalTemplate,
        personalTemplate,
        globalTemplate,
        setPersonalTemplate,
        setGlobalTemplate,
        upsertTemplate,
    };

    // Get current summary elements with IDs for easier manipulation
    const getSummaryElementsWithIds = useCallback((template: PageTemplateFragment | null): SummaryElementWithId[] => {
        const summaryElements = template?.properties?.assetSummary?.summaryElements || [];
        return summaryElements.map(addIdToSummaryElement);
    }, []);

    // Add a new summary element to the template
    const addSummaryElement = useCallback(
        (input: AddSummaryElementInput) => {
            // Validate input
            const validationError = validateAddSummaryElementInput(input);
            if (validationError) {
                console.error('Invalid addSummaryElement input:', validationError);
                message.error(validationError);
                return;
            }

            const { template: templateToUpdate, isPersonal } = getTemplateToUpdate(context);

            if (!templateToUpdate) {
                console.error('No template provided to update');
                message.error('No template available to update');
                return;
            }

            // Create new summary element
            const newSummaryElement = createSummaryElementFromInput(input);
            const currentSummaryElements = templateToUpdate.properties?.assetSummary?.summaryElements || [];
            const updatedSummaryElements = [...currentSummaryElements, newSummaryElement];

            // Create updated template
            const updatedTemplate: PageTemplateFragment = {
                ...templateToUpdate,
                properties: {
                    ...templateToUpdate.properties,
                    assetSummary: {
                        // __typename: 'DataHubPageTemplateAssetSummary',
                        summaryElements: updatedSummaryElements,
                    },
                },
            };

            // Update local state immediately for optimistic UI
            updateTemplateStateOptimistically(context, updatedTemplate, isPersonal);

            // Persist changes
            persistTemplateChanges(context, updatedTemplate, isPersonal, 'add summary element');
        },
        [context],
    );

    // Remove a summary element from the template
    const removeSummaryElement = useCallback(
        (position: number) => {
            // Validate input
            const validationError = validateRemoveSummaryElementInput(position);
            if (validationError) {
                console.error('Invalid removeSummaryElement input:', validationError);
                message.error(validationError);
                return;
            }

            const { template: templateToUpdate, isPersonal } = getTemplateToUpdate(context);

            if (!templateToUpdate) {
                console.error('No template provided to update');
                message.error('No template available to update');
                return;
            }

            const currentSummaryElements = templateToUpdate.properties?.assetSummary?.summaryElements || [];

            if (position >= currentSummaryElements.length) {
                console.error('Position is out of bounds');
                message.error('Invalid position for removal');
                return;
            }

            // Create updated summary elements array
            const updatedSummaryElements = [...currentSummaryElements];
            updatedSummaryElements.splice(position, 1);

            // Create updated template
            const updatedTemplate: PageTemplateFragment = {
                ...templateToUpdate,
                properties: {
                    ...templateToUpdate.properties,
                    assetSummary: {
                        // __typename: 'DataHubPageTemplateAssetSummary',
                        summaryElements: updatedSummaryElements,
                    },
                },
            };

            // Update local state immediately for optimistic UI
            updateTemplateStateOptimistically(context, updatedTemplate, isPersonal);

            // Persist changes
            persistTemplateChanges(context, updatedTemplate, isPersonal, 'remove summary element');
        },
        [context],
    );

    // Replace a summary element in the template
    const replaceSummaryElement = useCallback(
        (input: ReplaceSummaryElementInput) => {
            // Validate input
            const validationError = validateReplaceSummaryElementInput(input);
            if (validationError) {
                console.error('Invalid replaceSummaryElement input:', validationError);
                message.error(validationError);
                return;
            }

            const { template: templateToUpdate, isPersonal } = getTemplateToUpdate(context);

            if (!templateToUpdate) {
                console.error('No template provided to update');
                message.error('No template available to update');
                return;
            }

            const currentSummaryElements = templateToUpdate.properties?.assetSummary?.summaryElements || [];

            if (input.position >= currentSummaryElements.length) {
                console.error('Position is out of bounds');
                message.error('Invalid position for replacement');
                return;
            }

            // Create new summary element
            const newSummaryElement = createSummaryElementFromInput(input);

            // Create updated summary elements array
            const updatedSummaryElements = [...currentSummaryElements];
            updatedSummaryElements.splice(input.position, 1, newSummaryElement);

            // Create updated template
            const updatedTemplate: PageTemplateFragment = {
                ...templateToUpdate,
                properties: {
                    ...templateToUpdate.properties,
                    assetSummary: {
                        // __typename: 'DataHubPageTemplateAssetSummary',
                        summaryElements: updatedSummaryElements,
                    },
                },
            };

            // Update local state immediately for optimistic UI
            updateTemplateStateOptimistically(context, updatedTemplate, isPersonal);

            // Persist changes
            persistTemplateChanges(context, updatedTemplate, isPersonal, 'replace summary element');
        },
        [context],
    );

    return {
        addSummaryElement,
        removeSummaryElement,
        replaceSummaryElement,
        getSummaryElementsWithIds,
    };
}
