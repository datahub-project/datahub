import { useCallback, useMemo } from 'react';

import analytics, { EventType } from '@app/analytics';
import {
    TemplateUpdateContext,
    getTemplateToUpdate,
    handleValidationError,
    persistTemplateChanges,
    updateTemplateStateOptimistically,
    validateTemplateAvailability,
} from '@app/homeV3/context/hooks/utils/templateOperationUtils';
import {
    validateArrayBounds,
    validateElementType,
    validatePosition,
    validateStructuredProperty,
} from '@app/homeV3/context/hooks/utils/validationUtils';

import { StructuredPropertyFieldsFragment } from '@graphql/fragments.generated';
import { PageTemplateFragment, SummaryElementFragment } from '@graphql/template.generated';
import { SummaryElementType } from '@types';

export interface AddSummaryElementInput {
    elementType: SummaryElementType;
    structuredProperty?: StructuredPropertyFieldsFragment;
}

export interface ReplaceSummaryElementInput {
    elementType: SummaryElementType;
    structuredProperty?: StructuredPropertyFieldsFragment;
    position: number;
    currentElementType: SummaryElementType;
}

// Helper function to create a new summary element from input
const createSummaryElementFromInput = (input: AddSummaryElementInput): SummaryElementFragment => ({
    __typename: 'SummaryElement',
    elementType: input.elementType,
    structuredProperty: input.structuredProperty ? input.structuredProperty : undefined,
});

// Validation functions
const validateAddSummaryElementInput = (input: AddSummaryElementInput): string | null => {
    const elementTypeError = validateElementType(input.elementType);
    if (elementTypeError) return elementTypeError;

    const structuredPropertyError = validateStructuredProperty(input.elementType, input.structuredProperty?.urn);
    if (structuredPropertyError) return structuredPropertyError;

    return null;
};

const validateReplaceSummaryElementInput = (input: ReplaceSummaryElementInput): string | null => {
    const baseValidation = validateAddSummaryElementInput(input);
    if (baseValidation) return baseValidation;

    return validatePosition(input.position, 'replace summary element');
};

const validateRemoveSummaryElementInput = (position: number): string | null => {
    return validatePosition(position, 'remove summary element');
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

    // Add a new summary element to the template
    const addSummaryElement = useCallback(
        (input: AddSummaryElementInput) => {
            // Validate input
            const validationError = validateAddSummaryElementInput(input);
            if (handleValidationError(validationError, 'addSummaryElement')) return;

            const { template: templateToUpdate, isPersonal } = getTemplateToUpdate(context);
            if (!validateTemplateAvailability(templateToUpdate) || !templateToUpdate) return;

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
                        summaryElements: updatedSummaryElements,
                    },
                },
            };

            // Update local state immediately for optimistic UI
            updateTemplateStateOptimistically(context, updatedTemplate, isPersonal);

            // Persist changes
            persistTemplateChanges(context, updatedTemplate, isPersonal, 'add summary element');

            analytics.event({
                type: EventType.AssetPageAddSummaryElement,
                templateUrn: templateToUpdate.urn,
                elementType: input.elementType,
            });
        },
        [context],
    );

    // Remove a summary element from the template
    const removeSummaryElement = useCallback(
        (position: number, elementType: SummaryElementType) => {
            // Validate input
            const validationError = validateRemoveSummaryElementInput(position);
            if (handleValidationError(validationError, 'removeSummaryElement')) return;

            const { template: templateToUpdate, isPersonal } = getTemplateToUpdate(context);
            if (!validateTemplateAvailability(templateToUpdate) || !templateToUpdate) return;

            const currentSummaryElements = templateToUpdate.properties?.assetSummary?.summaryElements || [];

            const boundsError = validateArrayBounds(position, currentSummaryElements.length, 'removal');
            if (handleValidationError(boundsError, 'removeSummaryElement')) return;

            // Create updated summary elements array
            const updatedSummaryElements = [...currentSummaryElements];
            updatedSummaryElements.splice(position, 1);

            // Create updated template
            const updatedTemplate: PageTemplateFragment = {
                ...templateToUpdate,
                properties: {
                    ...templateToUpdate.properties,
                    assetSummary: {
                        summaryElements: updatedSummaryElements,
                    },
                },
            };

            // Update local state immediately for optimistic UI
            updateTemplateStateOptimistically(context, updatedTemplate, isPersonal);

            // Persist changes
            persistTemplateChanges(context, updatedTemplate, isPersonal, 'remove summary element');

            analytics.event({
                type: EventType.AssetPageRemoveSummaryElement,
                templateUrn: templateToUpdate.urn,
                elementType,
            });
        },
        [context],
    );

    // Replace a summary element in the template
    const replaceSummaryElement = useCallback(
        (input: ReplaceSummaryElementInput) => {
            // Validate input
            const validationError = validateReplaceSummaryElementInput(input);
            if (handleValidationError(validationError, 'replaceSummaryElement')) return;

            const { template: templateToUpdate, isPersonal } = getTemplateToUpdate(context);
            if (!validateTemplateAvailability(templateToUpdate) || !templateToUpdate) return;

            const currentSummaryElements = templateToUpdate.properties?.assetSummary?.summaryElements || [];

            const boundsError = validateArrayBounds(input.position, currentSummaryElements.length, 'replacement');
            if (handleValidationError(boundsError, 'replaceSummaryElement')) return;

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
                        summaryElements: updatedSummaryElements,
                    },
                },
            };

            // Update local state immediately for optimistic UI
            updateTemplateStateOptimistically(context, updatedTemplate, isPersonal);

            // Persist changes
            persistTemplateChanges(context, updatedTemplate, isPersonal, 'replace summary element');

            analytics.event({
                type: EventType.AssetPageReplaceSummaryElement,
                templateUrn: templateToUpdate.urn,
                currentElementType: input.currentElementType,
                newElementType: input.elementType,
            });
        },
        [context],
    );

    return {
        addSummaryElement,
        removeSummaryElement,
        replaceSummaryElement,
    };
}
