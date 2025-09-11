import {
    SCHEMA_FIELD_STRUCTURED_PROPERTY_REFERENCE_PLACEHOLDER_ID,
    STRUCTURED_PROPERTY_REFERENCE_PLACEHOLDER_ID,
} from '@app/tests/builder/steps/definition/builder/property/constants';
import { ActionType } from '@app/tests/builder/steps/definition/builder/property/types/action';
import {
    isOwnershipTypeId,
    isSchemaFieldStructuredPropertyId,
    isStructuredPropertyId,
} from '@app/tests/builder/steps/definition/builder/property/utils';
import { StructuredPropertyDefinitionCache, ValidationWarning } from '@app/tests/builder/validation/types';
import {
    ASSET_CATEGORIES,
    extractStructuredPropertyUrn,
    isActionSupportedForEntities,
    isPropertySupportedForEntities,
} from '@app/tests/builder/validation/utils';

import { EntityType } from '@types';

/**
 * Check if property is a placeholder (not yet selected)
 */
function isPlaceholderProperty(propertyId: string): boolean {
    return (
        propertyId === STRUCTURED_PROPERTY_REFERENCE_PLACEHOLDER_ID ||
        propertyId === SCHEMA_FIELD_STRUCTURED_PROPERTY_REFERENCE_PLACEHOLDER_ID
    );
}

/**
 * Validate placeholder properties against entity types
 */
function validatePlaceholderProperty(propertyId: string, entityTypes: EntityType[]): boolean {
    const isSchemaFieldPlaceholder = propertyId === SCHEMA_FIELD_STRUCTURED_PROPERTY_REFERENCE_PLACEHOLDER_ID;
    return isSchemaFieldPlaceholder
        ? entityTypes.every((type) => type === EntityType.Dataset)
        : entityTypes.every((type) => ASSET_CATEGORIES.DATA_ASSETS.has(type));
}

/**
 * Validate against cached structured property definition
 */
function validateWithDefinition(definition: any, entityTypes: EntityType[]): boolean {
    const hasRestrictions = definition.entityTypes && definition.entityTypes.length > 0;
    return hasRestrictions
        ? entityTypes.every((type) => definition.entityTypes.includes(type))
        : entityTypes.every((type) => ASSET_CATEGORIES.DATA_ASSETS.has(type));
}

/**
 * Enhanced validation for structured properties that checks actual supported entity types
 */
export function isStructuredPropertySupportedForEntities(
    propertyId: string,
    entityTypes: EntityType[],
    cache?: StructuredPropertyDefinitionCache,
): boolean {
    if (entityTypes.length === 0) return true;

    // Handle placeholder properties
    if (isPlaceholderProperty(propertyId)) {
        return validatePlaceholderProperty(propertyId, entityTypes);
    }

    // Get structured property definition
    const structuredPropertyUrn = extractStructuredPropertyUrn(propertyId);
    const definition = structuredPropertyUrn ? cache?.[structuredPropertyUrn] : null;

    // Use definition if available, otherwise fall back to basic validation
    return definition
        ? validateWithDefinition(definition, entityTypes)
        : entityTypes.every((type) => ASSET_CATEGORIES.DATA_ASSETS.has(type));
}

/**
 * Create validation message for unsupported properties
 */
function createValidationMessage(
    propertyId: string,
    unsupportedEntitiesText: string,
    structuredPropertyCache?: StructuredPropertyDefinitionCache,
): string {
    const isStructuredProp = isStructuredPropertyId(propertyId) || isSchemaFieldStructuredPropertyId(propertyId);

    if (!isStructuredProp) {
        // Handle non-structured properties
        if (isOwnershipTypeId(propertyId)) {
            return `Property "Ownership Type" is not available for ${unsupportedEntitiesText}. This property only works with data assets.`;
        }
        return `Property "${propertyId}" is not available for ${unsupportedEntitiesText}. This property only works with data assets.`;
    }

    // Handle structured properties
    const structuredPropertyUrn = extractStructuredPropertyUrn(propertyId);
    const definition = structuredPropertyUrn ? structuredPropertyCache?.[structuredPropertyUrn] : null;
    const displayName = definition?.displayName || 'Structured Property';

    if (isSchemaFieldStructuredPropertyId(propertyId)) {
        return `Property "${displayName}" is not available for ${unsupportedEntitiesText}. This column-level structured property only works with datasets.`;
    }

    if (definition?.entityTypes) {
        const supportedTypes = definition.entityTypes.map((t) => t.toLowerCase()).join(', ');
        return `Property "${displayName}" is not available for ${unsupportedEntitiesText}. This structured property only works with: ${supportedTypes}.`;
    }

    return `Property "${displayName}" is not available for ${unsupportedEntitiesText}. This structured property may have entity type restrictions.`;
}

/**
 * Get enhanced validation warnings that include structured property-specific validation
 */
export function getEnhancedValidationWarnings(
    entityTypes: EntityType[],
    properties: string[],
    actions: ActionType[],
    structuredPropertyCache?: StructuredPropertyDefinitionCache,
): ValidationWarning[] {
    // Early returns for simple cases
    if (entityTypes.length === 0) return [];
    if (properties.length === 0 && actions.length === 0) return [];

    const warnings: ValidationWarning[] = [];

    // Validate properties
    properties.forEach((propertyId) => {
        const isStructuredProp = isStructuredPropertyId(propertyId) || isSchemaFieldStructuredPropertyId(propertyId);
        const isSupported = isStructuredProp
            ? isStructuredPropertySupportedForEntities(propertyId, entityTypes, structuredPropertyCache)
            : isPropertySupportedForEntities(propertyId, entityTypes);

        if (!isSupported) {
            // Get unsupported entity types for error message
            const unsupportedEntities = entityTypes.filter((entityType) => {
                return isStructuredProp
                    ? !isStructuredPropertySupportedForEntities(propertyId, [entityType], structuredPropertyCache)
                    : !isPropertySupportedForEntities(propertyId, [entityType]);
            });

            const unsupportedEntitiesText = unsupportedEntities
                .map((type) =>
                    type
                        .toLowerCase()
                        .replace(/([A-Z])/g, ' $1')
                        .trim(),
                )
                .join(', ');

            const message = createValidationMessage(propertyId, unsupportedEntitiesText, structuredPropertyCache);

            warnings.push({
                type: 'property',
                propertyId,
                message,
            });
        }
    });

    // Validate actions
    actions.forEach((action) => {
        const isSupported = isActionSupportedForEntities(action.id, entityTypes);
        if (!isSupported) {
            const unsupportedEntities = entityTypes.filter(
                (entityType) => !isActionSupportedForEntities(action.id, [entityType]),
            );
            const unsupportedEntitiesText = unsupportedEntities
                .map((type) =>
                    type
                        .toLowerCase()
                        .replace(/([A-Z])/g, ' $1')
                        .trim(),
                )
                .join(', ');

            warnings.push({
                type: 'action',
                actionId: action.id,
                message: `Action "${action.displayName}" is not available for ${unsupportedEntitiesText}. This action only works with data assets.`,
            });
        }
    });

    return warnings;
}
