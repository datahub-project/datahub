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
import {
    ASSET_CATEGORIES,
    StructuredPropertyDefinitionCache,
    ValidationWarning,
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
 * Validate structured property value based on its type definition
 * @param propertyId The structured property ID
 * @param value The value to validate
 * @param cache Optional cache of structured property definitions
 */
/**
 * Extract allowed string values from definition
 */
function getAllowedStringValues(allowedValues?: Array<{ value: any }>): string[] {
    return allowedValues?.map((av) => av.value?.stringValue).filter(Boolean) || [];
}

/**
 * Extract allowed number values from definition
 */
function getAllowedNumberValues(allowedValues?: Array<{ value: any }>): number[] {
    return allowedValues?.map((av) => av.value?.numberValue).filter((val) => val !== undefined && val !== null) || [];
}

/**
 * Validate string value against allowed values
 */
function validateStringValue(
    value: any,
    allowedValues?: Array<{ value: any }>,
): { isValid: boolean; errorMessage?: string } {
    if (typeof value !== 'string') {
        return { isValid: false, errorMessage: `Expected string value, got ${typeof value}` };
    }

    const allowedStringValues = getAllowedStringValues(allowedValues);
    const hasRestrictions = allowedStringValues.length > 0;
    const isAllowed = !hasRestrictions || allowedStringValues.includes(value);

    return isAllowed
        ? { isValid: true }
        : { isValid: false, errorMessage: `Value must be one of: ${allowedStringValues.join(', ')}` };
}

/**
 * Validate number value against allowed values
 */
function validateNumberValue(
    value: any,
    allowedValues?: Array<{ value: any }>,
): { isValid: boolean; errorMessage?: string } {
    const numValue = typeof value === 'string' ? parseFloat(value) : value;

    if (Number.isNaN(numValue)) {
        return { isValid: false, errorMessage: 'Expected numeric value' };
    }

    const allowedNumValues = getAllowedNumberValues(allowedValues);
    const hasRestrictions = allowedNumValues.length > 0;
    const isAllowed = !hasRestrictions || allowedNumValues.includes(numValue);

    return isAllowed
        ? { isValid: true }
        : { isValid: false, errorMessage: `Value must be one of: ${allowedNumValues.join(', ')}` };
}

/**
 * Validate structured property value based on its type definition
 */
export function validateStructuredPropertyValue(
    propertyId: string,
    value: any,
    cache?: StructuredPropertyDefinitionCache,
): { isValid: boolean; errorMessage?: string } {
    const structuredPropertyUrn = extractStructuredPropertyUrn(propertyId);
    const definition = structuredPropertyUrn ? cache?.[structuredPropertyUrn] : null;

    // Without definition, assume valid
    if (!definition) return { isValid: true };

    const { valueType, allowedValues } = definition;

    // Validate based on type
    if (valueType === 'string') return validateStringValue(value, allowedValues);
    if (valueType === 'number') return validateNumberValue(value, allowedValues);

    // Unknown types are valid
    return { isValid: true };
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
/**
 * Check if property is supported for entity types
 */
function checkPropertySupport(
    propertyId: string,
    entityTypes: EntityType[],
    structuredPropertyCache?: StructuredPropertyDefinitionCache,
): boolean {
    const isStructuredProp = isStructuredPropertyId(propertyId) || isSchemaFieldStructuredPropertyId(propertyId);
    return isStructuredProp
        ? isStructuredPropertySupportedForEntities(propertyId, entityTypes, structuredPropertyCache)
        : isPropertySupportedForEntities(propertyId, entityTypes);
}

/**
 * Get unsupported entity types for a property
 */
function getUnsupportedEntitiesForProperty(propertyId: string, entityTypes: EntityType[]): EntityType[] {
    return entityTypes.filter((entityType) => {
        const isStructuredProp = isStructuredPropertyId(propertyId) || isSchemaFieldStructuredPropertyId(propertyId);
        return isStructuredProp
            ? !isStructuredPropertySupportedForEntities(propertyId, [entityType])
            : !isPropertySupportedForEntities(propertyId, [entityType]);
    });
}

/**
 * Get unsupported entity types for an action
 */
function getUnsupportedEntitiesForAction(actionId: any, entityTypes: EntityType[]): EntityType[] {
    return entityTypes.filter((entityType) => !isActionSupportedForEntities(actionId, [entityType]));
}

/**
 * Create property validation warning
 */
function createPropertyWarning(
    propertyId: string,
    entityTypes: EntityType[],
    structuredPropertyCache?: StructuredPropertyDefinitionCache,
): ValidationWarning {
    const unsupportedEntities = getUnsupportedEntitiesForProperty(propertyId, entityTypes);
    const unsupportedEntitiesText = unsupportedEntities
        .map((type) =>
            type
                .toLowerCase()
                .replace(/([A-Z])/g, ' $1')
                .trim(),
        )
        .join(', ');

    const message = createValidationMessage(propertyId, unsupportedEntitiesText, structuredPropertyCache);

    return {
        type: 'property',
        propertyId,
        message,
    };
}

/**
 * Create action validation warning
 */
function createActionWarning(action: ActionType, entityTypes: EntityType[]): ValidationWarning {
    const unsupportedEntities = getUnsupportedEntitiesForAction(action.id, entityTypes);
    const unsupportedEntitiesText = unsupportedEntities
        .map((type) =>
            type
                .toLowerCase()
                .replace(/([A-Z])/g, ' $1')
                .trim(),
        )
        .join(', ');

    return {
        type: 'action',
        actionId: action.id,
        message: `Action "${action.displayName}" is not available for ${unsupportedEntitiesText}. This action only works with data assets.`,
    };
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
        const isSupported = checkPropertySupport(propertyId, entityTypes, structuredPropertyCache);
        if (!isSupported) {
            warnings.push(createPropertyWarning(propertyId, entityTypes, structuredPropertyCache));
        }
    });

    // Validate actions
    actions.forEach((action) => {
        const isSupported = isActionSupportedForEntities(action.id, entityTypes);
        if (!isSupported) {
            warnings.push(createActionWarning(action, entityTypes));
        }
    });

    return warnings;
}
