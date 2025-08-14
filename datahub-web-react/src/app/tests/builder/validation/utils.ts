import { ACTION_TYPES, ActionId, ActionType } from '@app/tests/builder/steps/definition/builder/property/types/action';
import {
    Property,
    assetProps,
    commonProps,
    dataAssetProps,
    entityProperties,
} from '@app/tests/builder/steps/definition/builder/property/types/properties';
import { LogicalPredicate, PropertyPredicate } from '@app/tests/builder/steps/definition/builder/types';
import { convertTestPredicateToLogicalPredicate } from '@app/tests/builder/steps/definition/builder/utils';
import { TestDefinition } from '@app/tests/types';

import { EntityType } from '@types';

/**
 * METADATA TEST VALIDATION UTILITIES
 *
 * These utils provide validation for metadata test configurations by checking:
 * 1. Property compatibility: Whether selected properties work with chosen entity types
 * 2. Action compatibility: Whether selected actions work with chosen entity types
 *
 *
 * All mappings are automatically derived from properties.ts and action.ts to ensure consistency.
 */

// =============================================================================
// PROPERTY ANALYSIS HELPERS
// =============================================================================

/**
 * Recursively extract property IDs from a property definition
 */
function extractFromProperty(prop: Property, ids: Set<string>) {
    ids.add(prop.id);
    if (prop.children) {
        prop.children.forEach((child) => extractFromProperty(child, ids));
    }
}

/**
 * Automatically extract all property IDs from a property array
 */
function extractPropertyIds(properties: Property[]): Set<string> {
    const ids = new Set<string>();

    properties.forEach((prop) => extractFromProperty(prop, ids));
    return ids;
}

// =============================================================================
// AUTOMATIC CONFIGURATION DERIVATION
// =============================================================================

/**
 * Automatically derived property support sets from properties.ts
 * This creates a map showing which property categories are supported by which entity types.
 */
const AUTO_PROPERTY_SUPPORT = {
    // Properties from commonProps - supported by all entities
    UNIVERSAL: extractPropertyIds(commonProps),
    // Properties from assetProps (excluding commonProps) - not supported by metadata entities like glossaryTerm
    BASE_ENTITY_ONLY: (() => {
        const baseIds = new Set<string>();
        // Extract only the properties that are in assetProps but not in commonProps
        assetProps.forEach((prop) => {
            // Skip properties that are also in commonProps
            if (!commonProps.some((commonProp) => commonProp.id === prop.id)) {
                extractFromProperty(prop, baseIds);
            }
        });
        return baseIds;
    })(),
    // Properties from dataAssetProps - only supported by data assets
    DATA_ASSET_ONLY: extractPropertyIds(dataAssetProps),
    // Entity-specific properties - only supported by specific entity types
    ENTITY_SPECIFIC: (() => {
        const entitySpecificIds = new Set<string>();
        const universalIds = extractPropertyIds(commonProps);
        const baseEntityIds = extractPropertyIds(assetProps);
        const dataAssetIds = extractPropertyIds(dataAssetProps);

        // Extract properties that are specific to individual entities
        entityProperties.forEach((entityProp) => {
            const entityPropertyIds = extractPropertyIds(entityProp.properties);

            entityPropertyIds.forEach((propId) => {
                // Check if this property is NOT in any of the general categories
                if (!universalIds.has(propId) && !baseEntityIds.has(propId) && !dataAssetIds.has(propId)) {
                    entitySpecificIds.add(propId);
                }
            });
        });
        return entitySpecificIds;
    })(),
};

/**
 * Automatically derived asset categories based on which properties they support.
 * This groups entity types into data assets vs metadata assets.
 */
const AUTO_ASSET_CATEGORIES = (() => {
    const categories = {
        DATA_ASSETS: new Set<EntityType>(),
        METADATA_ASSETS: new Set<EntityType>(),
        ALL_ASSETS: new Set<EntityType>(),
    };

    entityProperties.forEach((entityProp) => {
        categories.ALL_ASSETS.add(entityProp.type);

        const entityPropertyIds = extractPropertyIds(entityProp.properties);

        // Check if this entity supports data asset properties
        const supportsDataAssetProps = Array.from(AUTO_PROPERTY_SUPPORT.DATA_ASSET_ONLY).some((propId) =>
            entityPropertyIds.has(propId),
        );

        if (supportsDataAssetProps) {
            categories.DATA_ASSETS.add(entityProp.type);
        } else {
            categories.METADATA_ASSETS.add(entityProp.type);
        }
    });

    return categories;
})();

/**
 * Automatically derived action support based on ACTION_TYPES configuration.
 * This creates a map showing which actions are supported by which entity types.
 */
const AUTO_ACTION_SUPPORT = (() => {
    const support: Record<ActionId, Set<EntityType>> = {} as Record<ActionId, Set<EntityType>>;

    // Extract action support from ACTION_TYPES configuration
    ACTION_TYPES.forEach((actionType) => {
        if (actionType.entityTypes) {
            support[actionType.id] = new Set(actionType.entityTypes);
        } else {
            // If no entityTypes specified, assume it supports all entities
            support[actionType.id] = new Set(AUTO_ASSET_CATEGORIES.ALL_ASSETS);
        }
    });

    return support;
})();

/**
 * Check if an action is supported for the given entity types
 */
export function isActionSupportedForEntities(actionId: ActionId, entityTypes: EntityType[]): boolean {
    if (entityTypes.length === 0) return true; // No entities selected, allow all actions

    // Use the automatically derived action support
    const supportedEntities = AUTO_ACTION_SUPPORT[actionId];
    if (!supportedEntities) {
        return true; // Unknown action, assume supported
    }

    // All selected entities must support this action
    return entityTypes.every((type) => supportedEntities.has(type));
}

/**
 * Filter action types to only include those supported by the given entity types
 */
export function filterActionTypesByEntities(actionTypes: ActionType[], entityTypes: EntityType[]): ActionType[] {
    return actionTypes.filter((actionType) => isActionSupportedForEntities(actionType.id, entityTypes));
}

// =============================================================================
// PUBLIC API - EXPORTED CONSTANTS AND FUNCTIONS
// =============================================================================

/**
 * Export the automatically derived categories for external use
 */
export const ASSET_CATEGORIES = AUTO_ASSET_CATEGORIES;

/**
 * Check if a property is supported for the given entity types
 */
export function isPropertySupportedForEntities(propertyId: string, entityTypes: EntityType[]): boolean {
    if (entityTypes.length === 0) return true; // No entities selected, allow all properties

    // Universal properties are supported by all entities
    if (AUTO_PROPERTY_SUPPORT.UNIVERSAL.has(propertyId)) {
        return true;
    }

    // Check if property requires data asset support
    if (AUTO_PROPERTY_SUPPORT.BASE_ENTITY_ONLY.has(propertyId)) {
        // All selected entities must be data assets (not metadata entities like glossaryTerm)
        return entityTypes.every((type) => AUTO_ASSET_CATEGORIES.DATA_ASSETS.has(type));
    }

    // Check if property requires data asset specific support
    if (
        AUTO_PROPERTY_SUPPORT.DATA_ASSET_ONLY.has(propertyId) ||
        AUTO_PROPERTY_SUPPORT.ENTITY_SPECIFIC.has(propertyId)
    ) {
        // For data asset specific properties, check if ALL selected entities actually support this property
        return entityTypes.every((entityType) => {
            const entityConfig = entityProperties.find((ep) => ep.type === entityType);
            if (!entityConfig) return false;

            const entityPropertyIds = extractPropertyIds(entityConfig.properties);
            return entityPropertyIds.has(propertyId);
        });
    }

    // For other properties (entity-specific), check if ALL selected entities actually support this property
    return entityTypes.every((entityType) => {
        const entityConfig = entityProperties.find((ep) => ep.type === entityType);
        if (!entityConfig) return false;

        const entityPropertyIds = extractPropertyIds(entityConfig.properties);
        return entityPropertyIds.has(propertyId);
    });
}

// =============================================================================
// INTERNAL HELPER FUNCTIONS
// =============================================================================

/**
 * Helper function to format entity names for display (pluralize and format)
 */
function formatEntityName(entityType: EntityType, pluralize = false): string {
    // Convert enum to readable format (remove underscores, add spaces, lowercase)
    let formatted = entityType.toLowerCase().replace(/_/g, ' ');

    if (pluralize) {
        // Simple pluralization rules
        if (
            formatted.endsWith('y') &&
            !formatted.endsWith('ay') &&
            !formatted.endsWith('ey') &&
            !formatted.endsWith('iy') &&
            !formatted.endsWith('oy') &&
            !formatted.endsWith('uy')
        ) {
            // Only change 'y' to 'ies' if it's a consonant + y (like 'company' -> 'companies')
            // Don't change vowel + y (like 'day' -> 'days')
            formatted = `${formatted.slice(0, -1)}ies`;
        } else if (
            formatted.endsWith('s') ||
            formatted.endsWith('sh') ||
            formatted.endsWith('ch') ||
            formatted.endsWith('x') ||
            formatted.endsWith('z')
        ) {
            formatted += 'es';
        } else {
            formatted += 's';
        }
    }

    return formatted;
}

/**
 * Helper function to get entities that don't support a specific property
 */
function getUnsupportedEntitiesForProperty(propertyId: string, entityTypes: EntityType[]): string[] {
    return entityTypes
        .filter((entityType) => {
            // Check if this specific entity type supports this property
            const entityConfig = entityProperties.find((ep) => ep.type === entityType);
            if (!entityConfig) return true; // If entity config not found, consider it unsupported

            const entityPropertyIds = extractPropertyIds(entityConfig.properties);
            return !entityPropertyIds.has(propertyId);
        })
        .map((type) => formatEntityName(type, true));
}

/**
 * Helper function to get entities that don't support a specific action
 */
function getUnsupportedEntitiesForAction(actionId: ActionId, entityTypes: EntityType[]): string[] {
    const supportedEntities = AUTO_ACTION_SUPPORT[actionId];
    if (!supportedEntities) {
        return []; // Unknown action, assume all entities are supported
    }

    return entityTypes.filter((type) => !supportedEntities.has(type)).map((type) => formatEntityName(type, true));
}

/**
 * Get validation warnings for invalid configurations
 */
export interface ValidationWarning {
    type: 'property' | 'action';
    message: string;
    propertyId?: string;
    actionId?: ActionId;
}

/**
 * Extract and validate all properties and actions from a complete test definition.
 * This is a comprehensive validation that checks selection filters, rules, and actions.
 */
export function validateCompleteTestDefinition(
    entityTypes: EntityType[],
    testDefinition: TestDefinition,
): ValidationWarning[] {
    // Validate selection filters
    const selectionPredicate = convertTestPredicateToLogicalPredicate(testDefinition.on?.conditions || []);
    const selectionProperties = getPropertiesFromLogicalPredicate(selectionPredicate);

    // Validate rules
    const rulesPredicate = convertTestPredicateToLogicalPredicate(testDefinition.rules);
    const rulesProperties = getPropertiesFromLogicalPredicate(rulesPredicate);

    // Extract actions (convert from TestAction to ActionType format)
    const passingActions = testDefinition.actions?.passing || [];
    const failingActions = testDefinition.actions?.failing || [];
    const allActions: ActionType[] = [
        ...passingActions.map((action) => ({ id: action.type }) as ActionType),
        ...failingActions.map((action) => ({ id: action.type }) as ActionType),
    ];

    // Get all validation warnings
    const allProperties = [...selectionProperties, ...rulesProperties];
    return getValidationWarnings(entityTypes, allProperties, allActions);
}

export function getValidationWarnings(
    entityTypes: EntityType[],
    properties: string[],
    actions: ActionType[],
): ValidationWarning[] {
    const warnings: ValidationWarning[] = [];

    // Early return if no entities selected or nothing to validate
    if (entityTypes.length === 0) {
        return warnings;
    }

    // Only validate if there are actually properties or actions to check
    if (properties.length === 0 && actions.length === 0) {
        return warnings;
    }

    // Only show entity compatibility warnings if there are actual conflicts
    // Don't warn just for mixing entity types - only warn when incompatible properties/actions are used

    // Check for invalid properties
    properties.forEach((propertyId) => {
        const isSupported = isPropertySupportedForEntities(propertyId, entityTypes);

        if (!isSupported) {
            const unsupportedEntities = getUnsupportedEntitiesForProperty(propertyId, entityTypes);
            const unsupportedEntitiesText = unsupportedEntities.join(', ');

            warnings.push({
                type: 'property',
                message: `Property "${propertyId}" is not available for ${unsupportedEntitiesText}. This property only works with data assets.`,
                propertyId,
            });
        }
    });

    // Check for invalid actions
    actions.forEach((action) => {
        const actionId = action.id;
        const isActionSupported = isActionSupportedForEntities(actionId, entityTypes);

        if (!isActionSupported) {
            const unsupportedEntities = getUnsupportedEntitiesForAction(actionId, entityTypes);
            const actionType = ACTION_TYPES.find((at) => at.id === actionId);
            const actionDisplayName = actionType?.displayName || actionId;

            warnings.push({
                type: 'action',
                message: `${actionDisplayName} is not available for ${unsupportedEntities.join(', ')}. This action only works with compatible entity types.`,
                actionId,
            });
        }
    });

    return warnings;
}

/**
 * Extract all property IDs used in a logical predicate
 */
export function getPropertiesFromLogicalPredicate(predicate: LogicalPredicate | PropertyPredicate | null): string[] {
    if (!predicate) return [];

    const properties: string[] = [];

    const extractFromPredicate = (pred: LogicalPredicate | PropertyPredicate) => {
        if (pred.type === 'logical') {
            // This is a LogicalPredicate - recurse through operands
            const logicalPred = pred as LogicalPredicate;
            logicalPred.operands.forEach((subPred) => extractFromPredicate(subPred));
        } else if (pred.type === 'property') {
            // This is a PropertyPredicate - extract the property
            const propertyPred = pred as PropertyPredicate;
            if (propertyPred.property) {
                properties.push(propertyPred.property);
            }
        }
    };

    extractFromPredicate(predicate);
    return [...new Set(properties)]; // Remove duplicates
}
