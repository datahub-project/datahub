import { EntityType } from '@types';
import { 
    ActionId, 
    ActionType, 
    ACTION_TYPES 
} from '@app/tests/builder/steps/definition/builder/property/types/action';
import { 
    Property, 
    entityProperties, 
    commonProps, 
    baseEntityProps, 
    dataEntityProps 
} from '@app/tests/builder/steps/definition/builder/property/types/properties';
import { LogicalPredicate, PropertyPredicate } from '@app/tests/builder/steps/definition/builder/types';

/**
 * Utility functions for validating metadata test configurations
 * These are automatically derived from the property definitions in properties.ts
 */

/**
 * Automatically extract all property IDs from a property array
 */
function extractPropertyIds(properties: Property[]): Set<string> {
    const ids = new Set<string>();
    
    function extractFromProperty(prop: Property) {
        ids.add(prop.id);
        if (prop.children) {
            prop.children.forEach(child => extractFromProperty(child));
        }
    }
    
    properties.forEach(prop => extractFromProperty(prop));
    return ids;
}

/**
 * Automatically derived property support sets from properties.ts
 */
const AUTO_PROPERTY_SUPPORT = {
    // Properties from commonProps - supported by all entities
    UNIVERSAL: extractPropertyIds(commonProps),
    // Properties from baseEntityProps (excluding commonProps) - not supported by metadata entities like glossaryTerm
    BASE_ENTITY_ONLY: (() => {
        const baseIds = new Set<string>();
        // Extract only the properties that are in baseEntityProps but not in commonProps
        baseEntityProps.forEach(prop => {
            // Skip properties that are also in commonProps
            if (!commonProps.some(commonProp => commonProp.id === prop.id)) {
                function extractFromProperty(property: Property) {
                    baseIds.add(property.id);
                    if (property.children) {
                        property.children.forEach(child => extractFromProperty(child));
                    }
                }
                extractFromProperty(prop);
            }
        });
        return baseIds;
    })(),
    // Properties from dataEntityProps - only supported by data entities
    DATA_ENTITY_ONLY: extractPropertyIds(dataEntityProps),
    // Entity-specific properties - only supported by specific entity types
    ENTITY_SPECIFIC: (() => {
        const entitySpecificIds = new Set<string>();
        const universalIds = extractPropertyIds(commonProps);
        const baseEntityIds = extractPropertyIds(baseEntityProps);
        const dataEntityIds = extractPropertyIds(dataEntityProps);
        
        // Extract properties that are specific to individual entities
        entityProperties.forEach(entityProp => {
            const entityPropertyIds = extractPropertyIds(entityProp.properties);
            
            entityPropertyIds.forEach(propId => {
                // Check if this property is NOT in any of the general categories
                if (!universalIds.has(propId) && !baseEntityIds.has(propId) && !dataEntityIds.has(propId)) {
                    entitySpecificIds.add(propId);
                }
            });
        });
        return entitySpecificIds;
    })(),
};

/**
 * Automatically derived entity categories based on which properties they support
 */
const AUTO_ENTITY_CATEGORIES = (() => {
    const categories = {
        DATA_ENTITIES: new Set<EntityType>(),
        METADATA_ENTITIES: new Set<EntityType>(),
        ALL_ENTITIES: new Set<EntityType>(),
    };
    
    entityProperties.forEach(entityProp => {
        categories.ALL_ENTITIES.add(entityProp.type);
        
        const entityPropertyIds = extractPropertyIds(entityProp.properties);
        
        // Check if this entity supports data entity properties
        const supportsDataEntityProps = Array.from(AUTO_PROPERTY_SUPPORT.DATA_ENTITY_ONLY).some(propId => 
            entityPropertyIds.has(propId)
        );
        
        if (supportsDataEntityProps) {
            categories.DATA_ENTITIES.add(entityProp.type);
        } else {
            categories.METADATA_ENTITIES.add(entityProp.type);
        }
    });
    
    return categories;
})();



/**
 * Automatically derived action support based on ACTION_TYPES configuration
 */
const AUTO_ACTION_SUPPORT = (() => {
    const support: Record<ActionId, Set<EntityType>> = {} as Record<ActionId, Set<EntityType>>;
    
    // Extract action support from ACTION_TYPES configuration
    ACTION_TYPES.forEach(actionType => {
        if (actionType.supportedEntityTypes) {
            support[actionType.id] = new Set(actionType.supportedEntityTypes);
        } else {
            // If no supportedEntityTypes specified, assume it supports all entities
            support[actionType.id] = new Set(AUTO_ENTITY_CATEGORIES.ALL_ENTITIES);
        }
    });
    
    return support;
})();



/**
 * Export automatically derived action support sets
 */
export const TAG_SUPPORTED_ENTITIES = AUTO_ACTION_SUPPORT[ActionId.ADD_TAGS];
export const GLOSSARY_TERM_SUPPORTED_ENTITIES = AUTO_ACTION_SUPPORT[ActionId.ADD_GLOSSARY_TERMS];
export const DOMAIN_SUPPORTED_ENTITIES = AUTO_ACTION_SUPPORT[ActionId.SET_DOMAIN];
export const OWNERSHIP_SUPPORTED_ENTITIES = AUTO_ACTION_SUPPORT[ActionId.ADD_OWNERS];
export const DEPRECATION_SUPPORTED_ENTITIES = AUTO_ACTION_SUPPORT[ActionId.DEPRECATE];

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
    return entityTypes.every(type => supportedEntities.has(type));
}

/**
 * Filter action types to only include those supported by the given entity types
 */
export function filterActionTypesByEntities(actionTypes: ActionType[], entityTypes: EntityType[]): ActionType[] {
    return actionTypes.filter(actionType => 
        isActionSupportedForEntities(actionType.id, entityTypes)
    );
}

/**
 * Export the automatically derived categories and property support for external use
 */
export const ENTITY_CATEGORIES = AUTO_ENTITY_CATEGORIES;
export const PROPERTY_SUPPORT = {
    UNIVERSAL: AUTO_PROPERTY_SUPPORT.UNIVERSAL,
    DATA_ENTITY_COMMON: AUTO_PROPERTY_SUPPORT.BASE_ENTITY_ONLY,
    DATA_ENTITY_SPECIFIC: new Set([...AUTO_PROPERTY_SUPPORT.DATA_ENTITY_ONLY, ...AUTO_PROPERTY_SUPPORT.ENTITY_SPECIFIC]),
};

/**
 * Debug function to check what we automatically derived (temporary)
 */
export function debugValidationConfig() {
    const config = {
        entityCategories: {
            dataEntities: Array.from(ENTITY_CATEGORIES.DATA_ENTITIES),
            metadataEntities: Array.from(ENTITY_CATEGORIES.METADATA_ENTITIES),
            allEntities: Array.from(ENTITY_CATEGORIES.ALL_ENTITIES),
        },
        propertySupport: {
            universal: Array.from(PROPERTY_SUPPORT.UNIVERSAL),
            dataEntityCommon: Array.from(PROPERTY_SUPPORT.DATA_ENTITY_COMMON),
            dataEntitySpecific: Array.from(PROPERTY_SUPPORT.DATA_ENTITY_SPECIFIC),
        },
        actionSupport: Object.fromEntries(
            Object.entries(AUTO_ACTION_SUPPORT).map(([actionId, entitySet]) => [
                actionId, Array.from(entitySet)
            ])
        )
    };
    
    return config;
}

/**
 * Check if a property is supported for the given entity types
 */
export function isPropertySupportedForEntities(propertyId: string, entityTypes: EntityType[]): boolean {
    if (entityTypes.length === 0) return true; // No entities selected, allow all properties
    
    // Universal properties are supported by all entities
    if (PROPERTY_SUPPORT.UNIVERSAL.has(propertyId)) {
        return true;
    }
    
    // Check if property requires data entity support
    if (PROPERTY_SUPPORT.DATA_ENTITY_COMMON.has(propertyId)) {
        // All selected entities must be data entities (not metadata entities like glossaryTerm)
        return entityTypes.every(type => ENTITY_CATEGORIES.DATA_ENTITIES.has(type));
    }
    
    // Check if property requires data entity specific support
    if (PROPERTY_SUPPORT.DATA_ENTITY_SPECIFIC.has(propertyId)) {
        // All selected entities must be data entities
        return entityTypes.every(type => ENTITY_CATEGORIES.DATA_ENTITIES.has(type));
    }
    
    // For other properties, assume they're supported (entity-specific properties)
    return true;
}

/**
 * Helper function to format entity names for display (pluralize and format)
 */
function formatEntityName(entityType: EntityType, pluralize: boolean = false): string {
    // Convert enum to readable format (remove underscores, add spaces, lowercase)
    let formatted = entityType.toLowerCase().replace(/_/g, ' ');
    
    if (pluralize) {
        // Simple pluralization rules
        if (formatted.endsWith('y') && !formatted.endsWith('ay') && !formatted.endsWith('ey') && !formatted.endsWith('iy') && !formatted.endsWith('oy') && !formatted.endsWith('uy')) {
            // Only change 'y' to 'ies' if it's a consonant + y (like 'company' -> 'companies')
            // Don't change vowel + y (like 'day' -> 'days')
            formatted = formatted.slice(0, -1) + 'ies';
        } else if (formatted.endsWith('s') || formatted.endsWith('sh') || formatted.endsWith('ch') || formatted.endsWith('x') || formatted.endsWith('z')) {
            formatted = formatted + 'es';
        } else {
            formatted = formatted + 's';
        }
    }
    
    return formatted;
}

/**
 * Helper function to get display name for actions
 */
function getActionDisplayName(actionId: ActionId): string {
    switch (actionId) {
        case ActionId.ADD_TAGS:
            return 'Add Tags';
        case ActionId.REMOVE_TAGS:
            return 'Remove Tags';
        case ActionId.ADD_GLOSSARY_TERMS:
            return 'Add Glossary Terms';
        case ActionId.REMOVE_GLOSSARY_TERMS:
            return 'Remove Glossary Terms';
        case ActionId.SET_DOMAIN:
            return 'Set Domain';
        case ActionId.UNSET_DOMAIN:
            return 'Unset Domain';
        case ActionId.ADD_OWNERS:
            return 'Add Owners';
        case ActionId.REMOVE_OWNERS:
            return 'Remove Owners';
        case ActionId.DEPRECATE:
            return 'Mark as Deprecated';
        case ActionId.UN_DEPRECATE:
            return 'Remove Deprecated Status';
        default:
            return String(actionId).replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
    }
}

/**
 * Helper function to get entities that don't support a specific action
 */
function getUnsupportedEntitiesForAction(actionId: ActionId, entityTypes: EntityType[]): string[] {
    const supportedEntities = AUTO_ACTION_SUPPORT[actionId];
    if (!supportedEntities) {
        return []; // Unknown action, assume all entities are supported
    }
    
    return entityTypes
        .filter(type => !supportedEntities.has(type))
        .map(type => formatEntityName(type, true));
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

export function getValidationWarnings(
    entityTypes: EntityType[],
    properties: string[],
    actions: { type: string }[]
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
    properties.forEach(propertyId => {
        const isSupported = isPropertySupportedForEntities(propertyId, entityTypes);
        
        if (!isSupported) {
            const unsupportedEntities = entityTypes
                .filter(type => !ENTITY_CATEGORIES.DATA_ENTITIES.has(type))
                .map(type => formatEntityName(type, true))
                .join(', ');
                
            warnings.push({
                type: 'property',
                message: `Property "${propertyId}" is not available for ${unsupportedEntities}. This property only works with data assets.`,
                propertyId,
            });
        }
    });
    
    // Check for invalid actions
    actions.forEach(action => {
        const actionId = action.type as ActionId;
        const isActionSupported = isActionSupportedForEntities(actionId, entityTypes);
        
        if (!isActionSupported) {
            const unsupportedEntities = getUnsupportedEntitiesForAction(actionId, entityTypes);
            const actionDisplayName = getActionDisplayName(actionId);
            
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
export function getPropertiesFromPredicate(predicate: LogicalPredicate | PropertyPredicate | null): string[] {
    if (!predicate) return [];
    
    const properties: string[] = [];
    
    const extractFromPredicate = (pred: LogicalPredicate | PropertyPredicate) => {
        if (pred.type === 'logical') {
            // This is a LogicalPredicate - recurse through operands
            const logicalPred = pred as LogicalPredicate;
            logicalPred.operands.forEach(subPred => extractFromPredicate(subPred));
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
