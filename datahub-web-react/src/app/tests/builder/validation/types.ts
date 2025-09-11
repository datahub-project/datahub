import { ActionId } from '@app/tests/builder/steps/definition/builder/property/types/action';

import { EntityType } from '@types';

/**
 * Cache to store structured property definitions for validation
 * This helps avoid repeated API calls and enables synchronous validation
 */
export interface StructuredPropertyDefinitionCache {
    [urn: string]: {
        entityTypes: EntityType[];
        valueType: string;
        allowedValues?: Array<{ value: any; description?: string }>;
        cardinality?: string;
        displayName?: string;
    };
}

export interface ValidationWarning {
    type: 'property' | 'action';
    message: string;
    propertyId?: string;
    actionId?: ActionId;
}
