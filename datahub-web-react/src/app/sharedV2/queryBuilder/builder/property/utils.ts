import {
    OWNERSHIP_TYPE_REFERENCE_PLACEHOLDER_ID,
    OWNERSHIP_TYPE_REFERENCE_REGEX,
    STRUCTURED_PROPERTY_REFERENCE_PLACEHOLDER_ID,
    STRUCTURED_PROPERTY_REFERENCE_REGEX,
} from '@app/sharedV2/queryBuilder/builder/property/constants';
import {
    OPERATOR_ID_TO_DETAILS,
    Operator,
    OperatorId,
    isUnaryOperator,
} from '@app/sharedV2/queryBuilder/builder/property/types/operators';
import { Property, entityProperties } from '@app/sharedV2/queryBuilder/builder/property/types/properties';
import {
    VALUE_TYPE_ID_TO_DETAILS,
    ValueInputType,
    ValueOptions,
    ValueTypeId,
} from '@app/sharedV2/queryBuilder/builder/property/types/values';
import { PropertyPredicate } from '@app/sharedV2/queryBuilder/builder/types';

import { EntityType, PropertyCardinality, StdDataType, StructuredPropertyEntity } from '@types';

/**
 * Returns true if a well-supported Property supports searchable
 * entity values.
 */
const isSearchableProperty = (property: Property): boolean => {
    return property.valueOptions?.entityTypes && property.valueOptions?.mode;
};

/**
 * Returns true if a well-supported Property supports a fixed
 * set of select options.
 */
const isSelectableProperty = (property: Property): boolean => {
    return property.valueOptions?.options && property.valueOptions?.mode;
};

/**
 * Returns true if we are dealing with a Time-Select property.
 */
const isTimeProperty = (property: Property): boolean => {
    return property.valueType === ValueTypeId.TIMESTAMP;
};

/**
 * Returns a list of properties supported fora given entity type.
 */
const getPropertiesForEntityType = (type: EntityType): Property[] => {
    const maybeProperties = entityProperties.filter((entry) => entry.type === type);
    return maybeProperties.length > 0 ? maybeProperties[0].properties : [];
};

const intersectPropertySets = (a: Property[], b: Property[]): Property[] => {
    const aIds = new Set<string>(a.map((prop) => prop.id));
    const mergedProperties: Property[] = [];
    b.forEach((prop) => {
        if (aIds.has(prop.id)) {
            mergedProperties.push(prop);
        }
    });
    return mergedProperties;
};

/**
 * Returns the subset of properties supported by all entity types in the set. (intersection)
 */
export const getPropertiesForEntityTypes = (types: EntityType[]): Property[] => {
    const propertySets: Property[][] = [];
    types.forEach((type) => {
        const props = getPropertiesForEntityType(type);
        propertySets.push(props);
    });
    return propertySets.length > 0 ? propertySets.reduce(intersectPropertySets) : [];
};

/**
 * Returns the set of operators that are supported
 * for a given well-supported property.
 *
 * This is based on the "value type" of the property, along with the options.
 */
export const getOperatorOptions = (valueType: ValueTypeId): Operator[] | undefined => {
    const maybeDetails = VALUE_TYPE_ID_TO_DETAILS.get(valueType);
    if (maybeDetails) {
        return maybeDetails.operators.map((op) => OPERATOR_ID_TO_DETAILS.get(op));
    }
    return [];
};

/**
 * Returns a set of ValueOptions which determines how to render
 * the value selector for a particular well-supported Property.
 */
export const getValueOptions = (property: Property, predicate: PropertyPredicate): ValueOptions | undefined => {
    if (!predicate.operator || isUnaryOperator(predicate.operator)) {
        return undefined;
    }
    // Display an Entity Search values input.
    if (isSearchableProperty(property)) {
        return {
            inputType: ValueInputType.ENTITY_SEARCH,
            options: property.valueOptions,
        };
    }
    // Display a fixed select values input.
    if (isSelectableProperty(property)) {
        return {
            inputType: ValueInputType.SELECT,
            options: property.valueOptions,
        };
    }
    // Display a fixed select values input.
    if (isTimeProperty(property)) {
        return {
            inputType: ValueInputType.TIME_SELECT,
            options: property.valueOptions,
        };
    }
    // By default, just render a normal text input.
    return {
        inputType: ValueInputType.TEXT,
        options: property.valueOptions,
    };
};
