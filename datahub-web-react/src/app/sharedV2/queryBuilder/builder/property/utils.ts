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
 * Returns true if a Property uses aggregation-based value fetching.
 */
const isAggregationProperty = (property: Property): boolean => {
    return !!property.valueOptions?.aggregationField;
};

/**
 * Returns a list of properties supported fora given entity type.
 */
export const getPropertiesForEntityType = (type: EntityType): Property[] => {
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
 * Retrieves a specific Property from a list of properties given the
 * property's unique id, or undefined if one cannot be found.
 */
export const getPropertyById = (propertyId: string, properties: Property[]): Property | undefined => {
    // eslint-disable-next-line
    for (const prop of properties) {
        if (prop.id === propertyId) {
            return prop;
        }
        if (prop?.children) {
            const foundProp = getPropertyById(propertyId, prop?.children);
            if (foundProp) {
                return foundProp;
            }
        }
    }
    return undefined;
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
    // Display an aggregation-based values input.
    if (isAggregationProperty(property)) {
        return {
            inputType: ValueInputType.AGGREGATION,
            options: {
                facetField: property.valueOptions.aggregationField,
                mode: property.valueOptions.mode,
            },
        };
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

/**
 * Attempts to determine whether the property id should be treated as a "structured property id",
 * which implies that the editor experience we show will be handled slightly differently than the default.
 *
 * There are 2 cases where we will consider the property to be related to structured properties:
 *
 * 1. The property id is equivalent to the "placeholder" property id used by the normal Property Select.
 * This implies that the user has just selected "Structured Property" from the list of possible entity properties,
 * but has not yet selected a specific property (which is required to complete the test).
 *
 * 2. The property id is a reference to a specific structured property, having the form "structuredProperties.urn:li:structuredProperty:xyz".
 * This implies that a property has already been chosen for testing by the user.
 *
 * @param propertyId the property id that may refer to the structured property concept.
 */
export const isStructuredPropertyId = (propertyId: string) => {
    return (
        propertyId === STRUCTURED_PROPERTY_REFERENCE_PLACEHOLDER_ID ||
        STRUCTURED_PROPERTY_REFERENCE_REGEX.test(propertyId)
    );
};

/**
 * Attempts to determine whether the property id should be treated as a "ownership type id",
 * which implies that the editor experience we show will be handled slightly differently than the default.
 *
 * There are 2 cases where we will consider the property to be related to ownership types:
 *
 * 1. The property id is equivalent to the "placeholder" property id used by the normal Property Select.
 * This implies that the user has just selected "Ownership Type" from the list of possible entity properties,
 * but has not yet selected a specific type (which is required to complete the test).
 *
 * 2. The property id is a reference to a specific ownership type, having the form "ownership.ownerTypes.urn:li:ownershipType:xyz".
 * This implies that a property has already been chosen for testing by the user.
 *
 * @param propertyId the property id that may refer to the structured property concept.
 */
export const isOwnershipTypeId = (propertyId: string) => {
    return propertyId === OWNERSHIP_TYPE_REFERENCE_PLACEHOLDER_ID || OWNERSHIP_TYPE_REFERENCE_REGEX.test(propertyId);
};

/**
 * Attempts to extract the URN of a structured property to reference using the property id.
 * Does this by extracting a regex group if there is a match:
 *
 * structuredProperties.urn:li:structuredProperty:xyz
 *
 * @param propertyId the property id that may contain reference to a specific structured property.
 * Returns undefined if a structured property urn cannot be found (meaning one is not yet selected).
 */
export const extractStructuredPropertyReferenceUrn = (propertyId: string) => {
    const match = propertyId.match(STRUCTURED_PROPERTY_REFERENCE_REGEX);
    return match ? match[1] : undefined;
};

/**
 * Attempts to extract the URN of a ownership type to reference using the property id.
 * Does this by extracting a regex group if there is a match:
 *
 * ownership.ownerTypes.urn:li:ownershipType:xyz
 *
 * @param propertyId the property id that may contain reference to a specific ownership type.
 * Returns undefined if a ownership type urn cannot be found (meaning one is not yet selected).
 */
export const extractOwnershipTypeReferenceUrn = (propertyId: string) => {
    const match = propertyId.match(OWNERSHIP_TYPE_REFERENCE_REGEX);
    return match ? match[1] : undefined;
};

/**
 * Returns a set of valid operator options given a structured property definition.
 * This is achieved by considering the valueType of the property.
 */
export const getStructuredPropertiesOperatorOptions = (property: StructuredPropertyEntity) => {
    const valueType = property.definition?.valueType;
    const stdValueType = valueType.info?.type;
    const cardinality = property.definition?.cardinality;
    const allowedValues = property.definition?.allowedValues;
    switch (stdValueType) {
        case StdDataType.String: {
            if (allowedValues) {
                return [OPERATOR_ID_TO_DETAILS.get(OperatorId.EXISTS), OPERATOR_ID_TO_DETAILS.get(OperatorId.EQUAL_TO)];
            }
            return [
                OPERATOR_ID_TO_DETAILS.get(OperatorId.EXISTS),
                OPERATOR_ID_TO_DETAILS.get(OperatorId.EQUAL_TO),
                OPERATOR_ID_TO_DETAILS.get(OperatorId.REGEX_MATCH),
                OPERATOR_ID_TO_DETAILS.get(OperatorId.CONTAINS_STR),
                OPERATOR_ID_TO_DETAILS.get(OperatorId.STARTS_WITH),
            ];
        }
        case StdDataType.Number:
            if (allowedValues) {
                return [OPERATOR_ID_TO_DETAILS.get(OperatorId.EXISTS), OPERATOR_ID_TO_DETAILS.get(OperatorId.EQUAL_TO)];
            }
            return [
                OPERATOR_ID_TO_DETAILS.get(OperatorId.EXISTS),
                OPERATOR_ID_TO_DETAILS.get(OperatorId.GREATER_THAN),
                OPERATOR_ID_TO_DETAILS.get(OperatorId.LESS_THAN),
                OPERATOR_ID_TO_DETAILS.get(OperatorId.EQUAL_TO),
            ];
        case StdDataType.Urn:
        case StdDataType.Other:
        default: {
            const baseTypes = [
                OPERATOR_ID_TO_DETAILS.get(OperatorId.EXISTS),
                OPERATOR_ID_TO_DETAILS.get(OperatorId.EQUAL_TO),
            ];
            return cardinality === PropertyCardinality.Multiple
                ? [...baseTypes, OPERATOR_ID_TO_DETAILS.get(OperatorId.CONTAINS_ANY)]
                : baseTypes;
        }
    }
};

/**
 * Returns a set of valid operator options given a ownership type definition.
 */
export const getOwnershipTypeOperatorOptions = () => {
    return [
        OPERATOR_ID_TO_DETAILS.get(OperatorId.EXISTS),
        OPERATOR_ID_TO_DETAILS.get(OperatorId.EQUAL_TO),
        OPERATOR_ID_TO_DETAILS.get(OperatorId.REGEX_MATCH),
        OPERATOR_ID_TO_DETAILS.get(OperatorId.CONTAINS_STR),
        OPERATOR_ID_TO_DETAILS.get(OperatorId.STARTS_WITH),
    ];
};

/**
 * Returns a set of valid operator options given a structured property definition.
 * This is achieved by considering the valueType and the allowedValues of the property.
 */
export const getStructuredPropertyValueOptions = (property: StructuredPropertyEntity) => {
    const valueType = property.definition?.valueType;
    const stdValueType = valueType.info?.type;
    const allowedValues = property.definition?.allowedValues;
    const allowedEntityTypes = property.definition?.typeQualifier?.allowedTypes;
    switch (stdValueType) {
        case StdDataType.String: {
            if (allowedValues) {
                return {
                    inputType: ValueInputType.SELECT,
                    options: {
                        options:
                            allowedValues
                                .filter((value) => (value.value as any).stringValue)
                                .map((value) => {
                                    const actualValue = (value.value as any).stringValue;
                                    return {
                                        id: actualValue,
                                        displayName: actualValue,
                                    };
                                }) || [],
                    },
                };
            }
            return {
                inputType: ValueInputType.TEXT,
                options: undefined,
            };
        }
        case StdDataType.Number: {
            if (allowedValues) {
                return {
                    inputType: ValueInputType.SELECT, // Should become numeric
                    options: {
                        options: allowedValues
                            .filter((value) => (value.value as any).numberValue)
                            .map((value) => {
                                const actualValue = (value.value as any).numberValue;
                                return {
                                    id: actualValue,
                                    displayName: actualValue,
                                };
                            }),
                    },
                };
            }
            return {
                inputType: ValueInputType.TEXT,
                options: undefined,
            };
        }
        case StdDataType.Urn: {
            // Note: Currently we do not support limiting URN types using allowed values
            // on the frontend side.
            return {
                inputType: ValueInputType.ENTITY_SEARCH,
                options:
                    (allowedEntityTypes && {
                        entityTypes: allowedEntityTypes.map((type) => type.info?.type),
                    }) ||
                    undefined,
            };
        }
        case StdDataType.Other:
        default:
            if (allowedValues) {
                return {
                    inputType: ValueInputType.SELECT,
                    options: {
                        options: allowedValues
                            .filter((value) => (value.value as any).numberValue || (value.value as any).stringValue)
                            .map((value) => {
                                const actualValue =
                                    (value.value as any).stringValue || (value.value as any).numberValue;
                                return {
                                    id: actualValue,
                                    displayName: actualValue,
                                };
                            }),
                    },
                };
            }
            return {
                inputType: ValueInputType.TEXT,
                options: undefined,
            };
    }
};

/**
 * Returns a set of valid operator options given a ownership type definition.
 */
export const getOwnershipTypeValueOptions = (predicate: PropertyPredicate): ValueOptions | undefined => {
    if (!predicate.operator || isUnaryOperator(predicate.operator)) {
        return undefined;
    }
    return {
        inputType: ValueInputType.TEXT,
        options: undefined,
    };
};
