import { StructuredPropertyEntity } from '@types';

export const getPropertyDisplayName = (property: StructuredPropertyEntity) => {
    return property.definition?.displayName || property.definition?.qualifiedName || property.urn;
};

export const createPropertyUrnMap = (properties: StructuredPropertyEntity[]) => {
    const results = new Map();
    properties.forEach((property) => {
        results.set(property.urn, property);
    });
    return results;
};
