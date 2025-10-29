
import { PropertyRow } from '@app/entity/shared/tabs/Properties/types';
import { GenericEntityProperties } from '@app/entity/shared/types';
import { getStructuredPropertyValue } from '@app/entity/shared/utils';

import { PropertyValue, StructuredPropertiesEntry } from '@types';

const typeNameToType = {
    StringValue: { type: 'string', nativeDataType: 'text' },
    NumberValue: { type: 'number', nativeDataType: 'float' },
};

function mapStructuredPropertyValues(structuredPropertiesEntry: StructuredPropertiesEntry) {
    return structuredPropertiesEntry.values
        .filter((value) => !!value)
        .map((value) => ({
            value: getStructuredPropertyValue(value as PropertyValue),
            entity:
                structuredPropertiesEntry.valueEntities?.find(
                    (entity) => entity?.urn === getStructuredPropertyValue(value as PropertyValue),
                ) || null,
        }));
}

export function mapStructuredPropertyToPropertyRow(structuredPropertiesEntry: StructuredPropertiesEntry) {
    const { displayName, qualifiedName } = structuredPropertiesEntry.structuredProperty.definition;
    return {
        displayName: displayName || qualifiedName,
        qualifiedName,
        values: mapStructuredPropertyValues(structuredPropertiesEntry),
        dataType: structuredPropertiesEntry.structuredProperty.definition.valueType,
        structuredProperty: structuredPropertiesEntry.structuredProperty,
        type:
            structuredPropertiesEntry.values[0] && structuredPropertiesEntry.values[0].__typename
                ? {
                      type: typeNameToType[structuredPropertiesEntry.values[0].__typename].type,
                      nativeDataType: typeNameToType[structuredPropertiesEntry.values[0].__typename].nativeDataType,
                  }
                : undefined,
        associatedUrn: structuredPropertiesEntry.associatedUrn,
        attribution: structuredPropertiesEntry.attribution,
    };
}

function findAllSubstrings(s: string): Array<string> {
    const substrings: Array<string> = [];

    for (let i = 0; i < s.length; i++) {
        if (s[i] === '.') {
            substrings.push(s.substring(0, i));
        }
    }
    substrings.push(s);
    return substrings;
}

function createParentPropertyRow(displayName: string, qualifiedName: string): PropertyRow {
    return {
        displayName,
        qualifiedName,
        isParentRow: true,
    };
}
