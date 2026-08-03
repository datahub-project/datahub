import { CustomAssertionInfo, Maybe, SchemaFieldRef } from '@types';

/**
 * True when there is enough structured metadata to generate DatasetAssertionDescription-style copy.
 * Field-only customs (legacy type + field URN, no scope/operator) must fall back to type/description.
 */
export const hasStructuredAssertionDescriptionFields = (fields: {
    scope?: Maybe<string>;
    operator?: Maybe<string>;
    aggregation?: Maybe<string>;
    nativeType?: Maybe<string>;
}): boolean => {
    return !!(fields.scope && (fields.operator || fields.aggregation || fields.nativeType));
};

/**
 * Normalize fields from CustomAssertionInfo: prefer fields[], fall back to singular field.
 */
export const getCustomAssertionFields = (customAssertion?: Maybe<CustomAssertionInfo>): SchemaFieldRef[] => {
    if (!customAssertion) {
        return [];
    }
    if (customAssertion.fields && customAssertion.fields.length > 0) {
        return customAssertion.fields.filter((f): f is SchemaFieldRef => !!f);
    }
    if (customAssertion.field) {
        return [customAssertion.field];
    }
    return [];
};
