import { AssertionInfo, AssertionType, CustomAssertionInfo, DatasetAssertionInfo, Maybe, SchemaFieldRef } from '@types';

/**
 * Returns true when a CustomAssertionInfo carries structured fields that can drive
 * DatasetAssertionDescription-style auto-generated copy (scope/operator/etc.).
 */
export const hasStructuredCustomAssertionFields = (customAssertion?: Maybe<CustomAssertionInfo>): boolean => {
    if (!customAssertion) {
        return false;
    }
    return !!(
        customAssertion.scope ||
        customAssertion.operator ||
        customAssertion.aggregation ||
        customAssertion.nativeType ||
        (customAssertion.fields && customAssertion.fields.length > 0) ||
        customAssertion.field
    );
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

/**
 * Map expanded CustomAssertionInfo into a DatasetAssertionInfo-shaped view so we can
 * reuse DatasetAssertionDescription / plain-text helpers without duplicating i18n logic.
 *
 * Returns null when the custom assertion has no structured display fields.
 */
export const customAssertionToDatasetAssertionView = (
    customAssertion?: Maybe<CustomAssertionInfo>,
): DatasetAssertionInfo | null => {
    if (!customAssertion || !hasStructuredCustomAssertionFields(customAssertion)) {
        return null;
    }
    const fields = getCustomAssertionFields(customAssertion);
    return {
        datasetUrn: customAssertion.entityUrn,
        scope: customAssertion.scope ?? undefined,
        aggregation: customAssertion.aggregation ?? undefined,
        operator: customAssertion.operator as DatasetAssertionInfo['operator'],
        parameters: customAssertion.parameters ?? undefined,
        fields: fields.length > 0 ? fields : undefined,
        nativeType: customAssertion.nativeType ?? undefined,
        nativeParameters: customAssertion.nativeParameters ?? undefined,
        logic: customAssertion.logic ?? undefined,
    } as DatasetAssertionInfo;
};

/**
 * Read-path normalization: prefer live structured CUSTOM fields; for legacy DATASET
 * assertions, return datasetAssertion unchanged. Does not rewrite storage.
 */
export const getStructuredAssertionViewForDisplay = (
    assertionInfo?: Maybe<AssertionInfo>,
): DatasetAssertionInfo | null => {
    if (!assertionInfo) {
        return null;
    }
    if (assertionInfo.type === AssertionType.Custom) {
        return customAssertionToDatasetAssertionView(assertionInfo.customAssertion);
    }
    if (assertionInfo.type === AssertionType.Dataset && assertionInfo.datasetAssertion) {
        return assertionInfo.datasetAssertion as DatasetAssertionInfo;
    }
    return null;
};
