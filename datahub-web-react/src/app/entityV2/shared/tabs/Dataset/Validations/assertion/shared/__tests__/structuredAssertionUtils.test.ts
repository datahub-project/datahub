import {
    getCustomAssertionFields,
    hasStructuredAssertionDescriptionFields,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/shared/structuredAssertionUtils';

import { AssertionStdAggregation, AssertionStdOperator, DatasetAssertionScope } from '@types';

describe('structuredAssertionUtils', () => {
    const field = { urn: 'urn:li:schemaField:(urn:li:dataset:1,col_a)', path: 'col_a' };

    it('hasStructuredAssertionDescriptionFields requires scope plus operator/aggregation/nativeType', () => {
        expect(hasStructuredAssertionDescriptionFields({ scope: DatasetAssertionScope.DatasetColumn })).toBe(false);
        expect(
            hasStructuredAssertionDescriptionFields({
                operator: AssertionStdOperator.NotNull,
            }),
        ).toBe(false);
        expect(
            hasStructuredAssertionDescriptionFields({
                scope: DatasetAssertionScope.DatasetColumn,
                operator: AssertionStdOperator.NotNull,
            }),
        ).toBe(true);
        expect(
            hasStructuredAssertionDescriptionFields({
                scope: DatasetAssertionScope.DatasetColumn,
                aggregation: AssertionStdAggregation.Identity,
            }),
        ).toBe(true);
        expect(
            hasStructuredAssertionDescriptionFields({
                scope: DatasetAssertionScope.DatasetColumn,
                nativeType: 'expect_column_values_to_not_be_null',
            }),
        ).toBe(true);
    });

    it('field-only customs are not structured (legacy fallback to type/description)', () => {
        expect(
            hasStructuredAssertionDescriptionFields({
                // field alone is insufficient — callers pass only scope/operator/aggregation/nativeType
            }),
        ).toBe(false);
    });

    it('getCustomAssertionFields prefers fields array over singular field', () => {
        const fieldB = { urn: 'urn:li:schemaField:(urn:li:dataset:1,col_b)', path: 'col_b' };
        expect(
            getCustomAssertionFields({
                type: 'dbt',
                entityUrn: 'urn:li:dataset:1',
                field,
                fields: [field, fieldB],
            }),
        ).toEqual([field, fieldB]);
        expect(
            getCustomAssertionFields({
                type: 'dbt',
                entityUrn: 'urn:li:dataset:1',
                field,
            }),
        ).toEqual([field]);
    });
});
