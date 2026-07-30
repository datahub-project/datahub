import {
    customAssertionToDatasetAssertionView,
    getCustomAssertionFields,
    getStructuredAssertionViewForDisplay,
    hasStructuredCustomAssertionFields,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/shared/structuredAssertionUtils';

import { AssertionStdAggregation, AssertionStdOperator, AssertionType, DatasetAssertionScope } from '@types';

describe('structuredAssertionUtils', () => {
    const field = { urn: 'urn:li:schemaField:(urn:li:dataset:1,col_a)', path: 'col_a' };

    it('hasStructuredCustomAssertionFields detects structured metadata', () => {
        expect(hasStructuredCustomAssertionFields({ type: 'dbt', entityUrn: 'urn:li:dataset:1' })).toBe(false);
        expect(
            hasStructuredCustomAssertionFields({
                type: 'dbt',
                entityUrn: 'urn:li:dataset:1',
                operator: AssertionStdOperator.NotNull,
            }),
        ).toBe(true);
        expect(
            hasStructuredCustomAssertionFields({
                type: 'dbt',
                entityUrn: 'urn:li:dataset:1',
                field,
            }),
        ).toBe(true);
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

    it('customAssertionToDatasetAssertionView maps structured fields', () => {
        const view = customAssertionToDatasetAssertionView({
            type: 'great_expectations',
            entityUrn: 'urn:li:dataset:1',
            scope: DatasetAssertionScope.DatasetColumn,
            aggregation: AssertionStdAggregation.Identity,
            operator: AssertionStdOperator.NotNull,
            field,
            nativeType: 'expect_column_values_to_not_be_null',
            logic: 'SELECT 1',
        });
        expect(view).toMatchObject({
            datasetUrn: 'urn:li:dataset:1',
            scope: DatasetAssertionScope.DatasetColumn,
            aggregation: AssertionStdAggregation.Identity,
            operator: AssertionStdOperator.NotNull,
            nativeType: 'expect_column_values_to_not_be_null',
            logic: 'SELECT 1',
            fields: [field],
        });
    });

    it('customAssertionToDatasetAssertionView returns null without structured fields', () => {
        expect(
            customAssertionToDatasetAssertionView({
                type: 'opaque',
                entityUrn: 'urn:li:dataset:1',
                logic: 'SELECT 1',
            }),
        ).toBeNull();
    });

    it('getStructuredAssertionViewForDisplay handles DATASET and CUSTOM', () => {
        const datasetView = getStructuredAssertionViewForDisplay({
            type: AssertionType.Dataset,
            datasetAssertion: {
                datasetUrn: 'urn:li:dataset:1',
                scope: DatasetAssertionScope.DatasetRows,
                operator: AssertionStdOperator.EqualTo,
            },
        } as any);
        expect(datasetView?.datasetUrn).toBe('urn:li:dataset:1');

        const customView = getStructuredAssertionViewForDisplay({
            type: AssertionType.Custom,
            customAssertion: {
                type: 'dbt',
                entityUrn: 'urn:li:dataset:1',
                scope: DatasetAssertionScope.DatasetColumn,
                operator: AssertionStdOperator.NotNull,
                field,
            },
        } as any);
        expect(customView?.operator).toBe(AssertionStdOperator.NotNull);
    });
});
