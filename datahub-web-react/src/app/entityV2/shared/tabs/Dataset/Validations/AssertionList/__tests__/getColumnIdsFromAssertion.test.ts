import { describe, expect, it } from 'vitest';

import { getColumnIdsFromAssertion } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/utils';

import { Assertion, AssertionType } from '@types';

describe('getColumnIdsFromAssertion', () => {
    it('returns field path for FIELD assertions', () => {
        const assertion = {
            info: {
                type: AssertionType.Field,
                fieldAssertion: {
                    fieldMetricAssertion: { field: { path: 'user_id', urn: 'urn:li:schemaField:1' } },
                },
            },
        } as unknown as Assertion;
        expect(getColumnIdsFromAssertion(assertion)).toEqual(['user_id']);
    });

    it('returns all customAssertion fields for CUSTOM assertions', () => {
        const assertion = {
            info: {
                type: AssertionType.Custom,
                customAssertion: {
                    type: 'dbt',
                    entityUrn: 'urn:li:dataset:1',
                    fields: [
                        { path: 'col_a', urn: 'urn:li:schemaField:a' },
                        { path: 'col_b', urn: 'urn:li:schemaField:b' },
                    ],
                },
            },
        } as unknown as Assertion;
        expect(getColumnIdsFromAssertion(assertion)).toEqual(['col_a', 'col_b']);
    });

    it('falls back to singular customAssertion.field', () => {
        const assertion = {
            info: {
                type: AssertionType.Custom,
                customAssertion: {
                    type: 'dbt',
                    entityUrn: 'urn:li:dataset:1',
                    field: { path: 'legacy_col', urn: 'urn:li:schemaField:l' },
                },
            },
        } as unknown as Assertion;
        expect(getColumnIdsFromAssertion(assertion)).toEqual(['legacy_col']);
    });

    it('returns datasetAssertion fields for legacy DATASET assertions', () => {
        const assertion = {
            info: {
                type: AssertionType.Dataset,
                datasetAssertion: {
                    datasetUrn: 'urn:li:dataset:1',
                    fields: [{ path: 'legacy_ds_col', urn: 'urn:li:schemaField:d' }],
                },
            },
        } as unknown as Assertion;
        expect(getColumnIdsFromAssertion(assertion)).toEqual(['legacy_ds_col']);
    });

    it('returns empty list when no columns are targeted', () => {
        expect(
            getColumnIdsFromAssertion({
                info: { type: AssertionType.Freshness },
            } as unknown as Assertion),
        ).toEqual([]);
    });
});
