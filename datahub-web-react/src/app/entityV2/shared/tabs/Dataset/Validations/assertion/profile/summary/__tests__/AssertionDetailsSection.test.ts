import { describe, expect, it } from 'vitest';

import {
    getLogicFromAssertion,
    hasAssertionDetails,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/AssertionDetailsSection';

import { Assertion, AssertionType } from '@types';

describe('AssertionDetailsSection helpers', () => {
    it('extracts logic from CUSTOM, DATASET, and SQL assertions', () => {
        expect(
            getLogicFromAssertion({
                info: {
                    type: AssertionType.Custom,
                    customAssertion: { logic: 'SELECT 1', type: 'dbt', entityUrn: 'u' },
                },
            } as Assertion),
        ).toBe('SELECT 1');

        expect(
            getLogicFromAssertion({
                info: {
                    type: AssertionType.Dataset,
                    datasetAssertion: { logic: 'WHERE x IS NOT NULL', datasetUrn: 'u' },
                },
            } as Assertion),
        ).toBe('WHERE x IS NOT NULL');

        expect(
            getLogicFromAssertion({
                info: {
                    type: AssertionType.Sql,
                    sqlAssertion: { statement: 'SELECT COUNT(*) FROM t', entityUrn: 'u' },
                },
            } as Assertion),
        ).toBe('SELECT COUNT(*) FROM t');
    });

    it('returns null when there is no logic for the assertion type', () => {
        expect(
            getLogicFromAssertion({
                info: { type: AssertionType.Freshness },
            } as Assertion),
        ).toBeNull();
        expect(getLogicFromAssertion({} as Assertion)).toBeNull();
    });

    it('hasAssertionDetails is true when logic or customProperties are present', () => {
        expect(
            hasAssertionDetails({
                info: {
                    type: AssertionType.Custom,
                    customAssertion: { type: 'dbt', entityUrn: 'u', logic: 'SELECT 1' },
                },
            } as Assertion),
        ).toBe(true);

        expect(
            hasAssertionDetails({
                info: {
                    type: AssertionType.Custom,
                    customAssertion: { type: 'dbt', entityUrn: 'u' },
                    customProperties: [{ key: 'suite', value: 'demo' }],
                },
            } as Assertion),
        ).toBe(true);

        expect(
            hasAssertionDetails({
                info: {
                    type: AssertionType.Custom,
                    customAssertion: { type: 'dbt', entityUrn: 'u' },
                },
            } as Assertion),
        ).toBe(false);
    });
});
