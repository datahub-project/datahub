import { hasAssertionDetails } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/AssertionDetailsSection';

import { Assertion, AssertionType, EntityType } from '@types';

function buildAssertion(overrides: Partial<Assertion['info']> = {}): Assertion {
    return {
        urn: 'urn:li:assertion:test',
        type: EntityType.Assertion,
        info: {
            type: AssertionType.Custom,
            ...overrides,
        },
    } as Assertion;
}

describe('hasAssertionDetails', () => {
    it('returns true when custom assertion has logic', () => {
        const assertion = buildAssertion({
            type: AssertionType.Custom,
            customAssertion: { type: 'test', entityUrn: 'urn:li:dataset:1', fields: [], logic: 'SELECT 1' },
        });
        expect(hasAssertionDetails(assertion)).toBe(true);
    });

    it('returns true when dataset assertion has logic', () => {
        const assertion = buildAssertion({
            type: AssertionType.Dataset,
            datasetAssertion: {
                datasetUrn: 'urn:li:dataset:1',
                scope: 'DATASET_COLUMN' as any,
                operator: 'GREATER_THAN' as any,
                logic: 'some logic',
            } as any,
        });
        expect(hasAssertionDetails(assertion)).toBe(true);
    });

    it('returns true when SQL assertion has statement', () => {
        const assertion = buildAssertion({
            type: AssertionType.Sql,
            sqlAssertion: { statement: 'SELECT COUNT(*) FROM table', entityUrn: 'urn:li:dataset:1' } as any,
        });
        expect(hasAssertionDetails(assertion)).toBe(true);
    });

    it('returns true when assertion has custom properties', () => {
        const assertion = buildAssertion({
            type: AssertionType.Custom,
            customProperties: [{ key: 'owner', value: 'team-a', associatedUrn: 'urn:li:assertion:test' }],
        });
        expect(hasAssertionDetails(assertion)).toBe(true);
    });

    it('returns false when assertion has no logic and no custom properties', () => {
        const assertion = buildAssertion({
            type: AssertionType.Custom,
            customAssertion: { type: 'test', entityUrn: 'urn:li:dataset:1', fields: [] },
        });
        expect(hasAssertionDetails(assertion)).toBe(false);
    });

    it('returns false when assertion has no info', () => {
        const assertion = { urn: 'urn:li:assertion:test', type: EntityType.Assertion } as Assertion;
        expect(hasAssertionDetails(assertion)).toBe(false);
    });

    it('returns false for empty custom properties array', () => {
        const assertion = buildAssertion({
            type: AssertionType.Custom,
            customProperties: [],
        });
        expect(hasAssertionDetails(assertion)).toBe(false);
    });
});
