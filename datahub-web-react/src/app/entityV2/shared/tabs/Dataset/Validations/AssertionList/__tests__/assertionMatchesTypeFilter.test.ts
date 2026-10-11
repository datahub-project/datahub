import { describe, expect, it } from 'vitest';

import { assertionMatchesTypeFilter } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/utils';

import { Assertion, AssertionType } from '@types';

describe('assertionMatchesTypeFilter', () => {
    const structuredCustom = {
        urn: 'urn:li:assertion:1',
        info: {
            type: AssertionType.Custom,
            customAssertion: {
                type: 'great_expectations',
                entityUrn: 'urn:li:dataset:1',
            },
        },
    } as Assertion;

    it('matches provider subtype when that subtype is selected', () => {
        expect(assertionMatchesTypeFilter(structuredCustom, ['GREAT_EXPECTATIONS' as AssertionType])).toBe(true);
    });

    it('matches Custom filter even when getAssertionType resolves to a provider subtype', () => {
        expect(assertionMatchesTypeFilter(structuredCustom, [AssertionType.Custom])).toBe(true);
    });

    it('does not match unrelated types', () => {
        expect(assertionMatchesTypeFilter(structuredCustom, [AssertionType.Volume])).toBe(false);
    });

    it('matches everything when no types are selected', () => {
        expect(assertionMatchesTypeFilter(structuredCustom, [])).toBe(true);
    });
});
