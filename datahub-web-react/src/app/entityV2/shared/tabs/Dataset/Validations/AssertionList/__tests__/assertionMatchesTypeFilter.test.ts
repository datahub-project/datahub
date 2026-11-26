import { describe, expect, it } from 'vitest';

import { assertionMatchesTypeFilter } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/utils';

import { Assertion, AssertionType } from '@types';

describe('assertionMatchesTypeFilter', () => {
    const greatExpectationsAssertion = {
        urn: 'urn:li:assertion:1',
        info: {
            type: AssertionType.Custom,
            customAssertion: {
                type: 'great_expectations',
                entityUrn: 'urn:li:dataset:1',
            },
        },
    } as Assertion;

    it('matches Custom for an externally reported assertion', () => {
        expect(assertionMatchesTypeFilter(greatExpectationsAssertion, [AssertionType.Custom])).toBe(true);
    });

    it('does not match on the reported custom type', () => {
        expect(assertionMatchesTypeFilter(greatExpectationsAssertion, ['GREAT_EXPECTATIONS' as AssertionType])).toBe(
            false,
        );
    });

    it('does not match unrelated types', () => {
        expect(assertionMatchesTypeFilter(greatExpectationsAssertion, [AssertionType.Volume])).toBe(false);
    });

    it('matches everything when no types are selected', () => {
        expect(assertionMatchesTypeFilter(greatExpectationsAssertion, [])).toBe(true);
    });
});
