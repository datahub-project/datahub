import { describe, expect, it } from 'vitest';

import { assertionMatchesTypeFilter } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/utils';

import { Assertion, AssertionType, DatasetAssertionScope } from '@types';

describe('assertionMatchesTypeFilter', () => {
    const columnScopedCustom = {
        urn: 'urn:li:assertion:1',
        info: {
            type: AssertionType.Custom,
            customAssertion: {
                type: 'great_expectations',
                entityUrn: 'urn:li:dataset:1',
                scope: DatasetAssertionScope.DatasetColumn,
            },
        },
    } as Assertion;

    it('matches the structural category it resolves to', () => {
        expect(assertionMatchesTypeFilter(columnScopedCustom, [AssertionType.Field])).toBe(true);
    });

    it('matches Custom filter even when getAssertionType resolves to a structural category', () => {
        expect(assertionMatchesTypeFilter(columnScopedCustom, [AssertionType.Custom])).toBe(true);
    });

    it('does not match unrelated types', () => {
        expect(assertionMatchesTypeFilter(columnScopedCustom, [AssertionType.Volume])).toBe(false);
    });

    it('matches everything when no types are selected', () => {
        expect(assertionMatchesTypeFilter(columnScopedCustom, [])).toBe(true);
    });
});
