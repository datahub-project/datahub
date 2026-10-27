import { describe, expect, it } from 'vitest';

import {
    getAssertionType,
    getAssertionsSummary,
    getCustomAssertionType,
} from '@app/entityV2/shared/tabs/Dataset/Validations/acrylUtils';
import {
    Assertion,
    AssertionInfo,
    AssertionResultType,
    AssertionType,
    DatasetAssertionScope,
} from '@src/types.generated';

describe('getAssertionsSummary', () => {
    it('distinguishes initializing and never-run assertions', () => {
        const assertions = [
            {
                runEvents: {
                    runEvents: [{ result: { type: AssertionResultType.Init } }],
                },
            },
            {
                runEvents: {
                    runEvents: [{ result: { type: AssertionResultType.Success } }],
                },
            },
            { runEvents: { runEvents: [] } },
        ] as unknown as Assertion[];

        expect(getAssertionsSummary(assertions)).toEqual({
            passing: 1,
            failing: 0,
            erroring: 0,
            initializing: 1,
            notRunning: 1,
            total: 1,
            totalAssertions: 3,
        });
    });
});

const DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:dbt,my_db.my_schema.events,PROD)';

const buildCustomAssertion = (customType: string): Assertion =>
    ({
        urn: 'urn:li:assertion:1',
        info: {
            type: AssertionType.Custom,
            customAssertion: {
                type: customType,
                entityUrn: DATASET_URN,
                scope: DatasetAssertionScope.DatasetColumn,
            },
        } as AssertionInfo,
    }) as Assertion;

const volumeAssertion = { urn: 'urn:li:assertion:2', info: { type: AssertionType.Volume } } as Assertion;

// The list's server-side type filter and sort use the stored assertionType, so the label must too.
describe('getAssertionType', () => {
    it('uses the stored type for CUSTOM assertions, not the reported custom type', () => {
        expect(getAssertionType(buildCustomAssertion('dbt'))).toBe(AssertionType.Custom);
        expect(getAssertionType(buildCustomAssertion('FRESHNESS'))).toBe(AssertionType.Custom);
    });

    it('leaves native assertion types unchanged', () => {
        expect(getAssertionType(volumeAssertion)).toBe(AssertionType.Volume);
    });
});

describe('getCustomAssertionType', () => {
    it('returns the reported type of a CUSTOM assertion as-is', () => {
        expect(getCustomAssertionType(buildCustomAssertion('My Custom Category'))).toBe('My Custom Category');
    });

    it('ignores non-CUSTOM assertions', () => {
        expect(getCustomAssertionType(volumeAssertion)).toBeUndefined();
    });
});
