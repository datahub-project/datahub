import { describe, expect, it } from 'vitest';

import { getAssertionsSummary } from '@app/entityV2/shared/tabs/Dataset/Validations/acrylUtils';
import { Assertion, AssertionResultType } from '@src/types.generated';

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
