import { describe, expect, it, vi } from 'vitest';

import { getAssertionResultSeverityDisplay } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/shared/assertionResultSeverityUtils';

import { AssertionResult, AssertionResultSeverity, AssertionResultType } from '@types';

// vi.mock factories are hoisted before variable declarations, so icons must be
// declared with vi.hoisted() to be accessible inside the factory callbacks.
const { MockLowIcon, MockMediumIcon, MockHighIcon } = vi.hoisted(() => ({
    MockLowIcon: vi.fn(),
    MockMediumIcon: vi.fn(),
    MockHighIcon: vi.fn(),
}));

vi.mock('@src/images/incident-chart-bar-one.svg?react', () => ({ default: MockLowIcon }));
vi.mock('@src/images/incident-chart-bar-two.svg?react', () => ({ default: MockMediumIcon }));
vi.mock('@src/images/incident-chart-bar-three.svg?react', () => ({ default: MockHighIcon }));

const makeResult = (severity?: AssertionResultSeverity, type = AssertionResultType.Failure): AssertionResult =>
    ({ type, severity }) as AssertionResult;

describe('getAssertionResultSeverityDisplay', () => {
    it('returns undefined when result is undefined', () => {
        expect(getAssertionResultSeverityDisplay(undefined)).toBeUndefined();
    });

    it('returns undefined when result type is not Failure', () => {
        expect(
            getAssertionResultSeverityDisplay(makeResult(AssertionResultSeverity.High, AssertionResultType.Success)),
        ).toBeUndefined();
        expect(
            getAssertionResultSeverityDisplay(makeResult(AssertionResultSeverity.High, AssertionResultType.Error)),
        ).toBeUndefined();
        expect(
            getAssertionResultSeverityDisplay(makeResult(AssertionResultSeverity.High, AssertionResultType.Init)),
        ).toBeUndefined();
    });

    it('returns undefined when result is Failure but has no severity', () => {
        expect(getAssertionResultSeverityDisplay(makeResult(undefined))).toBeUndefined();
        expect(getAssertionResultSeverityDisplay(makeResult(null as any))).toBeUndefined();
    });

    it('returns High severity display for HIGH severity', () => {
        const display = getAssertionResultSeverityDisplay(makeResult(AssertionResultSeverity.High));
        expect(display?.label).toBe('High severity');
        expect(display?.icon).toBe(MockHighIcon);
    });

    it('returns Medium severity display for MEDIUM severity', () => {
        const display = getAssertionResultSeverityDisplay(makeResult(AssertionResultSeverity.Medium));
        expect(display?.label).toBe('Medium severity');
        expect(display?.icon).toBe(MockMediumIcon);
    });

    it('returns Low severity display for LOW severity', () => {
        const display = getAssertionResultSeverityDisplay(makeResult(AssertionResultSeverity.Low));
        expect(display?.label).toBe('Low severity');
        expect(display?.icon).toBe(MockLowIcon);
    });

    it('is case-insensitive for severity values', () => {
        expect(getAssertionResultSeverityDisplay(makeResult('high' as AssertionResultSeverity))?.label).toBe(
            'High severity',
        );
        expect(getAssertionResultSeverityDisplay(makeResult('medium' as AssertionResultSeverity))?.label).toBe(
            'Medium severity',
        );
        expect(getAssertionResultSeverityDisplay(makeResult('low' as AssertionResultSeverity))?.label).toBe(
            'Low severity',
        );
    });

    it('returns undefined for an unrecognized severity value', () => {
        expect(getAssertionResultSeverityDisplay(makeResult('CRITICAL' as AssertionResultSeverity))).toBeUndefined();
    });
});
