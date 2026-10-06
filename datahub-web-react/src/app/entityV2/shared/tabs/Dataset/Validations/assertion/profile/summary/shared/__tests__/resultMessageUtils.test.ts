import { describe, expect, it } from 'vitest';

import {
    ResultStatusType,
    getDetailedErrorMessage,
    getFormattedReasonText,
    getFormattedResultText,
    getResultStatusText,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/shared/resultMessageUtils';

import { Assertion, AssertionResultErrorType, AssertionResultType, AssertionRunEvent, AssertionType } from '@types';

const makeRun = (type: AssertionResultType, errorType?: AssertionResultErrorType, errorMessage?: string) =>
    ({
        asserteeUrn: 'urn:li:dataset:test',
        assertionUrn: 'urn:li:assertion:test',
        runId: 'run-1',
        status: 'COMPLETE' as any,
        timestampMillis: 0,
        result: {
            type,
            error: errorType
                ? {
                      type: errorType,
                      properties: errorMessage ? [{ key: 'message', value: errorMessage }] : [],
                  }
                : undefined,
        },
    }) as AssertionRunEvent;

const makeAssertion = (type: AssertionType) =>
    ({
        urn: 'urn:li:assertion:test',
        type: 'ASSERTION' as any,
        platform: { urn: 'urn:li:dataPlatform:snowflake', type: 'DATA_PLATFORM' as any },
        info: { type },
    }) as Assertion;

describe('getFormattedResultText', () => {
    it('returns "Not completed" when result is undefined', () => {
        expect(getFormattedResultText(undefined)).toBe('Not completed');
    });

    it('returns "Passing" for Success', () => {
        expect(getFormattedResultText(AssertionResultType.Success)).toBe('Passing');
    });

    it('returns "Failing" for Failure', () => {
        expect(getFormattedResultText(AssertionResultType.Failure)).toBe('Failing');
    });

    it('returns "Error" for Error', () => {
        expect(getFormattedResultText(AssertionResultType.Error)).toBe('Error');
    });

    it('returns "Initializing" for Init', () => {
        expect(getFormattedResultText(AssertionResultType.Init)).toBe('Initializing');
    });

    it('returns undefined for an unrecognized result type', () => {
        expect(getFormattedResultText('UNKNOWN_RESULT_TYPE' as AssertionResultType)).toBeUndefined();
    });
});

describe('getFormattedReasonText', () => {
    it('returns initial data recorded message when result type is Init', () => {
        const run = makeRun(AssertionResultType.Init);
        const assertion = makeAssertion(AssertionType.Dataset);
        expect(getFormattedReasonText(assertion, run)).toBe(
            'Initial data recorded successfully. Assertion result will be available on the next evaluation.',
        );
    });

    it('returns error message from getResultErrorMessage when result type is Error', () => {
        const run = makeRun(AssertionResultType.Error, AssertionResultErrorType.SourceConnectionError);
        const assertion = makeAssertion(AssertionType.Dataset);
        expect(getFormattedReasonText(assertion, run)).toBe(
            'Unable to connect to source data platform. Please check the connection.',
        );
    });

    it('returns conditions met message for Dataset assertion with Success result', () => {
        const run = makeRun(AssertionResultType.Success);
        const assertion = makeAssertion(AssertionType.Dataset);
        expect(getFormattedReasonText(assertion, run)).toBe('The expected conditions were met');
    });

    it('returns conditions not met message for Dataset assertion with Failure result', () => {
        const run = makeRun(AssertionResultType.Failure);
        const assertion = makeAssertion(AssertionType.Dataset);
        expect(getFormattedReasonText(assertion, run)).toBe('The expected conditions were not met.');
    });

    it('returns conditions met message for Custom assertion with Success result', () => {
        const run = makeRun(AssertionResultType.Success);
        const assertion = makeAssertion(AssertionType.Custom);
        expect(getFormattedReasonText(assertion, run)).toBe('The expected conditions were met');
    });

    it('returns conditions not met message for Custom assertion with Failure result', () => {
        const run = makeRun(AssertionResultType.Failure);
        const assertion = makeAssertion(AssertionType.Custom);
        expect(getFormattedReasonText(assertion, run)).toBe('The expected conditions were not met.');
    });

    it('returns no reason provided message for unsupported assertion type', () => {
        const run = makeRun(AssertionResultType.Failure);
        const assertion = makeAssertion(AssertionType.Freshness);
        expect(getFormattedReasonText(assertion, run)).toBe('No reason provided');
    });
});

describe('getDetailedErrorMessage', () => {
    it('returns the error message property when present', () => {
        const run = makeRun(AssertionResultType.Error, AssertionResultErrorType.SourceQueryFailed, 'Query timed out');
        expect(getDetailedErrorMessage(run)).toBe('Query timed out');
    });

    it('returns undefined when there is no error', () => {
        const run = makeRun(AssertionResultType.Failure);
        expect(getDetailedErrorMessage(run)).toBeUndefined();
    });

    it('returns undefined when error has no properties', () => {
        const run = makeRun(AssertionResultType.Error, AssertionResultErrorType.UnknownError);
        expect(getDetailedErrorMessage(run)).toBeUndefined();
    });

    it('returns undefined when the message key is absent from properties', () => {
        const run = {
            ...makeRun(AssertionResultType.Error, AssertionResultErrorType.UnknownError),
            result: {
                type: AssertionResultType.Error,
                error: {
                    type: AssertionResultErrorType.UnknownError,
                    properties: [{ key: 'other', value: 'irrelevant' }],
                },
            },
        } as AssertionRunEvent;
        expect(getDetailedErrorMessage(run)).toBeUndefined();
    });
});

describe('getResultStatusText', () => {
    it('returns "Passing" for Success + LATEST', () => {
        expect(getResultStatusText(AssertionResultType.Success, ResultStatusType.LATEST)).toBe('Passing');
    });

    it('returns "Passed" for Success + HISTORICAL', () => {
        expect(getResultStatusText(AssertionResultType.Success, ResultStatusType.HISTORICAL)).toBe('Passed');
    });

    it('returns "Failing" for Failure + LATEST', () => {
        expect(getResultStatusText(AssertionResultType.Failure, ResultStatusType.LATEST)).toBe('Failing');
    });

    it('returns "Failed" for Failure + HISTORICAL', () => {
        expect(getResultStatusText(AssertionResultType.Failure, ResultStatusType.HISTORICAL)).toBe('Failed');
    });

    it('returns "Error" for Error regardless of status type', () => {
        expect(getResultStatusText(AssertionResultType.Error, ResultStatusType.LATEST)).toBe('Error');
        expect(getResultStatusText(AssertionResultType.Error, ResultStatusType.HISTORICAL)).toBe('Error');
    });

    it('returns "Initializing" for Init + LATEST', () => {
        expect(getResultStatusText(AssertionResultType.Init, ResultStatusType.LATEST)).toBe('Initializing');
    });

    it('returns "Initialized" for Init + HISTORICAL', () => {
        expect(getResultStatusText(AssertionResultType.Init, ResultStatusType.HISTORICAL)).toBe('Initialized');
    });

    it('throws for an unsupported result type', () => {
        expect(() => getResultStatusText('UNSUPPORTED' as AssertionResultType, ResultStatusType.LATEST)).toThrow(
            'Unsupported Assertion Result Type UNSUPPORTED provided.',
        );
    });
});
