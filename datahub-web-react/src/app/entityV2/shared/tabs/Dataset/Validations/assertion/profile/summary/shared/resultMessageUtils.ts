import {
    Assertion,
    AssertionResult,
    AssertionResultType,
    AssertionRunEvent,
    AssertionType,
} from '../../../../../../../../../../types.generated';
import { getResultErrorMessage } from '../../../../assertionUtils';

export const getFormattedResultText = (result?: AssertionResultType) => {
    if (result === undefined) {
        return 'Not completed';
    }
    if (result === AssertionResultType.Success) {
        return 'Passing';
    }
    if (result === AssertionResultType.Failure) {
        return 'Failing';
    }
    if (result === AssertionResultType.Error) {
        return 'Error';
    }
    if (result === AssertionResultType.Init) {
        return 'Initializing';
    }
    return undefined;
};

const getFormattedReasonTextForDefaultAssertion = (run: AssertionRunEvent) => {
    const type = run.result?.type;
    switch (type) {
        case AssertionResultType.Success: {
            return `The expected conditions were met`;
        }
        case AssertionResultType.Error: {
            let message = `Assertion encountered an error during execution.`;

            const maybeError = run.result?.error;
            const maybeType = maybeError?.type;
            const maybeErrorMessage = maybeError?.properties?.find((e) => e.key === 'message')?.value;
            if (maybeType) {
                message = `Assertion execution encountered an error with type ${maybeType}.`;
            }
            if (maybeErrorMessage) {
                message += ` "${maybeErrorMessage}"`;
            }
            return message;
        }
        default: {
            return `The expected conditions were not met.`;
        }
    }
};

export const getFormattedReasonText = (assertion: Assertion, run: AssertionRunEvent) => {
    if (run?.result?.type === AssertionResultType.Init) {
        return 'Initial data recorded successfully. Assertion result will be available on the next evaluation.';
    }
    if (run?.result?.type === AssertionResultType.Error) {
        const formattedError = getResultErrorMessage(run?.result);
        return `${formattedError}`;
    }

    // Some historical assertion results may not have asseriton info...
    // so we coalesce the current info onto there to avoid blanks
    const coalescedResult: AssertionResult | undefined | null = run.result && {
        ...run.result,
    };
    const coalescedRun: AssertionRunEvent = {
        ...run,
        result: coalescedResult,
    };
    switch (assertion.info?.type) {
        case AssertionType.Dataset:
            return getFormattedReasonTextForDefaultAssertion(coalescedRun);
        case AssertionType.Custom:
            return getFormattedReasonTextForDefaultAssertion(coalescedRun);
        default:
            return 'No reason provided';
    }
};

export const getFormattedExpectedResultText = (): string | undefined => {
    return undefined;
};

export const getDetailedErrorMessage = (run: AssertionRunEvent) => {
    return run.result?.error?.properties?.find((property) => property.key === 'message')?.value || undefined;
};

export enum ResultStatusType {
    LATEST = 'latest',
    HISTORICAL = 'historical',
}

/**
 * Returns the display text assoociated with an AssertionResultType
 */
export const getResultStatusText = (result: AssertionResultType, type: ResultStatusType) => {
    switch (result) {
        case AssertionResultType.Success:
            return type === ResultStatusType.LATEST ? 'Passing' : 'Passed';
        case AssertionResultType.Failure:
            return type === ResultStatusType.LATEST ? 'Failing' : 'Failed';
        case AssertionResultType.Error:
            return 'Error';
        case AssertionResultType.Init:
            return type === ResultStatusType.LATEST ? 'Initializing' : 'Initialized';
        default:
            throw new Error(`Unsupported Assertion Result Type ${result} provided.`);
    }
};
