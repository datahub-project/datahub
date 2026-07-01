import i18next from 'i18next';

import { getResultErrorMessage } from '@app/entityV2/shared/tabs/Dataset/Validations/assertionUtils';

import {
    Assertion,
    AssertionInfo,
    AssertionResult,
    AssertionResultType,
    AssertionRunEvent,
    AssertionType,
    Maybe,
} from '@types';

export const getFormattedResultText = (result?: AssertionResultType) => {
    if (result === undefined) {
        return i18next.t('entity.profile.validations:resultMessage.notCompleted');
    }
    if (result === AssertionResultType.Success) {
        return i18next.t('entity.profile.validations:resultMessage.passing');
    }
    if (result === AssertionResultType.Failure) {
        return i18next.t('entity.profile.validations:resultMessage.failing');
    }
    if (result === AssertionResultType.Error) {
        return i18next.t('entity.profile.validations:resultMessage.error');
    }
    if (result === AssertionResultType.Init) {
        return i18next.t('entity.profile.validations:resultMessage.initializing');
    }
    return undefined;
};

const getFormattedReasonTextForDefaultAssertion = (run: AssertionRunEvent) => {
    const type = run.result?.type;
    switch (type) {
        case AssertionResultType.Success: {
            return i18next.t('entity.profile.validations:resultMessage.conditionsMet');
        }
        case AssertionResultType.Error: {
            let message: string = i18next.t('entity.profile.validations:resultMessage.assertionEncounteredError');

            const maybeError = run.result?.error;
            const maybeType = maybeError?.type;
            const maybeErrorMessage = maybeError?.properties?.find((e) => e.key === 'message')?.value;
            if (maybeType) {
                message = i18next.t('entity.profile.validations:resultMessage.assertionEncounteredErrorWithType', {
                    maybeType,
                });
            }
            if (maybeErrorMessage) {
                message += ` "${maybeErrorMessage}"`;
            }
            return message;
        }
        default: {
            return i18next.t('entity.profile.validations:resultMessage.conditionsNotMet');
        }
    }
};

export const getFormattedReasonText = (assertion: Assertion, run: AssertionRunEvent) => {
    if (run?.result?.type === AssertionResultType.Init) {
        return i18next.t('entity.profile.validations:resultMessage.initialDataRecorded');
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
            return i18next.t('entity.profile.validations:resultMessage.noReasonProvided');
    }
};

export const getFormattedExpectedResultText = (
    _assertion?: Maybe<AssertionInfo>,
    _run?: AssertionRunEvent,
): string | undefined => {
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
            return type === ResultStatusType.LATEST
                ? i18next.t('entity.profile.validations:resultPill.passing')
                : i18next.t('entity.profile.validations:resultPill.passedHistorical');
        case AssertionResultType.Failure:
            return type === ResultStatusType.LATEST
                ? i18next.t('entity.profile.validations:resultPill.failing')
                : i18next.t('entity.profile.validations:resultPill.failedHistorical');
        case AssertionResultType.Error:
            return i18next.t('entity.profile.validations:resultPill.error');
        case AssertionResultType.Init:
            return type === ResultStatusType.LATEST
                ? i18next.t('entity.profile.validations:resultPill.initializing')
                : i18next.t('entity.profile.validations:resultPill.initialized');
        default:
            throw new Error(`Unsupported Assertion Result Type ${result} provided.`);
    }
};
