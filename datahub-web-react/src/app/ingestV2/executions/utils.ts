import { Check } from '@phosphor-icons/react/dist/csr/Check';
import { CircleNotch } from '@phosphor-icons/react/dist/csr/CircleNotch';
import { Clock } from '@phosphor-icons/react/dist/csr/Clock';
import { Copy } from '@phosphor-icons/react/dist/csr/Copy';
import { Prohibit } from '@phosphor-icons/react/dist/csr/Prohibit';
import { Warning } from '@phosphor-icons/react/dist/csr/Warning';
import { WarningCircle } from '@phosphor-icons/react/dist/csr/WarningCircle';
import { X } from '@phosphor-icons/react/dist/csr/X';
import i18next from 'i18next';
import React from 'react';

import {
    EXECUTION_REQUEST_STATUS_ABORTED,
    EXECUTION_REQUEST_STATUS_ACTIVE,
    EXECUTION_REQUEST_STATUS_CANCELLED,
    EXECUTION_REQUEST_STATUS_DUPLICATE,
    EXECUTION_REQUEST_STATUS_FAILURE,
    EXECUTION_REQUEST_STATUS_LOADING,
    EXECUTION_REQUEST_STATUS_PENDING,
    EXECUTION_REQUEST_STATUS_ROLLBACK_FAILED,
    EXECUTION_REQUEST_STATUS_ROLLED_BACK,
    EXECUTION_REQUEST_STATUS_ROLLING_BACK,
    EXECUTION_REQUEST_STATUS_RUNNING,
    EXECUTION_REQUEST_STATUS_SUCCEEDED_WITH_WARNINGS,
    EXECUTION_REQUEST_STATUS_SUCCESS,
    EXECUTION_REQUEST_STATUS_UP_FOR_RETRY,
} from '@app/ingestV2/executions/constants';

import { ExecutionRequest } from '@types';

export function isExecutionRequestActive(executionRequest: ExecutionRequest) {
    return EXECUTION_REQUEST_STATUS_ACTIVE.includes(executionRequest?.result?.status ?? '');
}

export const getExecutionRequestStatusIcon = (status: string): React.ComponentType<any> => {
    switch (status) {
        case EXECUTION_REQUEST_STATUS_RUNNING:
            return CircleNotch;
        case EXECUTION_REQUEST_STATUS_SUCCESS:
            return Check;
        case EXECUTION_REQUEST_STATUS_SUCCEEDED_WITH_WARNINGS:
            return WarningCircle;
        case EXECUTION_REQUEST_STATUS_FAILURE:
            return X;
        case EXECUTION_REQUEST_STATUS_CANCELLED:
            return Prohibit;
        case EXECUTION_REQUEST_STATUS_UP_FOR_RETRY:
            return CircleNotch;
        case EXECUTION_REQUEST_STATUS_ROLLED_BACK:
            return Warning;
        case EXECUTION_REQUEST_STATUS_ROLLING_BACK:
            return CircleNotch;
        case EXECUTION_REQUEST_STATUS_ROLLBACK_FAILED:
            return X;
        case EXECUTION_REQUEST_STATUS_ABORTED:
            return X;
        case EXECUTION_REQUEST_STATUS_DUPLICATE:
            return Copy;
        case EXECUTION_REQUEST_STATUS_PENDING:
            return Clock;
        default:
            return CircleNotch;
    }
};

export const getExecutionRequestStatusDisplayText = (status: string) => {
    return (
        (status === EXECUTION_REQUEST_STATUS_RUNNING && i18next.t('ingestion:status.running')) ||
        (status === EXECUTION_REQUEST_STATUS_SUCCESS && i18next.t('ingestion:status.success')) ||
        (status === EXECUTION_REQUEST_STATUS_SUCCEEDED_WITH_WARNINGS && i18next.t('ingestion:status.success')) ||
        (status === EXECUTION_REQUEST_STATUS_FAILURE && i18next.t('ingestion:status.failed')) ||
        (status === EXECUTION_REQUEST_STATUS_CANCELLED && i18next.t('ingestion:status.cancelled')) ||
        (status === EXECUTION_REQUEST_STATUS_UP_FOR_RETRY && i18next.t('ingestion:status.upForRetry')) ||
        (status === EXECUTION_REQUEST_STATUS_ROLLED_BACK && i18next.t('ingestion:status.rolledBack')) ||
        (status === EXECUTION_REQUEST_STATUS_ROLLING_BACK && i18next.t('ingestion:status.rollingBack')) ||
        (status === EXECUTION_REQUEST_STATUS_ROLLBACK_FAILED && i18next.t('ingestion:status.rollbackFailed')) ||
        (status === EXECUTION_REQUEST_STATUS_ABORTED && i18next.t('ingestion:status.aborted')) ||
        (status === EXECUTION_REQUEST_STATUS_DUPLICATE && i18next.t('ingestion:status.duplicate')) ||
        (status === EXECUTION_REQUEST_STATUS_PENDING && i18next.t('ingestion:status.pending')) ||
        (status === EXECUTION_REQUEST_STATUS_LOADING && i18next.t('ingestion:status.loading')) ||
        /* untranslated-text -- raw status enum fallback */
        status
    );
};

export const getExecutionRequestStatusDisplayColor = (status: string) => {
    return (
        (status === EXECUTION_REQUEST_STATUS_RUNNING && 'blue') ||
        (status === EXECUTION_REQUEST_STATUS_SUCCESS && 'green') ||
        (status === EXECUTION_REQUEST_STATUS_SUCCEEDED_WITH_WARNINGS && 'yellow') ||
        (status === EXECUTION_REQUEST_STATUS_FAILURE && 'red') ||
        (status === EXECUTION_REQUEST_STATUS_UP_FOR_RETRY && 'yellow') ||
        (status === EXECUTION_REQUEST_STATUS_CANCELLED && 'gray') ||
        (status === EXECUTION_REQUEST_STATUS_ROLLED_BACK && 'yellow') ||
        (status === EXECUTION_REQUEST_STATUS_ROLLING_BACK && 'yellow') ||
        (status === EXECUTION_REQUEST_STATUS_ROLLBACK_FAILED && 'red') ||
        (status === EXECUTION_REQUEST_STATUS_ABORTED && 'gray') ||
        (status === EXECUTION_REQUEST_STATUS_DUPLICATE && 'gray') ||
        (status === EXECUTION_REQUEST_STATUS_PENDING && 'gray') ||
        'gray'
    );
};

export const getExecutionRequestSummaryText = (status: string) => {
    switch (status) {
        case EXECUTION_REQUEST_STATUS_RUNNING:
            return i18next.t('ingestion:executions.summaryRunning');
        case EXECUTION_REQUEST_STATUS_SUCCESS:
            return i18next.t('ingestion:executions.summarySuccess');
        case EXECUTION_REQUEST_STATUS_SUCCEEDED_WITH_WARNINGS:
            return i18next.t('ingestion:executions.summarySucceededWithWarnings');
        case EXECUTION_REQUEST_STATUS_FAILURE:
            return i18next.t('ingestion:executions.summaryFailure');
        case EXECUTION_REQUEST_STATUS_CANCELLED:
            return i18next.t('ingestion:executions.summaryCancelled');
        case EXECUTION_REQUEST_STATUS_ROLLED_BACK:
            return i18next.t('ingestion:executions.summaryRolledBack');
        case EXECUTION_REQUEST_STATUS_ROLLING_BACK:
            return i18next.t('ingestion:executions.summaryRollingBack');
        case EXECUTION_REQUEST_STATUS_ROLLBACK_FAILED:
            return i18next.t('ingestion:executions.summaryRollbackFailed');
        case EXECUTION_REQUEST_STATUS_ABORTED:
            return i18next.t('ingestion:executions.summaryAborted');
        default:
            return i18next.t('ingestion:executions.summaryUnknown');
    }
};
