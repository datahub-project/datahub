import {
    EXECUTION_REQUEST_STATUS_ABORTED,
    EXECUTION_REQUEST_STATUS_CANCELLED,
    EXECUTION_REQUEST_STATUS_FAILURE,
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
    return (
        executionRequest.result?.status === EXECUTION_REQUEST_STATUS_RUNNING ||
        executionRequest.result?.status === EXECUTION_REQUEST_STATUS_ROLLING_BACK
    );
}

export const getExecutionRequestStatusIcon = (status: string) => {
    return (
        (status === EXECUTION_REQUEST_STATUS_RUNNING && 'CircleNotch') ||
        (status === EXECUTION_REQUEST_STATUS_SUCCESS && 'Check') ||
        (status === EXECUTION_REQUEST_STATUS_SUCCEEDED_WITH_WARNINGS && 'ExclamationMark') ||
        (status === EXECUTION_REQUEST_STATUS_FAILURE && 'X') ||
        (status === EXECUTION_REQUEST_STATUS_CANCELLED && 'Prohibit') ||
        (status === EXECUTION_REQUEST_STATUS_UP_FOR_RETRY && 'CircleNotch') ||
        (status === EXECUTION_REQUEST_STATUS_ROLLED_BACK && 'Warning') ||
        (status === EXECUTION_REQUEST_STATUS_ROLLING_BACK && 'CircleNotch') ||
        (status === EXECUTION_REQUEST_STATUS_ROLLBACK_FAILED && 'X') ||
        (status === EXECUTION_REQUEST_STATUS_ABORTED && 'X') ||
        'Clock'
    );
};

export const getExecutionRequestStatusDisplayText = (status: string) => {
    return (
        (status === EXECUTION_REQUEST_STATUS_RUNNING && 'Running') ||
        (status === EXECUTION_REQUEST_STATUS_SUCCESS && 'Success') ||
        (status === EXECUTION_REQUEST_STATUS_SUCCEEDED_WITH_WARNINGS && 'Success') ||
        (status === EXECUTION_REQUEST_STATUS_FAILURE && 'Failed') ||
        (status === EXECUTION_REQUEST_STATUS_CANCELLED && 'Cancelled') ||
        (status === EXECUTION_REQUEST_STATUS_UP_FOR_RETRY && 'Up for Retry') ||
        (status === EXECUTION_REQUEST_STATUS_ROLLED_BACK && 'Rolled Back') ||
        (status === EXECUTION_REQUEST_STATUS_ROLLING_BACK && 'Rolling Back') ||
        (status === EXECUTION_REQUEST_STATUS_ROLLBACK_FAILED && 'Rollback Failed') ||
        (status === EXECUTION_REQUEST_STATUS_ABORTED && 'Aborted') ||
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
        'gray'
    );
};

export const getExecutionRequestSummaryText = (status: string) => {
    switch (status) {
        case EXECUTION_REQUEST_STATUS_RUNNING:
            return 'Ingestion is running...';
        case EXECUTION_REQUEST_STATUS_SUCCESS:
            return 'Ingestion completed with no errors or warnings.';
        case EXECUTION_REQUEST_STATUS_SUCCEEDED_WITH_WARNINGS:
            return 'Ingestion completed with some warnings.';
        case EXECUTION_REQUEST_STATUS_FAILURE:
            return 'Ingestion failed to complete, or completed with errors.';
        case EXECUTION_REQUEST_STATUS_CANCELLED:
            return 'Ingestion was cancelled.';
        case EXECUTION_REQUEST_STATUS_ROLLED_BACK:
            return 'Ingestion was rolled back.';
        case EXECUTION_REQUEST_STATUS_ROLLING_BACK:
            return 'Ingestion is in the process of rolling back.';
        case EXECUTION_REQUEST_STATUS_ROLLBACK_FAILED:
            return 'Ingestion rollback failed.';
        case EXECUTION_REQUEST_STATUS_ABORTED:
            return 'Ingestion job got aborted due to worker restart.';
        default:
            return 'Ingestion status not recognized.';
    }
};
