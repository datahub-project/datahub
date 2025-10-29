
import styled from 'styled-components';
import { ROLLING_BACK, RUNNING } from '@app/ingest/source/utils';
import { ExecutionRequest } from '@types';

export function isExecutionRequestActive(executionRequest: ExecutionRequest) {
    return executionRequest.result?.status === RUNNING || executionRequest.result?.status === ROLLING_BACK;
}
