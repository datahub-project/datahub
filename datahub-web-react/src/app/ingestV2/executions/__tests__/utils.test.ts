/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import {
    EXECUTION_REQUEST_STATUS_FAILURE,
    EXECUTION_REQUEST_STATUS_ROLLED_BACK,
    EXECUTION_REQUEST_STATUS_ROLLING_BACK,
    EXECUTION_REQUEST_STATUS_RUNNING,
    EXECUTION_REQUEST_STATUS_SUCCESS,
} from '@app/ingestV2/executions/constants';
import { isExecutionRequestActive } from '@app/ingestV2/executions/utils';

describe('isExecutionRequestActive', () => {
    it('should return true if the execution is RUNNING', () => {
        const isExecutionActive = isExecutionRequestActive({
            result: { status: EXECUTION_REQUEST_STATUS_RUNNING },
        } as any);
        expect(isExecutionActive).toBe(true);
    });
    it('should return true if the execution is ROLLING_BACK', () => {
        const isExecutionActive = isExecutionRequestActive({
            result: { status: EXECUTION_REQUEST_STATUS_ROLLING_BACK },
        } as any);
        expect(isExecutionActive).toBe(true);
    });
    it('should return false if the execution is ROLLING_BACK or RUNNING', () => {
        let isExecutionActive = isExecutionRequestActive({
            result: { status: EXECUTION_REQUEST_STATUS_ROLLED_BACK },
        } as any);
        expect(isExecutionActive).toBe(false);

        isExecutionActive = isExecutionRequestActive({ result: { status: EXECUTION_REQUEST_STATUS_SUCCESS } } as any);
        expect(isExecutionActive).toBe(false);

        isExecutionActive = isExecutionRequestActive({ result: { status: EXECUTION_REQUEST_STATUS_FAILURE } } as any);
        expect(isExecutionActive).toBe(false);
    });
});
