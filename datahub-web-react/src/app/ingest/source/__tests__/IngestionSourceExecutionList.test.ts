/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { isExecutionRequestActive } from '@app/ingest/source/executions/IngestionSourceExecutionList';
import { FAILURE, ROLLED_BACK, ROLLING_BACK, RUNNING, SUCCESS } from '@app/ingest/source/utils';

describe('isExecutionRequestActive', () => {
    it('should return true if the execution is RUNNING', () => {
        const isExecutionActive = isExecutionRequestActive({ result: { status: RUNNING } } as any);
        expect(isExecutionActive).toBe(true);
    });
    it('should return true if the execution is ROLLING_BACK', () => {
        const isExecutionActive = isExecutionRequestActive({ result: { status: ROLLING_BACK } } as any);
        expect(isExecutionActive).toBe(true);
    });
    it('should return false if the execution is ROLLING_BACK or RUNNING', () => {
        let isExecutionActive = isExecutionRequestActive({ result: { status: ROLLED_BACK } } as any);
        expect(isExecutionActive).toBe(false);

        isExecutionActive = isExecutionRequestActive({ result: { status: SUCCESS } } as any);
        expect(isExecutionActive).toBe(false);

        isExecutionActive = isExecutionRequestActive({ result: { status: FAILURE } } as any);
        expect(isExecutionActive).toBe(false);
    });
});
