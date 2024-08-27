import { isExecutionRequestActive } from '../executions/IngestionSourceExecutionList';
import { FAILURE, ROLLED_BACK, ROLLING_BACK, RUNNING, SUCCESS } from '../utils';

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
