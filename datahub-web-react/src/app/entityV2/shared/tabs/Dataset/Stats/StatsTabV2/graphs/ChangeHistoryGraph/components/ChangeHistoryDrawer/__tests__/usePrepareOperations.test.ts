import { renderHook } from '@testing-library/react-hooks';

import usePrepareOperations from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/components/ChangeHistoryDrawer/usePrepareOperations';
import { Operation, OperationType } from '@src/types.generated';

const SAMPLE_TIMESTAMP_1 = 1736260500000;
const SAMPLE_TIMESTAMP_2 = 1736260600000;

describe('usePrepareOperations', () => {
    it('should remove duplicates', () => {
        const operations: Operation[] = [
            {
                lastUpdatedTimestamp: SAMPLE_TIMESTAMP_1,
                operationType: OperationType.Update,
                timestampMillis: SAMPLE_TIMESTAMP_1,
            },
            {
                lastUpdatedTimestamp: SAMPLE_TIMESTAMP_1,
                operationType: OperationType.Update,
                timestampMillis: SAMPLE_TIMESTAMP_1,
            },
            {
                lastUpdatedTimestamp: SAMPLE_TIMESTAMP_2,
                operationType: OperationType.Update,
                timestampMillis: SAMPLE_TIMESTAMP_2,
            },
            {
                lastUpdatedTimestamp: SAMPLE_TIMESTAMP_2,
                operationType: OperationType.Create,
                timestampMillis: SAMPLE_TIMESTAMP_2,
            },
        ];

        const result = renderHook(() => usePrepareOperations(operations)).result.current;

        expect(result).toStrictEqual([
            {
                lastUpdatedTimestamp: SAMPLE_TIMESTAMP_1,
                operationType: OperationType.Update,
                timestampMillis: SAMPLE_TIMESTAMP_1,
            },
            {
                lastUpdatedTimestamp: SAMPLE_TIMESTAMP_2,
                operationType: OperationType.Update,
                timestampMillis: SAMPLE_TIMESTAMP_2,
            },
            {
                lastUpdatedTimestamp: SAMPLE_TIMESTAMP_2,
                operationType: OperationType.Create,
                timestampMillis: SAMPLE_TIMESTAMP_2,
            },
        ]);
    });

    it('should limit number of operations', () => {
        const operations: Operation[] = Array(1000)
            .fill(0)
            .map((_, index) => ({
                lastUpdatedTimestamp: SAMPLE_TIMESTAMP_1 + index,
                operationType: OperationType.Update,
                timestampMillis: SAMPLE_TIMESTAMP_1 + index,
            }));

        const result = renderHook(() => usePrepareOperations(operations)).result.current;

        expect(result.length).toEqual(100);
    });
});
