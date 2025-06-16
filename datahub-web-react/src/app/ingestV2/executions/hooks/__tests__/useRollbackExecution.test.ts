import { act, renderHook } from '@testing-library/react-hooks';
import message from 'antd/lib/message';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import useRollbackExecution from '@app/ingestV2/executions/hooks/useRollbackExecution';

import { useRollbackIngestionMutation } from '@graphql/ingestion.generated';

vi.mock('@graphql/ingestion.generated', () => ({
    useRollbackIngestionMutation: vi.fn(),
}));

describe('useRollbackExecution Hook', () => {
    const mockRefetch = vi.fn();
    const mockRollbackIngestion = vi.fn().mockReturnValue(Promise.resolve({}));

    beforeEach(() => {
        vi.clearAllMocks();
        vi.useFakeTimers();
    });

    it('should call rollbackIngestion with correct runId', async () => {
        (useRollbackIngestionMutation as any).mockReturnValue([mockRollbackIngestion]);

        const { result } = renderHook(() => useRollbackExecution(mockRefetch));

        const runId = 'test-run-id';

        await act(async () => {
            result.current(runId);
        });

        expect(mockRollbackIngestion).toHaveBeenCalledWith({
            variables: {
                input: {
                    runId,
                },
            },
        });
    });

    it('should show loading message when rollback starts', async () => {
        const mockLoading = vi.spyOn(message, 'loading');
        (useRollbackIngestionMutation as any).mockReturnValue([mockRollbackIngestion]);

        const { result } = renderHook(() => useRollbackExecution(mockRefetch));

        const runId = 'test-run-id';

        await act(async () => {
            result.current(runId);
        });

        expect(mockLoading).toHaveBeenCalledWith('Requesting rollback...');
    });

    it('should show success message and trigger refetch after timeout', async () => {
        const mockSuccess = vi.spyOn(message, 'success');
        const mockDestroy = vi.spyOn(message, 'destroy');

        (useRollbackIngestionMutation as any).mockReturnValue([mockRollbackIngestion]);

        const { result } = renderHook(() => useRollbackExecution(mockRefetch));

        const runId = 'test-run-id';

        await act(async () => {
            result.current(runId);
            await Promise.resolve(); // resolve initial promise
            vi.advanceTimersByTime(2000); // simulate timeout
        });

        expect(mockDestroy).toHaveBeenCalled();
        expect(mockSuccess).toHaveBeenCalledWith('Successfully requested ingestion rollback');
        expect(mockRefetch).toHaveBeenCalled();
    });

    it('should show error message if mutation fails', async () => {
        const mockError = vi.spyOn(message, 'error');
        const failedMutation = vi.fn().mockReturnValue(Promise.reject(new Error('GraphQL error')));

        (useRollbackIngestionMutation as any).mockReturnValue([failedMutation]);

        const { result } = renderHook(() => useRollbackExecution(mockRefetch));

        const runId = 'test-run-id';

        await act(async () => {
            result.current(runId);
            await Promise.resolve(); // wait for rejection
        });

        expect(mockError).toHaveBeenCalledWith('Error requesting ingestion rollback');
    });
});
