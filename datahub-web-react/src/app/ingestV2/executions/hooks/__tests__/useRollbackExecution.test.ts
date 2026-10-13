import { toast } from '@components';
import { act, renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import useRollbackExecution from '@app/ingestV2/executions/hooks/useRollbackExecution';

import { useRollbackIngestionMutation } from '@graphql/ingestion.generated';

vi.mock('@graphql/ingestion.generated', () => ({
    useRollbackIngestionMutation: vi.fn(),
}));

vi.mock('@components', async (importOriginal) => {
    const actual = await importOriginal<typeof import('@components')>();
    return {
        ...actual,
        toast: {
            success: vi.fn(),
            error: vi.fn(),
            warning: vi.fn(),
            info: vi.fn(),
            loading: vi.fn(),
            destroy: vi.fn(),
        },
    };
});

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
        (useRollbackIngestionMutation as any).mockReturnValue([mockRollbackIngestion]);

        const { result } = renderHook(() => useRollbackExecution(mockRefetch));

        const runId = 'test-run-id';

        await act(async () => {
            result.current(runId);
        });

        expect(toast.loading).toHaveBeenCalledWith(expect.any(String));
    });

    it('should show success message and trigger refetch after timeout', async () => {
        (useRollbackIngestionMutation as any).mockReturnValue([mockRollbackIngestion]);

        const { result } = renderHook(() => useRollbackExecution(mockRefetch));

        const runId = 'test-run-id';

        await act(async () => {
            result.current(runId);
            await Promise.resolve();
            vi.advanceTimersByTime(2000);
        });

        expect(toast.destroy).toHaveBeenCalled();
        expect(toast.success).toHaveBeenCalledWith(expect.any(String));
        expect(mockRefetch).toHaveBeenCalled();
    });

    it('should show error message if mutation fails', async () => {
        const failedMutation = vi.fn().mockReturnValue(Promise.reject(new Error('GraphQL error')));

        (useRollbackIngestionMutation as any).mockReturnValue([failedMutation]);

        const { result } = renderHook(() => useRollbackExecution(mockRefetch));

        const runId = 'test-run-id';

        await act(async () => {
            result.current(runId);
            await Promise.resolve();
        });

        expect(toast.error).toHaveBeenCalledWith(expect.any(String));
    });
});
