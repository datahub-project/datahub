import { toast } from '@components';
import { act, renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import useCancelExecution from '@app/ingestV2/executions/hooks/useCancelExecution';

import { useCancelIngestionExecutionRequestMutation } from '@graphql/ingestion.generated';

vi.mock('@graphql/ingestion.generated', () => ({
    useCancelIngestionExecutionRequestMutation: vi.fn(),
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

describe('useCancelExecution Hook', () => {
    const mockRefetch = vi.fn();
    const mockExecuteMutation = vi.fn().mockReturnValue(Promise.resolve({}));

    beforeEach(() => {
        vi.clearAllMocks();
        vi.useFakeTimers();
    });

    it('should call the mutation with correct variables', async () => {
        (useCancelIngestionExecutionRequestMutation as any).mockReturnValue([mockExecuteMutation]);

        const { result } = renderHook(() => useCancelExecution(mockRefetch));

        const executionUrn = 'test-execution-urn';
        const ingestionSourceUrn = 'test-source-urn';

        await act(async () => {
            result.current(executionUrn, ingestionSourceUrn);
        });

        expect(mockExecuteMutation).toHaveBeenCalledWith({
            variables: {
                input: {
                    ingestionSourceUrn,
                    executionRequestUrn: executionUrn,
                },
            },
        });
    });

    it('should show success message and trigger refetch after timeout', async () => {
        (useCancelIngestionExecutionRequestMutation as any).mockReturnValue([mockExecuteMutation]);

        const { result } = renderHook(() => useCancelExecution(mockRefetch));

        const executionUrn = 'test-execution-urn';
        const ingestionSourceUrn = 'test-source-urn';

        await act(async () => {
            result.current(executionUrn, ingestionSourceUrn);
            await Promise.resolve();
        });

        await act(async () => {
            vi.advanceTimersByTime(2000);
        });

        expect(toast.success).toHaveBeenCalledWith(expect.any(String), { duration: 3 });
        expect(mockRefetch).toHaveBeenCalled();
    });

    it('should show error message when mutation fails', async () => {
        const errorMessage = 'GraphQL error occurred';
        mockExecuteMutation.mockRejectedValueOnce({ message: errorMessage });
        (useCancelIngestionExecutionRequestMutation as any).mockReturnValue([mockExecuteMutation]);

        const { result } = renderHook(() => useCancelExecution(mockRefetch));

        const executionUrn = 'test-execution-urn';
        const ingestionSourceUrn = 'test-source-urn';

        await act(async () => {
            result.current(executionUrn, ingestionSourceUrn);
            await Promise.resolve();
        });

        expect(toast.destroy).toHaveBeenCalled();
        expect(toast.error).toHaveBeenCalledWith(expect.stringContaining(errorMessage), { duration: 3 });
    });

    it('should show error message without e.message gracefully', async () => {
        mockExecuteMutation.mockRejectedValueOnce({});

        (useCancelIngestionExecutionRequestMutation as any).mockReturnValue([mockExecuteMutation]);

        const { result } = renderHook(() => useCancelExecution(mockRefetch));

        const executionUrn = 'test-execution-urn';
        const ingestionSourceUrn = 'test-source-urn';

        await act(async () => {
            result.current(executionUrn, ingestionSourceUrn);
            await Promise.resolve();
        });

        expect(toast.destroy).toHaveBeenCalled();
        expect(toast.error).toHaveBeenCalledWith(expect.any(String), { duration: 3 });
    });
});
