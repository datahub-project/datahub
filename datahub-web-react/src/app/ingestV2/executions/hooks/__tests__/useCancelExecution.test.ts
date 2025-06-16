import { act, renderHook } from '@testing-library/react-hooks';
import message from 'antd/lib/message';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import useCancelExecution from '@app/ingestV2/executions/hooks/useCancelExecution';

import { useCancelIngestionExecutionRequestMutation } from '@graphql/ingestion.generated';

vi.mock('@graphql/ingestion.generated', () => ({
    useCancelIngestionExecutionRequestMutation: vi.fn(),
}));

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
        const mockSuccess = vi.spyOn(message, 'success');

        const { result } = renderHook(() => useCancelExecution(mockRefetch));

        const executionUrn = 'test-execution-urn';
        const ingestionSourceUrn = 'test-source-urn';

        await act(async () => {
            result.current(executionUrn, ingestionSourceUrn);
            // Resolve the promise chain
            await Promise.resolve();
        });

        // Fast-forward timers
        await act(async () => {
            vi.advanceTimersByTime(2000);
        });

        expect(mockSuccess).toHaveBeenCalledWith(
            expect.objectContaining({
                content: 'Successfully submitted cancellation request!',
            }),
        );
        expect(mockRefetch).toHaveBeenCalled();
    });

    it('should show error message when mutation fails', async () => {
        const errorMessage = 'GraphQL error occurred';
        mockExecuteMutation.mockRejectedValueOnce({ message: errorMessage });
        (useCancelIngestionExecutionRequestMutation as any).mockReturnValue([mockExecuteMutation]);

        const mockError = vi.spyOn(message, 'error');
        const mockDestroy = vi.spyOn(message, 'destroy');

        const { result } = renderHook(() => useCancelExecution(mockRefetch));

        const executionUrn = 'test-execution-urn';
        const ingestionSourceUrn = 'test-source-urn';

        await act(async () => {
            result.current(executionUrn, ingestionSourceUrn);
            // Wait for the promise to resolve or reject
            await Promise.resolve();
        });

        expect(mockDestroy).toHaveBeenCalled();
        expect(mockError).toHaveBeenCalledWith(
            expect.objectContaining({
                content: `Failed to cancel execution!: \n ${errorMessage}`,
            }),
        );
    });

    it('should show error message without e.message gracefully', async () => {
        mockExecuteMutation.mockRejectedValueOnce({}); // no message property

        (useCancelIngestionExecutionRequestMutation as any).mockReturnValue([mockExecuteMutation]);

        const mockError = vi.spyOn(message, 'error');
        const mockDestroy = vi.spyOn(message, 'destroy');

        const { result } = renderHook(() => useCancelExecution(mockRefetch));

        const executionUrn = 'test-execution-urn';
        const ingestionSourceUrn = 'test-source-urn';

        await act(async () => {
            result.current(executionUrn, ingestionSourceUrn);
            await Promise.resolve(); // Wait for promise to settle
        });

        expect(mockDestroy).toHaveBeenCalled();
        expect(mockError).toHaveBeenCalledWith(
            expect.objectContaining({
                content: `Failed to cancel execution!: \n `, // notice empty string after \n
            }),
        );
    });
});
