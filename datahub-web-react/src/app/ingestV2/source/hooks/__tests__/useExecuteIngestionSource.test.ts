import { act, renderHook } from '@testing-library/react-hooks';
import { Mock, beforeEach, describe, expect, it, vi } from 'vitest';

import { useExecuteIngestionSource } from '@app/ingestV2/source/hooks/useExecuteIngestionSource';

import { useCreateIngestionExecutionRequestMutation } from '@graphql/ingestion.generated';

// Mock all dependencies
vi.mock('@graphql/ingestion.generated');

describe('useExecuteIngestionSource', () => {
    const mockCreateExecutionRequestMutation = vi.fn();

    beforeEach(() => {
        vi.clearAllMocks();

        // Set up mock implementations
        (useCreateIngestionExecutionRequestMutation as Mock).mockReturnValue([mockCreateExecutionRequestMutation]);
    });

    it('should return a function', () => {
        const { result } = renderHook(() => useExecuteIngestionSource());

        expect(typeof result.current).toBe('function');
    });

    it('should execute ingestion source with correct parameters', async () => {
        const mockUrn = 'urn:li:ingestionSource:test-source';
        const mockExecutionResult = {
            data: {
                createIngestionExecutionRequest: { urn: 'urn:li:executionRequest:test-request' },
            },
        };

        mockCreateExecutionRequestMutation.mockResolvedValue(mockExecutionResult);

        const { result } = renderHook(() => useExecuteIngestionSource());

        let executionResult;
        await act(async () => {
            executionResult = await result.current(mockUrn);
        });

        expect(mockCreateExecutionRequestMutation).toHaveBeenCalledWith({
            variables: {
                input: { ingestionSourceUrn: mockUrn },
            },
            refetchQueries: ['listIngestionExecutionRequests'],
        });

        expect(executionResult).toEqual(mockExecutionResult);
    });

    it('should handle execution failure', async () => {
        const mockUrn = 'urn:li:ingestionSource:test-source';
        const mockError = new Error('Execution failed');
        mockCreateExecutionRequestMutation.mockRejectedValue(mockError);

        const { result } = renderHook(() => useExecuteIngestionSource());

        let caughtError;
        await act(async () => {
            try {
                await result.current(mockUrn);
            } catch (error) {
                caughtError = error;
            }
        });

        expect(mockCreateExecutionRequestMutation).toHaveBeenCalledWith({
            variables: {
                input: { ingestionSourceUrn: mockUrn },
            },
            refetchQueries: ['listIngestionExecutionRequests'],
        });

        expect(caughtError).toBe(mockError);
    });

    it('should return the mutation result', async () => {
        const mockUrn = 'urn:li:ingestionSource:test-source';
        const mockExecutionResult = {
            data: {
                createIngestionExecutionRequest: { urn: 'urn:li:executionRequest:test-request' },
            },
            loading: false,
            error: null,
        };

        mockCreateExecutionRequestMutation.mockResolvedValue(mockExecutionResult);

        const { result } = renderHook(() => useExecuteIngestionSource());

        let returnedResult;
        await act(async () => {
            returnedResult = await result.current(mockUrn);
        });

        expect(returnedResult).toEqual(mockExecutionResult);
    });
});
