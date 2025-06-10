import { renderHook } from '@testing-library/react-hooks';
import { Mock, beforeEach, describe, expect, it, vi } from 'vitest';

import {
    EXECUTION_REQUEST_STATUS_ROLLING_BACK,
    EXECUTION_REQUEST_STATUS_RUNNING,
    EXECUTION_REQUEST_STATUS_SUCCESS,
} from '@app/ingestV2/executions/constants';
import useRefresh from '@app/ingestV2/executions/hooks/useRefresh';
import useRefreshInterval from '@app/ingestV2/shared/hooks/useRefreshInterval';

import { EntityType, ExecutionRequest } from '@types';

// Mock the useRefreshInterval hook
vi.mock('@app/ingestV2/shared/hooks/useRefreshInterval', () => ({
    default: vi.fn(),
}));

const mockedUseRefreshInterval = useRefreshInterval as Mock;

const getExecutionRequest = (urn: string, status: string): ExecutionRequest => {
    return {
        id: urn,
        urn,
        type: EntityType.ExecutionRequest,
        input: {
            requestedAt: 0,
            source: {},
            task: 'task',
        },
        result: {
            status,
        },
    };
};

describe('useRefresh Hook', () => {
    const refresh = vi.fn();

    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('does not trigger refresh interval when no execution requests are active', () => {
        const executionRequests = [getExecutionRequest('1', EXECUTION_REQUEST_STATUS_SUCCESS)];
        renderHook(() => useRefresh(executionRequests, refresh));

        // Verify useRefreshInterval is called with hasRunningExecutions returning false
        expect(mockedUseRefreshInterval).toHaveBeenCalledWith(refresh, expect.any(Function));
        const hasRunningExecutions = mockedUseRefreshInterval.mock.calls[0][1];
        expect(hasRunningExecutions()).toBe(false);
    });

    it('triggers refresh interval when at least one execution is running', () => {
        const executionRequests = [
            getExecutionRequest('1', EXECUTION_REQUEST_STATUS_RUNNING),
            getExecutionRequest('2', EXECUTION_REQUEST_STATUS_SUCCESS),
        ];
        renderHook(() => useRefresh(executionRequests, refresh));

        const hasRunningExecutions = mockedUseRefreshInterval.mock.calls[0][1];
        expect(hasRunningExecutions()).toBe(true);
    });

    it('triggers refresh interval when at least one execution is rolling back', () => {
        const executionRequests = [getExecutionRequest('1', EXECUTION_REQUEST_STATUS_ROLLING_BACK)];
        renderHook(() => useRefresh(executionRequests, refresh));

        const hasRunningExecutions = mockedUseRefreshInterval.mock.calls[0][1];
        expect(hasRunningExecutions()).toBe(true);
    });

    it('updates hasRunningExecutions when executionRequests change', () => {
        const firstRequest = getExecutionRequest('1', EXECUTION_REQUEST_STATUS_SUCCESS);
        const secondRequest = getExecutionRequest('1', EXECUTION_REQUEST_STATUS_RUNNING);

        const { rerender } = renderHook(({ requests }) => useRefresh(requests, refresh), {
            initialProps: { requests: [firstRequest] },
        });

        const hasRunningExecutions = mockedUseRefreshInterval.mock.calls[0][1];
        expect(hasRunningExecutions()).toBe(false);

        // Update executionRequests to include a running request
        rerender({ requests: [firstRequest, secondRequest] });
        const hasRunningExecutions2 = mockedUseRefreshInterval.mock.calls[1][1];
        expect(hasRunningExecutions2()).toBe(true);
    });

    it('passes refresh function directly to useRefreshInterval', () => {
        const executionRequests = [getExecutionRequest('1', EXECUTION_REQUEST_STATUS_RUNNING)];
        const mockRefresh = vi.fn();

        renderHook(() => useRefresh(executionRequests, mockRefresh));
        expect(mockedUseRefreshInterval).toHaveBeenCalledWith(mockRefresh, expect.any(Function));
    });
});
