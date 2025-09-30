import { MockedProvider } from '@apollo/client/testing';
import { act, renderHook } from '@testing-library/react-hooks';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import {
    EXECUTION_REQUEST_STATUS_FAILURE,
    EXECUTION_REQUEST_STATUS_ROLLED_BACK,
    EXECUTION_REQUEST_STATUS_ROLLING_BACK,
    EXECUTION_REQUEST_STATUS_RUNNING,
    EXECUTION_REQUEST_STATUS_SUCCESS,
} from '@app/ingestV2/executions/constants';
import { isExecutionRequestActive, useExecutionLogsDownload } from '@app/ingestV2/executions/utils';
import { downloadFile } from '@app/search/utils/csvUtils';

import { useGetExecutionRequestDownloadUrlLazyQuery } from '@graphql/ingestion.generated';

vi.mock('@graphql/ingestion.generated', () => ({
    useGetExecutionRequestDownloadUrlLazyQuery: vi.fn(),
}));

vi.mock('@app/search/utils/csvUtils', () => ({
    downloadFile: vi.fn(),
}));

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

describe('useExecutionLogsDownload', () => {
    const mockGetDownloadUrl = vi.fn();
    const mockDownloadFile = vi.mocked(downloadFile);

    const mockClick = vi.fn();

    const wrapper = ({ children }: { children: React.ReactNode }) =>
        React.createElement(MockedProvider, { mocks: [] }, children);

    beforeEach(() => {
        vi.clearAllMocks();

        (useGetExecutionRequestDownloadUrlLazyQuery as any).mockReturnValue([mockGetDownloadUrl]);
    });

    it('should download file using downloadUrl when GraphQL query succeeds', async () => {
        const downloadUrl = 'https://example.com/logs.txt';
        const executionRequestUrn = 'urn:li:executionRequest:123';
        const filename = 'execution-logs.txt';

        mockGetDownloadUrl.mockResolvedValue({
            data: {
                getExecutionRequestDownloadUrl: {
                    downloadUrl,
                },
            },
        });

        const clickSpy = vi.spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(vi.fn());
        const createElementSpy = vi.spyOn(document, 'createElement').mockReturnValue(document.createElement('a'));

        const { result } = renderHook(() => useExecutionLogsDownload(), { wrapper });

        await act(async () => {
            await result.current(executionRequestUrn, filename);
        });

        // Verify GraphQL query was called correctly
        expect(mockGetDownloadUrl).toHaveBeenCalledWith({
            variables: {
                input: {
                    executionRequestUrn,
                },
            },
        });

        // Verify DOM manipulation for download
        expect(createElementSpy).toHaveBeenCalledWith('a');
        const anchor = createElementSpy.mock.results[0].value as HTMLAnchorElement;
        expect(anchor.href).toBe(downloadUrl);
        expect(anchor.style.display).toBe('none');
        expect(clickSpy).toHaveBeenCalled();

        // Verify fallback was not used
        expect(mockDownloadFile).not.toHaveBeenCalled();
    });

    it('should use fallback downloadFile when GraphQL query returns no downloadUrl', async () => {
        const executionRequestUrn = 'urn:li:executionRequest:123';
        const filename = 'execution-logs.txt';
        const fallbackLogs = 'fallback log content';

        mockGetDownloadUrl.mockResolvedValue({
            data: {
                getExecutionRequestDownloadUrl: {
                    downloadUrl: null,
                },
            },
        });

        const { result } = renderHook(() => useExecutionLogsDownload(), { wrapper });

        await act(async () => {
            await result.current(executionRequestUrn, filename, fallbackLogs);
        });

        // Verify GraphQL query was called
        expect(mockGetDownloadUrl).toHaveBeenCalledWith({
            variables: {
                input: {
                    executionRequestUrn,
                },
            },
        });

        // Verify DOM manipulation was not used
        expect(mockClick).not.toHaveBeenCalled();

        // Verify fallback was used
        expect(mockDownloadFile).toHaveBeenCalledWith(fallbackLogs, filename);
    });

    it('should use fallback downloadFile when GraphQL query fails and fallbackLogs provided', async () => {
        const executionRequestUrn = 'urn:li:executionRequest:123';
        const filename = 'execution-logs.txt';
        const fallbackLogs = 'fallback log content';
        const errorMessage = 'GraphQL error';

        mockGetDownloadUrl.mockRejectedValue(new Error(errorMessage));

        const { result } = renderHook(() => useExecutionLogsDownload(), { wrapper });

        await act(async () => {
            await result.current(executionRequestUrn, filename, fallbackLogs);
        });

        // Verify GraphQL query was called
        expect(mockGetDownloadUrl).toHaveBeenCalledWith({
            variables: {
                input: {
                    executionRequestUrn,
                },
            },
        });

        // Verify DOM manipulation was not used
        expect(mockClick).not.toHaveBeenCalled();

        // Verify fallback was used
        expect(mockDownloadFile).toHaveBeenCalledWith(fallbackLogs, filename);
    });

    it('should log error when GraphQL query fails and no fallbackLogs provided', async () => {
        const executionRequestUrn = 'urn:li:executionRequest:123';
        const filename = 'execution-logs.txt';
        const errorMessage = 'GraphQL error';

        mockGetDownloadUrl.mockRejectedValue(new Error(errorMessage));

        const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});

        const { result } = renderHook(() => useExecutionLogsDownload(), { wrapper });

        await act(async () => {
            await result.current(executionRequestUrn, filename);
        });

        // Verify GraphQL query was called
        expect(mockGetDownloadUrl).toHaveBeenCalledWith({
            variables: {
                input: {
                    executionRequestUrn,
                },
            },
        });

        // Verify DOM manipulation was not used
        expect(mockClick).not.toHaveBeenCalled();

        // Verify fallback was not used
        expect(mockDownloadFile).not.toHaveBeenCalled();

        // Verify error was logged
        expect(consoleSpy).toHaveBeenCalledWith('Failed to download execution logs:', expect.any(Error));

        consoleSpy.mockRestore();
    });

    it('should set correct anchor element properties when using downloadUrl', async () => {
        const downloadUrl = 'https://example.com/logs.txt';
        const executionRequestUrn = 'urn:li:executionRequest:123';
        const filename = 'execution-logs.txt';

        mockGetDownloadUrl.mockResolvedValue({
            data: {
                getExecutionRequestDownloadUrl: {
                    downloadUrl,
                },
            },
        });

        const createElementSpy = vi.spyOn(document, 'createElement').mockReturnValue(document.createElement('a'));

        const { result } = renderHook(() => useExecutionLogsDownload(), { wrapper });

        await act(async () => {
            await result.current(executionRequestUrn, filename);
        });

        expect(createElementSpy).toHaveBeenCalledWith('a');
        const anchor = createElementSpy.mock.results[0].value as HTMLAnchorElement;

        // Verify anchor element properties were set correctly
        expect(anchor.href).toBe(downloadUrl);
        expect(anchor.download).toBe(filename);
        expect(anchor.style.display).toBe('none');
    });
});
