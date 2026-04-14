import { MockedProvider } from '@apollo/client/testing';
import { waitFor } from '@testing-library/react';
import { renderHook } from '@testing-library/react-hooks';
import React from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import useFileUpload from '@app/shared/hooks/useFileUpload';
import { useAppConfig } from '@src/app/useAppConfig';

import { GetPresignedUploadUrlDocument } from '@graphql/app.generated';
import { UploadDownloadScenario } from '@types';

// Mock the resolveRuntimePath utility
vi.mock('@utils/runtimeBasePath', () => ({
    resolveRuntimePath: vi.fn((path) => `http://example.com${path}`),
}));

// Mock the useAppConfig hook
vi.mock('@src/app/useAppConfig', () => ({
    useAppConfig: vi.fn(),
}));

// Mock the useCreateFile hook
const mockCreateFile = vi.fn();
vi.mock('@app/shared/hooks/useCreateFile', () => ({
    __esModule: true,
    default: () => ({
        createFile: mockCreateFile,
    }),
    S3_FILE_ID_NAME_SEPARATOR: '__',
}));

describe('useFileUpload', () => {
    let originalFetch: typeof global.fetch;
    const mockAssetUrn = 'urn:li:glossaryNode:c21f8d1a-a2d6-4712-b363-cdd1a99f6c76';

    beforeEach(() => {
        originalFetch = global.fetch;
        // By default, enable the feature flag for existing tests
        vi.mocked(useAppConfig).mockReturnValue({
            config: {
                featureFlags: {
                    documentationFileUploadV1: true,
                },
            },
        } as any);
    });

    afterEach(() => {
        global.fetch = originalFetch;
        vi.clearAllMocks();
    });

    it('should successfully upload a file and return the file URL', async () => {
        const mockFile = new File(['test content'], 'test.pdf', { type: 'application/pdf' });
        const mockUploadUrl = 'https://s3.example.com/upload-url';
        const mockFileId = 'file-123';

        // Mock the GraphQL response
        const mocks = [
            {
                request: {
                    query: GetPresignedUploadUrlDocument,
                    variables: {
                        input: {
                            scenario: UploadDownloadScenario.AssetDocumentation,
                            assetUrn: mockAssetUrn,
                            contentType: 'application/pdf',
                            fileName: 'test.pdf',
                        },
                    },
                },
                result: {
                    data: {
                        getPresignedUploadUrl: {
                            url: mockUploadUrl,
                            fileId: mockFileId,
                        },
                    },
                },
            },
        ];

        // Mock fetch for the file upload
        global.fetch = vi.fn().mockResolvedValue({
            ok: true,
            statusText: 'OK',
        });

        const { result } = renderHook(
            () =>
                useFileUpload({
                    scenario: UploadDownloadScenario.AssetDocumentation,
                    assetUrn: mockAssetUrn,
                }),
            {
                wrapper: ({ children }) => (
                    <MockedProvider mocks={mocks} addTypename={false}>
                        {children}
                    </MockedProvider>
                ),
            },
        );

        mockCreateFile.mockResolvedValue(undefined); // Mock successful file creation

        const uploadPromise = result.current.uploadFile?.(mockFile);

        await waitFor(async () => {
            const url = await uploadPromise;
            expect(url).toBe('http://example.com/openapi/v1/files/product_assets/file-123');
        });

        // Verify fetch was called with correct parameters
        expect(global.fetch).toHaveBeenCalledWith(mockUploadUrl, {
            method: 'PUT',
            body: mockFile,
            headers: {
                'Content-Type': 'application/pdf',
            },
        });
        expect(mockCreateFile).toHaveBeenCalledTimes(1);
        expect(mockCreateFile).toHaveBeenCalledWith(mockFileId, mockFile);
    });

    it('should throw an error if presigned URL is not returned', async () => {
        const mockFile = new File(['test content'], 'test.pdf', { type: 'application/pdf' });

        // Mock the GraphQL response without URL
        const mocks = [
            {
                request: {
                    query: GetPresignedUploadUrlDocument,
                    variables: {
                        input: {
                            scenario: UploadDownloadScenario.AssetDocumentation,
                            assetUrn: mockAssetUrn,
                            contentType: 'application/pdf',
                            fileName: 'test.pdf',
                        },
                    },
                },
                result: {
                    data: {
                        getPresignedUploadUrl: {
                            url: null,
                            fileId: 'file-123',
                        },
                    },
                },
            },
        ];

        const { result } = renderHook(
            () =>
                useFileUpload({
                    scenario: UploadDownloadScenario.AssetDocumentation,
                    assetUrn: mockAssetUrn,
                }),
            {
                wrapper: ({ children }) => (
                    <MockedProvider mocks={mocks} addTypename={false}>
                        {children}
                    </MockedProvider>
                ),
            },
        );

        mockCreateFile.mockResolvedValue(undefined); // Mock successful file creation

        await expect(result.current.uploadFile?.(mockFile)).rejects.toThrow('Issue uploading file to server');
        expect(mockCreateFile).not.toHaveBeenCalled(); // Should not call createFile if presigned URL is missing
    });

    it('should throw an error if file upload to presigned URL fails', async () => {
        const mockFile = new File(['test content'], 'test.pdf', { type: 'application/pdf' });
        const mockUploadUrl = 'https://s3.example.com/upload-url';
        const mockFileId = 'file-123';

        // Mock the GraphQL response
        const mocks = [
            {
                request: {
                    query: GetPresignedUploadUrlDocument,
                    variables: {
                        input: {
                            scenario: UploadDownloadScenario.AssetDocumentation,
                            assetUrn: mockAssetUrn,
                            contentType: 'application/pdf',
                            fileName: 'test.pdf',
                        },
                    },
                },
                result: {
                    data: {
                        getPresignedUploadUrl: {
                            url: mockUploadUrl,
                            fileId: mockFileId,
                        },
                    },
                },
            },
        ];

        // Mock fetch to return an error response
        global.fetch = vi.fn().mockResolvedValue({
            ok: false,
            statusText: 'Internal Server Error',
        });

        const { result } = renderHook(
            () =>
                useFileUpload({
                    scenario: UploadDownloadScenario.AssetDocumentation,
                    assetUrn: mockAssetUrn,
                }),
            {
                wrapper: ({ children }) => (
                    <MockedProvider mocks={mocks} addTypename={false}>
                        {children}
                    </MockedProvider>
                ),
            },
        );

        await expect(result.current.uploadFile?.(mockFile)).rejects.toThrow(
            'Failed to upload file: Internal Server Error',
        );
        expect(mockCreateFile).not.toHaveBeenCalled(); // Should not call createFile if fetch fails
    });

    it('should handle different file types correctly', async () => {
        const mockFile = new File(['test content'], 'image.png', { type: 'image/png' });
        const mockUploadUrl = 'https://s3.example.com/upload-url';
        const mockFileId = 'file-456';

        // Mock the GraphQL response
        const mocks = [
            {
                request: {
                    query: GetPresignedUploadUrlDocument,
                    variables: {
                        input: {
                            scenario: UploadDownloadScenario.AssetDocumentation,
                            assetUrn: mockAssetUrn,
                            contentType: 'image/png',
                            fileName: 'image.png',
                        },
                    },
                },
                result: {
                    data: {
                        getPresignedUploadUrl: {
                            url: mockUploadUrl,
                            fileId: mockFileId,
                        },
                    },
                },
            },
        ];

        // Mock fetch for the file upload
        global.fetch = vi.fn().mockResolvedValue({
            ok: true,
            statusText: 'OK',
        });

        const { result } = renderHook(
            () =>
                useFileUpload({
                    scenario: UploadDownloadScenario.AssetDocumentation,
                    assetUrn: mockAssetUrn,
                }),
            {
                wrapper: ({ children }) => (
                    <MockedProvider mocks={mocks} addTypename={false}>
                        {children}
                    </MockedProvider>
                ),
            },
        );

        mockCreateFile.mockResolvedValue(undefined); // Mock successful file creation

        const uploadPromise = result.current.uploadFile?.(mockFile);

        await waitFor(async () => {
            const url = await uploadPromise;
            expect(url).toBe('http://example.com/openapi/v1/files/product_assets/file-456');
        });

        // Verify fetch was called with correct content type
        expect(global.fetch).toHaveBeenCalledWith(
            mockUploadUrl,
            expect.objectContaining({
                headers: {
                    'Content-Type': 'image/png',
                },
            }),
        );
        expect(mockCreateFile).toHaveBeenCalledTimes(1);
        expect(mockCreateFile).toHaveBeenCalledWith(mockFileId, mockFile);
    });

    it('should work without assetUrn when not provided', async () => {
        const mockFile = new File(['test content'], 'test.pdf', { type: 'application/pdf' });
        const mockUploadUrl = 'https://s3.example.com/upload-url';
        const mockFileId = 'file-789';

        // Mock the GraphQL response without assetUrn
        const mocks = [
            {
                request: {
                    query: GetPresignedUploadUrlDocument,
                    variables: {
                        input: {
                            scenario: UploadDownloadScenario.AssetDocumentation,
                            assetUrn: undefined,
                            contentType: 'application/pdf',
                            fileName: 'test.pdf',
                        },
                    },
                },
                result: {
                    data: {
                        getPresignedUploadUrl: {
                            url: mockUploadUrl,
                            fileId: mockFileId,
                        },
                    },
                },
            },
        ];

        // Mock fetch for the file upload
        global.fetch = vi.fn().mockResolvedValue({
            ok: true,
            statusText: 'OK',
        });

        const { result } = renderHook(
            () =>
                useFileUpload({
                    scenario: UploadDownloadScenario.AssetDocumentation,
                }),
            {
                wrapper: ({ children }) => (
                    <MockedProvider mocks={mocks} addTypename={false}>
                        {children}
                    </MockedProvider>
                ),
            },
        );

        mockCreateFile.mockResolvedValue(undefined); // Mock successful file creation

        const uploadPromise = result.current.uploadFile?.(mockFile);

        await waitFor(async () => {
            const url = await uploadPromise;
            expect(url).toBe('http://example.com/openapi/v1/files/product_assets/file-789');
        });
        expect(mockCreateFile).toHaveBeenCalledTimes(1);
        expect(mockCreateFile).toHaveBeenCalledWith(mockFileId, mockFile);
    });

    it('should return uploadFile function when feature flag is enabled', () => {
        const mocks: any[] = [];

        const { result } = renderHook(
            () =>
                useFileUpload({
                    scenario: UploadDownloadScenario.AssetDocumentation,
                    assetUrn: mockAssetUrn,
                }),
            {
                wrapper: ({ children }) => (
                    <MockedProvider mocks={mocks} addTypename={false}>
                        {children}
                    </MockedProvider>
                ),
            },
        );

        expect(result.current).toHaveProperty('uploadFile');
        expect(typeof result.current.uploadFile).toBe('function');
    });

    describe('feature flag disabled', () => {
        beforeEach(() => {
            // Override the default mock to disable the feature flag
            vi.mocked(useAppConfig).mockReturnValue({
                config: {
                    featureFlags: {
                        documentationFileUploadV1: false,
                    },
                },
            } as any);
        });

        it('should return undefined for uploadFile when feature flag is disabled', () => {
            const mocks: any[] = [];

            const { result } = renderHook(
                () =>
                    useFileUpload({
                        scenario: UploadDownloadScenario.AssetDocumentation,
                        assetUrn: mockAssetUrn,
                    }),
                {
                    wrapper: ({ children }) => (
                        <MockedProvider mocks={mocks} addTypename={false}>
                            {children}
                        </MockedProvider>
                    ),
                },
            );

            expect(result.current).toHaveProperty('uploadFile');
            expect(result.current.uploadFile).toBeUndefined();
        });

        it('should not allow file upload when feature flag is disabled', () => {
            const mocks: any[] = [];
            const mockFile = new File(['test content'], 'test.pdf', { type: 'application/pdf' });

            const { result } = renderHook(
                () =>
                    useFileUpload({
                        scenario: UploadDownloadScenario.AssetDocumentation,
                        assetUrn: mockAssetUrn,
                    }),
                {
                    wrapper: ({ children }) => (
                        <MockedProvider mocks={mocks} addTypename={false}>
                            {children}
                        </MockedProvider>
                    ),
                },
            );

            // uploadFile should be undefined, so attempting to call it should throw
            expect(result.current.uploadFile).toBeUndefined();
            expect(() => result.current.uploadFile?.(mockFile)).not.toThrow();
            expect(mockCreateFile).not.toHaveBeenCalled(); // Should not call createFile if feature flag is disabled
        });
    });
});
