import { act, renderHook } from '@testing-library/react-hooks';

import useCreateFile from '@app/shared/hooks/useCreateFile';

import { UploadDownloadScenario } from '@types';

const mockCreateFileMutation = vi.fn();

vi.mock('@graphql/app.generated', () => ({
    useCreateDataHubFileMutation: () => [mockCreateFileMutation],
}));

describe('useCreateFile', () => {
    beforeEach(() => {
        mockCreateFileMutation.mockClear();
    });

    it('should successfully create a file', async () => {
        const fileId = 'test-file-id';
        const mockFile = new File(['content'], 'test.txt', { type: 'text/plain' });
        const assetUrn = 'urn:li:dataPlatform:test';
        const schemaField = 'testField';
        const scenario = UploadDownloadScenario.AssetDocumentation;

        mockCreateFileMutation.mockResolvedValue({
            data: {
                createDataHubFile: {
                    file: {
                        urn: 'urn:li:dataHubFile:test-file-id',
                    },
                },
            },
        });

        const { result } = renderHook(() => useCreateFile({ scenario, assetUrn, schemaField }));

        await act(async () => {
            await result.current.createFile(fileId, mockFile);
        });

        expect(mockCreateFileMutation).toHaveBeenCalledTimes(1);
        expect(mockCreateFileMutation).toHaveBeenCalledWith({
            variables: {
                input: {
                    id: fileId,
                    mimeType: mockFile.type,
                    originalFileName: mockFile.name,
                    referencedByAsset: assetUrn,
                    schemaField,
                    scenario,
                    sizeInBytes: mockFile.size,
                    storageKey: `product-assets/${fileId}`,
                    contentHash: 'ed7002b439e9ac845f22357d822bac1444730fbdb6016d3ec9432297b9ec9f73', // Expected SHA-256 hash of 'content'
                },
            },
        });
    });

    it('should throw an error if mutation fails', async () => {
        const fileId = 'test-file-id';
        const mockFile = new File(['content'], 'test.txt', { type: 'text/plain' });
        const scenario = UploadDownloadScenario.AssetDocumentation;

        mockCreateFileMutation.mockRejectedValue(new Error('Network error'));

        const { result } = renderHook(() => useCreateFile({ scenario }));

        await act(async () => {
            await expect(result.current.createFile(fileId, mockFile)).rejects.toThrow(
                'Failed to upload file after 3 attempts: Error: Network error',
            );
        });
    });

    it('should throw an error if response data is missing or invalid', async () => {
        const fileId = 'test-file-id';
        const mockFile = new File(['content'], 'test.txt', { type: 'text/plain' });
        const scenario = UploadDownloadScenario.AssetDocumentation;

        mockCreateFileMutation.mockResolvedValue({
            data: {
                createDataHubFile: {
                    file: null, // Missing file urn
                },
            },
            errors: [{ message: 'File creation failed' }],
        });

        const { result } = renderHook(() => useCreateFile({ scenario }));

        await act(async () => {
            await expect(result.current.createFile(fileId, mockFile)).rejects.toThrow(
                'Failed to upload file: [{"message":"File creation failed"}]',
            );
        });
    });

    it('should retry mutation on failure and eventually succeed', async () => {
        const fileId = 'test-file-id';
        const mockFile = new File(['content'], 'test.txt', { type: 'text/plain' });
        const scenario = UploadDownloadScenario.AssetDocumentation;

        // Mock the mutation to fail twice and then succeed on the third attempt
        mockCreateFileMutation
            .mockRejectedValueOnce(new Error('Transient network error 1'))
            .mockRejectedValueOnce(new Error('Transient network error 2'))
            .mockResolvedValueOnce({
                data: {
                    createDataHubFile: {
                        file: {
                            urn: 'urn:li:dataHubFile:test-file-id',
                        },
                    },
                },
            });

        const { result } = renderHook(() => useCreateFile({ scenario }));

        await act(async () => {
            await result.current.createFile(fileId, mockFile);
        });

        // Expect the mutation to have been called 3 times (2 failures + 1 success)
        expect(mockCreateFileMutation).toHaveBeenCalledTimes(3);
        expect(mockCreateFileMutation).toHaveBeenCalledWith({
            variables: {
                input: {
                    id: fileId,
                    mimeType: mockFile.type,
                    originalFileName: mockFile.name,
                    referencedByAsset: undefined,
                    schemaField: undefined,
                    scenario,
                    sizeInBytes: mockFile.size,
                    storageKey: `product-assets/${fileId}`,
                    contentHash: 'ed7002b439e9ac845f22357d822bac1444730fbdb6016d3ec9432297b9ec9f73',
                },
            },
        });
    });
});
