import { renderHook, act } from '@testing-library/react-hooks';
import { UploadDownloadScenario } from '@types';
import useCreateFile from '@app/shared/hooks/useCreateFile';

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

        const { result } = renderHook(() =>
            useCreateFile({ scenario, assetUrn, schemaField }),
        );

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
                    storageBucket: 'test',
                    storageKey: `product-assets/${fileId}`,
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
                'Failed to upload file: Error: Network error',
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
});
