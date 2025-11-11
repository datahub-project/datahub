import { notification } from '@components';
import { act, renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { Mock } from 'vitest';

// Import the mocked modules
import { validateFile } from '@components/components/Editor/extensions/fileDragDrop';
import { FileUploadFailureType } from '@components/components/Editor/types';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { useUploadFileHandler } from '@app/entityV2/summary/links/useUploadFileHandler';
import useFileUpload from '@app/shared/hooks/useFileUpload';
import useFileUploadAnalyticsCallbacks from '@app/shared/hooks/useFileUploadAnalyticsCallbacks';

// Mock required modules
vi.mock('@components', () => ({
    notification: {
        error: vi.fn(),
    },
}));

vi.mock('@components/components/Editor/extensions/fileDragDrop', () => ({
    validateFile: vi.fn(),
}));

vi.mock('@app/entity/shared/EntityContext', () => ({
    useEntityData: vi.fn(),
}));

vi.mock('@app/shared/hooks/useFileUpload', () => ({
    default: vi.fn(),
}));

vi.mock('@app/shared/hooks/useFileUploadAnalyticsCallbacks', () => ({
    default: vi.fn(),
}));

describe('useUploadFileHandler', () => {
    const mockEntityUrn = 'urn:li:dataset:(urn:li:dataset,lineage-test,PROD)';
    const mockFile = {
        name: 'test.pdf',
        size: 1024,
        type: 'application/pdf',
    } as File;

    beforeEach(() => {
        vi.clearAllMocks();

        // Set up default mocks
        (useEntityData as Mock).mockReturnValue({ urn: mockEntityUrn });
        (useFileUpload as Mock).mockReturnValue({ uploadFile: vi.fn() });
        (useFileUploadAnalyticsCallbacks as Mock).mockReturnValue({
            onFileUploadAttempt: vi.fn(),
            onFileUploadSucceeded: vi.fn(),
            onFileUploadFailed: vi.fn(),
        });
    });

    it('should validate the file before attempting upload', async () => {
        const mockValidation = { isValid: true };
        (validateFile as Mock).mockReturnValueOnce(mockValidation);

        const { result } = renderHook(() => useUploadFileHandler());

        await act(async () => {
            await result.current(mockFile);
        });

        expect(validateFile).toHaveBeenCalledWith(mockFile);
    });

    it('should return null and show error notification if file validation fails', async () => {
        const mockValidation = {
            isValid: false,
            error: 'File size exceeds limit',
            displayError: 'File too large',
            failureType: FileUploadFailureType.FILE_SIZE,
        };
        (validateFile as Mock).mockReturnValueOnce(mockValidation);

        const analyticsCallbacks = {
            onFileUploadFailed: vi.fn(),
        };
        (useFileUploadAnalyticsCallbacks as Mock).mockReturnValueOnce(analyticsCallbacks);

        const { result } = renderHook(() => useUploadFileHandler());

        let resultValue;
        await act(async () => {
            resultValue = await result.current(mockFile);
        });

        expect(resultValue).toBeNull();
        expect(notification.error).toHaveBeenCalledWith({
            message: 'Upload Failed',
            description: 'File too large',
        });
        // When failureType is provided correctly, it should be used instead of the fallback
        expect(analyticsCallbacks.onFileUploadFailed).toHaveBeenCalledWith(
            mockFile.type,
            mockFile.size,
            'button',
            FileUploadFailureType.FILE_SIZE, // 'file_size'
        );
    });

    it('should skip invalid file and return null', async () => {
        const mockValidation = {
            isValid: false,
            error: 'Invalid file type',
            displayError: 'Unsupported file type',
            failureType: FileUploadFailureType.FILE_TYPE,
        };
        (validateFile as Mock).mockReturnValueOnce(mockValidation);

        const { result } = renderHook(() => useUploadFileHandler());

        let resultValue;
        await act(async () => {
            resultValue = await result.current(mockFile);
        });

        expect(resultValue).toBeNull();
        expect(notification.error).toHaveBeenCalledWith({
            message: 'Upload Failed',
            description: 'Unsupported file type',
        });
    });

    it('should upload file successfully and return the URL', async () => {
        const mockValidation = { isValid: true };
        (validateFile as Mock).mockReturnValueOnce(mockValidation);

        const mockFileURL = 'http://localhost:9002/fileUpload/test.pdf';
        const mockUploadFunction = vi.fn().mockResolvedValue(mockFileURL);
        (useFileUpload as Mock).mockReturnValueOnce({ uploadFile: mockUploadFunction });

        const analyticsCallbacks = {
            onFileUploadSucceeded: vi.fn(),
        };
        (useFileUploadAnalyticsCallbacks as Mock).mockReturnValueOnce(analyticsCallbacks);

        const { result } = renderHook(() => useUploadFileHandler());

        let resultValue;
        await act(async () => {
            resultValue = await result.current(mockFile);
        });

        expect(resultValue).toBe(mockFileURL);
        expect(mockUploadFunction).toHaveBeenCalledWith(mockFile);
        expect(analyticsCallbacks.onFileUploadSucceeded).toHaveBeenCalledWith(mockFile.type, mockFile.size, 'button');
    });

    it('should handle upload errors gracefully and return null', async () => {
        const mockValidation = { isValid: true };
        (validateFile as Mock).mockReturnValueOnce(mockValidation);

        const mockUploadFunction = vi.fn().mockRejectedValue(new Error('Upload failed'));
        (useFileUpload as Mock).mockReturnValueOnce({ uploadFile: mockUploadFunction });

        const analyticsCallbacks = {
            onFileUploadFailed: vi.fn(),
        };
        (useFileUploadAnalyticsCallbacks as Mock).mockReturnValueOnce(analyticsCallbacks);

        const { result } = renderHook(() => useUploadFileHandler());

        let resultValue;
        await act(async () => {
            resultValue = await result.current(mockFile);
        });

        expect(resultValue).toBeNull();
        expect(notification.error).toHaveBeenCalledWith({
            message: 'Upload Failed',
            description: 'Something went wrong',
        });
        expect(analyticsCallbacks.onFileUploadFailed).toHaveBeenCalledWith(
            mockFile.type,
            mockFile.size,
            'button',
            FileUploadFailureType.UNKNOWN,
            'Error: Upload failed',
        );
    });

    it('should handle general errors during file processing and return null', async () => {
        const mockValidation = vi.fn().mockImplementation(() => {
            throw new Error('Unexpected error during validation');
        });
        (validateFile as Mock).mockImplementation(mockValidation);

        const analyticsCallbacks = {
            onFileUploadFailed: vi.fn(),
        };
        (useFileUploadAnalyticsCallbacks as Mock).mockReturnValueOnce(analyticsCallbacks);

        const { result } = renderHook(() => useUploadFileHandler());

        let resultValue;
        await act(async () => {
            resultValue = await result.current(mockFile);
        });

        expect(resultValue).toBeNull();
        expect(notification.error).toHaveBeenCalledWith({
            message: 'Upload Failed',
            description: 'Something went wrong',
        });
        expect(analyticsCallbacks.onFileUploadFailed).toHaveBeenCalledWith(
            mockFile.type,
            mockFile.size,
            'button',
            FileUploadFailureType.UNKNOWN,
            'Error: Unexpected error during validation',
        );
    });
});
