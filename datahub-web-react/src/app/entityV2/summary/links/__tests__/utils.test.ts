import { describe, expect, it, vi } from 'vitest';
import type { Mock } from 'vitest';

import { isFileUrl } from '@components/components/Editor/extensions/fileDragDrop';

import { LinkFormVariant } from '@app/entityV2/summary/links/types';
import {
    getGeneralizedLinkFormDataFromFormData,
    getInitialLinkFormDataFromInstitutionMemory,
} from '@app/entityV2/summary/links/utils';

// Mock the isFileUrl function from the fileDragDrop module
vi.mock('@components/components/Editor/extensions/fileDragDrop', () => ({
    isFileUrl: vi.fn(),
}));

describe('utils', () => {
    describe('getInitialLinkFormDataFromInstitutionMemory', () => {
        it('should return correct form data for a regular URL when file upload is disabled', () => {
            const mockInstitutionalMemory = {
                url: 'https://example.com',
                label: 'Example Label',
                description: 'Example Description',
                settings: {
                    showInAssetPreview: true,
                },
            };

            const result = getInitialLinkFormDataFromInstitutionMemory(mockInstitutionalMemory, false);

            expect(result).toEqual({
                variant: LinkFormVariant.URL,
                url: 'https://example.com',
                label: 'Example Label',
                showInAssetPreview: true,
            });
        });

        it('should return correct form data for a regular URL when file upload is enabled', () => {
            const mockInstitutionalMemory = {
                url: 'https://example.com',
                label: 'Example Label',
                description: 'Example Description',
                settings: {
                    showInAssetPreview: true,
                },
            };

            // Mock isFileUrl to return false for regular URLs
            (isFileUrl as Mock).mockReturnValue(false);

            const result = getInitialLinkFormDataFromInstitutionMemory(mockInstitutionalMemory, true);

            expect(result).toEqual({
                variant: LinkFormVariant.URL,
                url: 'https://example.com',
                label: 'Example Label',
                showInAssetPreview: true,
            });
            expect(isFileUrl).toHaveBeenCalledWith('https://example.com');
        });

        it('should return upload file form data when URL is a file URL and upload is enabled', () => {
            const mockInstitutionalMemory = {
                url: 'http://localhost:9002/fileUpload/path/to/file.pdf',
                label: 'Example File',
                description: 'Example Description',
                settings: {
                    showInAssetPreview: false,
                },
            };

            // Mock isFileUrl to return true for file URLs
            (isFileUrl as Mock).mockReturnValue(true);

            const result = getInitialLinkFormDataFromInstitutionMemory(mockInstitutionalMemory, true);

            expect(result).toEqual({
                variant: LinkFormVariant.UploadFile,
                fileUrl: 'http://localhost:9002/fileUpload/path/to/file.pdf',
                label: 'Example File',
                showInAssetPreview: false,
            });
            expect(isFileUrl).toHaveBeenCalledWith('http://localhost:9002/fileUpload/path/to/file.pdf');
        });

        it('should return URL form data when URL is a file URL but upload is disabled', () => {
            // Clear any previous calls to isFileUrl
            vi.clearAllMocks();

            const mockInstitutionalMemory = {
                url: 'http://localhost:9002/fileUpload/path/to/file.pdf',
                label: 'Example File',
                description: 'Example Description',
                settings: {
                    showInAssetPreview: false,
                },
            };

            const result = getInitialLinkFormDataFromInstitutionMemory(mockInstitutionalMemory, false);

            expect(result).toEqual({
                variant: LinkFormVariant.URL,
                url: 'http://localhost:9002/fileUpload/path/to/file.pdf',
                label: 'Example File',
                showInAssetPreview: false,
            });
            // isFileUrl should not be called since file upload is disabled
            expect(isFileUrl).not.toHaveBeenCalled();
        });

        it('should use description as label when label is not available', () => {
            const mockInstitutionalMemory = {
                url: 'https://example.com',
                label: undefined,
                description: 'Example Description',
                settings: {
                    showInAssetPreview: true,
                },
            };

            const result = getInitialLinkFormDataFromInstitutionMemory(mockInstitutionalMemory, false);

            expect(result).toEqual({
                variant: LinkFormVariant.URL,
                url: 'https://example.com',
                label: 'Example Description',
                showInAssetPreview: true,
            });
        });

        it('should handle null institutionalMemory', () => {
            const result = getInitialLinkFormDataFromInstitutionMemory(null);

            expect(result).toEqual({
                variant: LinkFormVariant.URL,
                url: undefined,
                label: undefined,
                showInAssetPreview: false,
            });
        });

        it('should handle undefined institutionalMemory', () => {
            const result = getInitialLinkFormDataFromInstitutionMemory(undefined);

            expect(result).toEqual({
                variant: LinkFormVariant.URL,
                url: undefined,
                label: undefined,
                showInAssetPreview: false,
            });
        });

        it('should handle institutionalMemory with no settings', () => {
            const mockInstitutionalMemory = {
                url: 'https://example.com',
                label: 'Example Label',
                description: 'Example Description',
                // No settings property
            };

            const result = getInitialLinkFormDataFromInstitutionMemory(mockInstitutionalMemory, false);

            expect(result).toEqual({
                variant: LinkFormVariant.URL,
                url: 'https://example.com',
                label: 'Example Label',
                showInAssetPreview: false, // Should default to false when settings is undefined
            });
        });
    });

    describe('getGeneralizedLinkFormDataFromFormData', () => {
        it('should convert UploadFile variant to generalized form data', () => {
            const formData = {
                variant: LinkFormVariant.UploadFile,
                url: 'https://example.com', // This should be ignored for upload file variant
                fileUrl: 'http://localhost:9002/fileUpload/path/to/file.pdf',
                label: 'Example File',
                showInAssetPreview: true,
            };

            const result = getGeneralizedLinkFormDataFromFormData(formData);

            expect(result).toEqual({
                url: 'http://localhost:9002/fileUpload/path/to/file.pdf', // Should use fileUrl
                label: 'Example File',
                showInAssetPreview: true,
            });
        });

        it('should convert URL variant to generalized form data', () => {
            const formData = {
                variant: LinkFormVariant.URL,
                url: 'https://example.com',
                fileUrl: '', // This should be ignored for URL variant
                label: 'Example Label',
                showInAssetPreview: false,
            };

            const result = getGeneralizedLinkFormDataFromFormData(formData);

            expect(result).toEqual({
                url: 'https://example.com', // Should use url
                label: 'Example Label',
                showInAssetPreview: false,
            });
        });
    });
});
