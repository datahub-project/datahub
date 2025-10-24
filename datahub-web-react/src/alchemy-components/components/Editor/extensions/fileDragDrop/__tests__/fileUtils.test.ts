import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import {
    createFileNodeAttributes,
    generateFileId,
    getExtensionFromFileName,
    getFileIconFromExtension,
    getFileTypeFromFilename,
    getFileTypeFromUrl,
    handleFileDownload,
    isFileTypeSupported,
    isFileUrl,
    validateFile,
} from '@components/components/Editor/extensions/fileDragDrop/fileUtils';

describe('fileUtils', () => {
    describe('generateFileId', () => {
        it('should generate a unique file ID', () => {
            const id1 = generateFileId();
            const id2 = generateFileId();

            expect(id1).toMatch(/^file_\d+_[a-z0-9]+$/);
            expect(id2).toMatch(/^file_\d+_[a-z0-9]+$/);
            expect(id1).not.toBe(id2);
        });

        it('should always start with "file_"', () => {
            const id = generateFileId();
            expect(id.startsWith('file_')).toBe(true);
        });
    });

    describe('isFileTypeSupported', () => {
        it('should return true for supported file types', () => {
            expect(isFileTypeSupported('image/jpeg')).toBe(true);
            expect(isFileTypeSupported('application/pdf')).toBe(true);
            expect(isFileTypeSupported('text/plain')).toBe(true);
        });

        it('should return false for unsupported file types', () => {
            expect(isFileTypeSupported('application/octet-stream')).toBe(false);
            expect(isFileTypeSupported('video/avi')).toBe(false);
            expect(isFileTypeSupported('application/x-executable')).toBe(false);
        });

        it('should work with custom supported types', () => {
            const customTypes = ['image/png', 'image/gif'];
            expect(isFileTypeSupported('image/png', customTypes)).toBe(true);
            expect(isFileTypeSupported('image/jpeg', customTypes)).toBe(false);
        });

        it('should handle empty string', () => {
            expect(isFileTypeSupported('')).toBe(false);
        });
    });

    describe('createFileNodeAttributes', () => {
        it('should create file node attributes from a File object', () => {
            const mockFile = new File(['content'], 'test.pdf', { type: 'application/pdf' });
            Object.defineProperty(mockFile, 'size', { value: 1024 });

            const attributes = createFileNodeAttributes(mockFile);

            expect(attributes.url).toBe('');
            expect(attributes.name).toBe('test.pdf');
            expect(attributes.type).toBe('application/pdf');
            expect(attributes.size).toBe(1024);
            expect(attributes.id).toMatch(/^file_\d+_[a-z0-9]+$/);
        });

        it('should handle different file types', () => {
            const mockFile = new File(['content'], 'image.png', { type: 'image/png' });
            const attributes = createFileNodeAttributes(mockFile);

            expect(attributes.name).toBe('image.png');
            expect(attributes.type).toBe('image/png');
        });
    });

    describe('validateFile', () => {
        it('should validate files that meet requirements', () => {
            const mockFile = new File(['content'], 'test.pdf', { type: 'application/pdf' });
            Object.defineProperty(mockFile, 'size', { value: 1024 });

            const result = validateFile(mockFile);

            expect(result.isValid).toBe(true);
            expect(result.error).toBeUndefined();
            expect(result.failureType).toBeUndefined();
        });

        it('should reject files that exceed max size and return FILE_SIZE failure type', () => {
            const mockFile = new File(['content'], 'test.pdf', { type: 'application/pdf' });
            Object.defineProperty(mockFile, 'size', { value: 3000000000 }); // 3GB

            const result = validateFile(mockFile);

            expect(result.isValid).toBe(false);
            expect(result.error).toContain('exceeds maximum allowed size');
            expect(result.failureType).toBe('file_size');
        });

        it('should reject files with unsupported types and return FILE_TYPE failure type', () => {
            const mockFile = new File(['content'], 'test.exe', { type: 'application/x-executable' });
            Object.defineProperty(mockFile, 'size', { value: 1024 });

            const result = validateFile(mockFile);

            expect(result.isValid).toBe(false);
            expect(result.error).toContain('not allowed');
            expect(result.failureType).toBe('file_type');
        });

        it('should respect custom max size and return FILE_SIZE failure type', () => {
            const mockFile = new File(['content'], 'test.pdf', { type: 'application/pdf' });
            Object.defineProperty(mockFile, 'size', { value: 2048 });

            const result = validateFile(mockFile, { maxSize: 1024 });

            expect(result.isValid).toBe(false);
            expect(result.error).toContain('exceeds maximum allowed size');
            expect(result.failureType).toBe('file_size');
        });

        it('should respect custom allowed types and return FILE_TYPE failure type', () => {
            const mockFile = new File(['content'], 'test.pdf', { type: 'application/pdf' });
            Object.defineProperty(mockFile, 'size', { value: 1024 });

            const result = validateFile(mockFile, { allowedTypes: ['image/png', 'image/jpeg'] });

            expect(result.isValid).toBe(false);
            expect(result.error).toContain('not allowed');
            expect(result.failureType).toBe('file_type');
        });

        it('should validate when custom options allow file', () => {
            const mockFile = new File(['content'], 'test.pdf', { type: 'application/pdf' });
            Object.defineProperty(mockFile, 'size', { value: 1024 });

            const result = validateFile(mockFile, {
                maxSize: 2048,
                allowedTypes: ['application/pdf'],
            });

            expect(result.isValid).toBe(true);
            expect(result.failureType).toBeUndefined();
        });
    });

    describe('handleFileDownload', () => {
        let windowOpenSpy: any;
        let createElementSpy: any;

        beforeEach(() => {
            // Mock window.open
            windowOpenSpy = vi.spyOn(window, 'open');

            // Mock document.createElement
            createElementSpy = vi.spyOn(document, 'createElement');
        });

        afterEach(() => {
            vi.restoreAllMocks();
        });

        it('should not do anything if url is empty', () => {
            handleFileDownload('', 'test.pdf');
            expect(windowOpenSpy).not.toHaveBeenCalled();
        });

        it('should try to open url in new tab', () => {
            windowOpenSpy.mockReturnValue({}); // Simulate successful window.open

            handleFileDownload('https://example.com/file.pdf', 'test.pdf');

            expect(windowOpenSpy).toHaveBeenCalledWith('https://example.com/file.pdf', '_blank');
        });

        it('should fallback to download link if window.open is blocked', () => {
            windowOpenSpy.mockReturnValue(null); // Simulate blocked popup

            const mockLink = {
                href: '',
                download: '',
                click: vi.fn(),
            };
            createElementSpy.mockReturnValue(mockLink as any);
            const appendChildSpy = vi.spyOn(document.body, 'appendChild').mockImplementation(() => mockLink as any);
            const removeChildSpy = vi.spyOn(document.body, 'removeChild').mockImplementation(() => mockLink as any);

            handleFileDownload('https://example.com/file.pdf', 'test.pdf');

            expect(windowOpenSpy).toHaveBeenCalled();
            expect(createElementSpy).toHaveBeenCalledWith('a');
            expect(mockLink.href).toBe('https://example.com/file.pdf');
            expect(mockLink.download).toBe('test.pdf');
            expect(mockLink.click).toHaveBeenCalled();
            expect(appendChildSpy).toHaveBeenCalled();
            expect(removeChildSpy).toHaveBeenCalled();
        });
    });

    describe('isFileUrl', () => {
        it('should return true for internal file API URLs', () => {
            expect(isFileUrl('https://example.com/openapi/v1/files/123')).toBe(true);
        });

        it('should return false for non-file URLs', () => {
            expect(isFileUrl('https://example.com')).toBe(false);
            expect(isFileUrl('https://example.com/page.html')).toBe(false);
            expect(isFileUrl('https://example.com/api/data')).toBe(false);
        });
    });

    describe('getFileTypeFromUrl', () => {
        it('should extract MIME type from URL with known extension', () => {
            expect(getFileTypeFromUrl('https://example.com/document.pdf')).toBe('application/pdf');
            expect(getFileTypeFromUrl('https://example.com/image.jpg')).toBe('image/jpeg');
            expect(getFileTypeFromUrl('https://example.com/image.png')).toBe('image/png');
            expect(getFileTypeFromUrl('https://example.com/spreadsheet.xlsx')).toBe(
                'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            );
        });

        it('should handle URLs with query parameters', () => {
            expect(getFileTypeFromUrl('https://example.com/file.pdf?token=123')).toBe('');
            // Query params prevent extension extraction
        });

        it('should return empty string for unknown extensions', () => {
            expect(getFileTypeFromUrl('https://example.com/file.unknown')).toBe('');
        });

        it('should return empty string for URLs without extension', () => {
            expect(getFileTypeFromUrl('https://example.com/file')).toBe('');
        });

        it('should be case insensitive', () => {
            expect(getFileTypeFromUrl('https://example.com/file.PDF')).toBe('application/pdf');
            expect(getFileTypeFromUrl('https://example.com/file.PNG')).toBe('image/png');
        });

        it('should handle various file extensions', () => {
            expect(getFileTypeFromUrl('test.docx')).toBe(
                'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            );
            expect(getFileTypeFromUrl('test.mp4')).toBe('video/mp4');
            expect(getFileTypeFromUrl('test.mp3')).toBe('audio/mpeg');
            expect(getFileTypeFromUrl('test.zip')).toBe('application/zip');
        });
    });

    describe('getFileTypeFromFilename', () => {
        it('should extract MIME type from filename', () => {
            expect(getFileTypeFromFilename('document.pdf')).toBe('application/pdf');
            expect(getFileTypeFromFilename('image.jpg')).toBe('image/jpeg');
            expect(getFileTypeFromFilename('spreadsheet.xlsx')).toBe(
                'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            );
        });

        it('should return empty string for unknown extensions', () => {
            expect(getFileTypeFromFilename('file.unknown')).toBe('');
        });

        it('should return empty string for filenames without extension', () => {
            expect(getFileTypeFromFilename('file')).toBe('');
        });

        it('should be case insensitive', () => {
            expect(getFileTypeFromFilename('file.PDF')).toBe('application/pdf');
            expect(getFileTypeFromFilename('file.PNG')).toBe('image/png');
        });

        it('should handle filenames with multiple dots', () => {
            expect(getFileTypeFromFilename('my.file.name.pdf')).toBe('application/pdf');
        });
    });

    describe('getExtensionFromFileName', () => {
        it('should extract extension from filename with single dot', () => {
            expect(getExtensionFromFileName('document.pdf')).toBe('pdf');
            expect(getExtensionFromFileName('image.jpg')).toBe('jpg');
            expect(getExtensionFromFileName('spreadsheet.xlsx')).toBe('xlsx');
        });

        it('should extract extension from filename with multiple dots', () => {
            expect(getExtensionFromFileName('my.file.name.pdf')).toBe('pdf');
            expect(getExtensionFromFileName('archive.tar.gz')).toBe('gz');
        });

        it('should be case insensitive', () => {
            expect(getExtensionFromFileName('file.PDF')).toBe('pdf');
            expect(getExtensionFromFileName('file.PNG')).toBe('png');
            expect(getExtensionFromFileName('file.TXT')).toBe('txt');
        });
    });

    describe('getFileIconFromExtension', () => {
        it('should return FilePdf for pdf extension', () => {
            expect(getFileIconFromExtension('pdf')).toBe('FilePdf');
            expect(getFileIconFromExtension('PDF')).toBe('FilePdf'); // case-insensitive
        });

        it('should return FileWord for doc and docx extensions', () => {
            expect(getFileIconFromExtension('doc')).toBe('FileWord');
            expect(getFileIconFromExtension('DOCX')).toBe('FileWord');
        });

        it('should return FileText for txt, md, rtf extensions', () => {
            expect(getFileIconFromExtension('txt')).toBe('FileText');
            expect(getFileIconFromExtension('md')).toBe('FileText');
            expect(getFileIconFromExtension('RTF')).toBe('FileText');
        });

        it('should return FileXls for xls and xlsx extensions', () => {
            expect(getFileIconFromExtension('xls')).toBe('FileXls');
            expect(getFileIconFromExtension('XLSX')).toBe('FileXls');
        });

        it('should return FilePpt for ppt and pptx extensions', () => {
            expect(getFileIconFromExtension('ppt')).toBe('FilePpt');
            expect(getFileIconFromExtension('PPTX')).toBe('FilePpt');
        });

        it('should return FileJpg for image extensions', () => {
            ['jpg', 'jpeg'].forEach((ext) => {
                expect(getFileIconFromExtension(ext)).toBe('FileJpg');
                expect(getFileIconFromExtension(ext.toUpperCase())).toBe('FileJpg');
            });
        });

        it('should return FilePng for png extensions', () => {
            ['png'].forEach((ext) => {
                expect(getFileIconFromExtension(ext)).toBe('FilePng');
                expect(getFileIconFromExtension(ext.toUpperCase())).toBe('FilePng');
            });
        });

        it('should return FileImage for other image extensions', () => {
            ['gif', 'webp', 'bmp', 'tiff'].forEach((ext) => {
                expect(getFileIconFromExtension(ext)).toBe('FileImage');
                expect(getFileIconFromExtension(ext.toUpperCase())).toBe('FileImage');
            });
        });

        it('should return FileVideo for video extensions', () => {
            ['mp4', 'wmv', 'mov'].forEach((ext) => {
                expect(getFileIconFromExtension(ext)).toBe('FileVideo');
                expect(getFileIconFromExtension(ext.toUpperCase())).toBe('FileVideo');
            });
        });

        it('should return FileAudio for mp3 extension', () => {
            expect(getFileIconFromExtension('mp3')).toBe('FileAudio');
            expect(getFileIconFromExtension('MP3')).toBe('FileAudio');
        });

        it('should return FileZip for archive extensions', () => {
            ['zip', 'rar', 'gz'].forEach((ext) => {
                expect(getFileIconFromExtension(ext)).toBe('FileZip');
                expect(getFileIconFromExtension(ext.toUpperCase())).toBe('FileZip');
            });
        });

        it('should return FileCsv for csv extension', () => {
            expect(getFileIconFromExtension('csv')).toBe('FileCsv');
            expect(getFileIconFromExtension('CSV')).toBe('FileCsv');
        });

        it('should return FileArrowDown for unknown extensions', () => {
            expect(getFileIconFromExtension('unknown')).toBe('FileArrowDown');
            expect(getFileIconFromExtension('')).toBe('FileArrowDown');
        });
    });
});
