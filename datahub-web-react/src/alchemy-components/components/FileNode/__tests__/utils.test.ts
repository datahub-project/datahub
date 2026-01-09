import { describe, expect, it } from 'vitest';

import { getFileIconFromExtension } from '@components/components/FileNode/utils';

describe('utils', () => {
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
