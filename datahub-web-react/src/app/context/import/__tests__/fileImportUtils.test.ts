import { describe, expect, it, vi } from 'vitest';

import '@app/context/import/__tests__/testSetup';
import {
    TEXT_EXTENSIONS,
    extractDocxText,
    extractTextFromFile,
    getExtension,
    mapImportUseCase,
    stripHtmlTags,
} from '@app/context/import/fileImportUtils';
import { ImportUseCase } from '@app/context/import/import.types';

import { DocumentImportUseCase } from '@types';

vi.mock('mammoth', () => ({
    extractRawText: vi.fn(async () => ({ value: 'Doc body' })),
}));

describe('fileImportUtils', () => {
    it('getExtension returns lowercase extension', () => {
        expect(getExtension('README.MD')).toBe('.md');
        expect(getExtension('noext')).toBe('');
    });

    it('stripHtmlTags removes markup', () => {
        expect(stripHtmlTags('<p>Hello <b>world</b></p>')).toBe('Hello world');
    });

    it('mapImportUseCase maps skill vs context', () => {
        expect(mapImportUseCase(ImportUseCase.SKILL)).toBe(DocumentImportUseCase.Skill);
        expect(mapImportUseCase(ImportUseCase.CONTEXT_DOCUMENT)).toBe(DocumentImportUseCase.ContextDocument);
    });

    it('extractTextFromFile reads plain text files', async () => {
        const file = new File(['# Title\n\nBody'], 'note.md', { type: 'text/markdown' });
        expect(TEXT_EXTENSIONS.has('.md')).toBe(true);
        await expect(extractTextFromFile(file)).resolves.toBe('# Title\n\nBody');
    });

    it('extractTextFromFile strips HTML files', async () => {
        const file = new File(['<h1>Title</h1><p>Body</p>'], 'page.html', { type: 'text/html' });
        await expect(extractTextFromFile(file)).resolves.toBe('Title Body');
    });

    it('extractTextFromFile returns null for unsupported extensions', async () => {
        const file = new File(['binary'], 'image.png', { type: 'image/png' });
        await expect(extractTextFromFile(file)).resolves.toBeNull();
    });

    it('extractTextFromFile extracts docx text via mammoth', async () => {
        const file = new File([new ArrayBuffer(8)], 'notes.docx', {
            type: 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        });

        await expect(extractTextFromFile(file)).resolves.toBe('Doc body');
    });

    it('extractDocxText returns null when mammoth returns empty text', async () => {
        const mammoth = await import('mammoth');
        vi.mocked(mammoth.extractRawText).mockResolvedValueOnce({ value: '' });

        await expect(extractDocxText(new ArrayBuffer(8))).resolves.toBeNull();
    });
});
