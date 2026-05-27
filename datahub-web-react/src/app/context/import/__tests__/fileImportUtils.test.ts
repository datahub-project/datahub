import { describe, expect, it } from 'vitest';

import {
    TEXT_EXTENSIONS,
    extractTextFromFile,
    getExtension,
    mapImportUseCase,
    stripHtmlTags,
} from '@app/context/import/fileImportUtils';
import { ImportUseCase } from '@app/context/import/import.types';

import { DocumentImportUseCase } from '@types';

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
});
