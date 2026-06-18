import { act, renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import '@app/context/import/__tests__/testSetup';
import { extractTextFromFile } from '@app/context/import/fileImportUtils';
import { useImportFromFiles } from '@app/context/import/hooks/useImportFromFiles';
import { ImportUseCase } from '@app/context/import/import.types';

import { DocumentImportUseCase } from '@types';

const mockImportMutation = vi.fn();

vi.mock('@graphql/document.generated', () => ({
    useImportDocumentsFromFilesMutation: () => [mockImportMutation, { loading: false }],
}));

vi.mock('@app/context/import/fileImportUtils', async (importOriginal) => {
    const actual = await importOriginal<typeof import('@app/context/import/fileImportUtils')>();
    return {
        ...actual,
        extractTextFromFile: vi.fn(),
    };
});

const mockedExtractText = vi.mocked(extractTextFromFile);

function makeFile(name: string, content = 'file content'): File {
    return new File([content], name, { type: 'text/plain' });
}

describe('useImportFromFiles', () => {
    beforeEach(() => {
        mockImportMutation.mockReset();
        mockedExtractText.mockReset();
    });

    it('sets error and skips mutation when no files have readable content', async () => {
        mockedExtractText.mockResolvedValue(null);
        const { result } = renderHook(() => useImportFromFiles());

        let importResult: unknown;
        await act(async () => {
            importResult = await result.current.importFiles(
                [makeFile('empty.txt')],
                true,
                ImportUseCase.CONTEXT_DOCUMENT,
            );
        });

        expect(importResult).toBeNull();
        expect(result.current.error).toBe('No readable content found in the selected files.');
        expect(result.current.result).toBeNull();
        expect(mockImportMutation).not.toHaveBeenCalled();
    });

    it('filters blank files and sends readable documents to GraphQL', async () => {
        mockedExtractText.mockImplementation(async (file) => {
            if (file.name === 'valid.txt') {
                return 'hello world';
            }
            return file.name === 'whitespace.txt' ? '   ' : null;
        });
        mockImportMutation.mockResolvedValue({
            data: {
                importDocumentsFromFiles: {
                    createdCount: 1,
                    updatedCount: 0,
                    failedCount: 0,
                    errors: [],
                    documentUrns: ['urn:li:document:abc'],
                },
            },
        });

        const { result } = renderHook(() => useImportFromFiles());

        await act(async () => {
            await result.current.importFiles(
                [makeFile('valid.txt'), makeFile('blank.bin'), makeFile('whitespace.txt')],
                false,
                ImportUseCase.SKILL,
            );
        });

        expect(mockImportMutation).toHaveBeenCalledWith({
            variables: {
                input: {
                    documents: [{ fileName: 'valid.txt', content: 'hello world' }],
                    showInGlobalContext: false,
                    useCase: DocumentImportUseCase.Skill,
                },
            },
        });
        expect(result.current.error).toBeNull();
        expect(result.current.result).toEqual({
            createdCount: 1,
            updatedCount: 0,
            failedCount: 0,
            errors: [],
            documentUrns: ['urn:li:document:abc'],
        });
    });

    it('sets error when mutation throws', async () => {
        mockedExtractText.mockResolvedValue('content');
        mockImportMutation.mockRejectedValue(new Error('GraphQL unavailable'));

        const { result } = renderHook(() => useImportFromFiles());

        await act(async () => {
            await result.current.importFiles([makeFile('notes.txt')], true, ImportUseCase.CONTEXT_DOCUMENT);
        });

        expect(result.current.error).toBe('GraphQL unavailable');
        expect(result.current.result).toBeNull();
    });

    it('returns mutation data on success', async () => {
        mockedExtractText.mockResolvedValue('content');
        const mutationResult = {
            createdCount: 2,
            updatedCount: 1,
            failedCount: 0,
            errors: [],
            documentUrns: ['urn:li:document:one', 'urn:li:document:two'],
        };
        mockImportMutation.mockResolvedValue({
            data: {
                importDocumentsFromFiles: mutationResult,
            },
        });

        const { result } = renderHook(() => useImportFromFiles());

        let importResult: unknown;
        await act(async () => {
            importResult = await result.current.importFiles(
                [makeFile('a.txt'), makeFile('b.txt')],
                true,
                ImportUseCase.CONTEXT_DOCUMENT,
            );
        });

        expect(importResult).toEqual(mutationResult);
        expect(result.current.result).toEqual(mutationResult);
        expect(result.current.error).toBeNull();
    });
});
