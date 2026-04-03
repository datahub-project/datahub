import mammoth from 'mammoth';
import { useCallback, useState } from 'react';

import { ImportUseCase } from '@app/context/import/import.types';

import { useImportDocumentsFromFilesMutation } from '@graphql/document.generated';
import { DocumentImportUseCase } from '@types';

type DocumentFileInput = {
    fileName: string;
    content: string;
};

type FileImportResult = {
    createdCount: number;
    updatedCount: number;
    failedCount: number;
    errors: string[];
    documentUrns: string[];
};

const TEXT_EXTENSIONS = new Set(['.md', '.markdown', '.txt', '.rst', '.csv', '.json', '.yaml', '.yml']);
const HTML_EXTENSIONS = new Set(['.html', '.htm']);

function getExtension(name: string): string {
    const dot = name.lastIndexOf('.');
    return dot >= 0 ? name.substring(dot).toLowerCase() : '';
}

function stripHtmlTags(html: string): string {
    return html
        .replace(/<[^>]*>/g, ' ')
        .replace(/\s+/g, ' ')
        .trim();
}

async function extractText(file: File): Promise<string | null> {
    const ext = getExtension(file.name);

    if (TEXT_EXTENSIONS.has(ext)) {
        return file.text();
    }
    if (HTML_EXTENSIONS.has(ext)) {
        const html = await file.text();
        return stripHtmlTags(html);
    }
    if (ext === '.docx') {
        const arrayBuffer = await file.arrayBuffer();
        const result = await mammoth.extractRawText({ arrayBuffer });
        return result.value || null;
    }
    return null;
}

function mapUseCase(useCase: ImportUseCase): DocumentImportUseCase {
    return useCase === ImportUseCase.SKILL ? DocumentImportUseCase.Skill : DocumentImportUseCase.ContextDocument;
}

export function useImportFromFiles() {
    const [error, setError] = useState<string | null>(null);
    const [result, setResult] = useState<FileImportResult | null>(null);
    const [importMutation, { loading }] = useImportDocumentsFromFilesMutation();

    const importFiles = useCallback(
        async (
            files: File[],
            showInGlobalContext: boolean,
            useCase: ImportUseCase,
            parentDocumentUrn?: string | null,
        ) => {
            setError(null);
            setResult(null);

            try {
                const parsed = await Promise.all(
                    files.map(async (file) => {
                        const text = await extractText(file);
                        return text && text.trim().length > 0 ? { fileName: file.name, content: text } : null;
                    }),
                );
                const documents: DocumentFileInput[] = parsed.filter((d): d is DocumentFileInput => d !== null);

                if (documents.length === 0) {
                    setError('No readable content found in the selected files.');
                    return null;
                }

                const response = await importMutation({
                    variables: {
                        input: {
                            documents,
                            showInGlobalContext,
                            useCase: mapUseCase(useCase),
                            parentDocumentUrn: parentDocumentUrn ?? undefined,
                        },
                    },
                });

                const data = response.data?.importDocumentsFromFiles ?? null;
                if (data) {
                    setResult(data);
                }
                return data;
            } catch (e) {
                const message = e instanceof Error ? e.message : 'Unknown error during file import';
                setError(message);
                return null;
            }
        },
        [importMutation],
    );

    return { importFiles, loading, error, result };
}
