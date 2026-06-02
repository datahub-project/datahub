import { useCallback, useState } from 'react';

import { extractTextFromFile, mapImportUseCase } from '@app/context/import/fileImportUtils';
import { ImportUseCase } from '@app/context/import/import.types';

import { useImportDocumentsFromFilesMutation } from '@graphql/document.generated';

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

export function useImportFromFiles() {
    const [error, setError] = useState<string | null>(null);
    const [result, setResult] = useState<FileImportResult | null>(null);
    const [importMutation, { loading }] = useImportDocumentsFromFilesMutation();

    const importFiles = useCallback(
        async (files: File[], showInGlobalContext: boolean, useCase: ImportUseCase) => {
            setError(null);
            setResult(null);

            try {
                const parsed = await Promise.all(
                    files.map(async (file) => {
                        const text = await extractTextFromFile(file);
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
                            useCase: mapImportUseCase(useCase),
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
