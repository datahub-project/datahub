import { useCallback, useState } from 'react';

import { ImportUseCase } from '@app/context/import/import.types';

type FileImportResult = {
    createdCount: number;
    updatedCount: number;
    failedCount: number;
    errors: string[];
    documentUrns: string[];
};

export function useImportFromFiles() {
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);
    const [result, setResult] = useState<FileImportResult | null>(null);

    const importFiles = useCallback(
        async (
            files: File[],
            showInGlobalContext: boolean,
            useCase: ImportUseCase,
            parentDocumentUrn?: string | null,
        ) => {
            setLoading(true);
            setError(null);
            setResult(null);

            try {
                const formData = new FormData();
                files.forEach((file) => formData.append('files', file));
                formData.append('showInGlobalContext', String(showInGlobalContext));
                formData.append('useCase', useCase);
                if (parentDocumentUrn) {
                    formData.append('parentDocumentUrn', parentDocumentUrn);
                }

                const response = await fetch('/openapi/v1/documents/import/files', {
                    method: 'POST',
                    body: formData,
                    credentials: 'include',
                });

                if (!response.ok) {
                    const errorBody = await response.text();
                    throw new Error(errorBody || `Upload failed with status ${response.status}`);
                }

                const data: FileImportResult = await response.json();
                setResult(data);
                return data;
            } catch (e) {
                const message = e instanceof Error ? e.message : 'Unknown error during file upload';
                setError(message);
                return null;
            } finally {
                setLoading(false);
            }
        },
        [],
    );

    return { importFiles, loading, error, result };
}
