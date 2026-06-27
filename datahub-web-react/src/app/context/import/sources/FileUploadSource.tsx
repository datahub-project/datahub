import { FileDragAndDropArea, FileNode } from '@components';
import React, { useCallback, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

const ACCEPTED_EXTENSIONS = new Set([
    '.md',
    '.txt',
    '.docx',
    '.html',
    '.htm',
    '.markdown',
    '.rst',
    '.csv',
    '.json',
    '.yaml',
    '.yml',
]);

const ACCEPTED_EXTENSIONS_DISPLAY = Array.from(ACCEPTED_EXTENSIONS)
    .filter((e) => !['.htm', '.markdown', '.yml'].includes(e))
    .join(', ');

const FileList = styled.div`
    display: flex;
    flex-direction: column;
    gap: 4px;
    margin-top: 12px;
    max-height: 200px;
    overflow-y: auto;
`;

const WarningText = styled.span`
    display: block;
    margin-top: 8px;
    color: ${({ theme }) => theme.colors.iconWarning};
    font-size: 12px;
`;

const SecondaryText = styled.span`
    display: block;
    color: ${({ theme }) => theme.colors.textSecondary};
    font-size: 12px;
`;

type FileUploadSourceProps = {
    files: File[];
    onFilesChange: (files: File[]) => void;
};

function getExtension(name: string): string {
    const dot = name.lastIndexOf('.');
    return dot >= 0 ? name.substring(dot).toLowerCase() : '';
}

export default function FileUploadSource({ files, onFilesChange }: FileUploadSourceProps) {
    const { t } = useTranslation('misc');
    const [rejectedNames, setRejectedNames] = useState<string[]>([]);

    const handleFilesUpload = useCallback(
        async (newFiles: File[]) => {
            const existingNames = new Set(files.map((f) => f.name));

            const accepted: File[] = [];
            const rejected: string[] = [];

            newFiles.forEach((f) => {
                if (existingNames.has(f.name)) return;
                if (ACCEPTED_EXTENSIONS.has(getExtension(f.name))) {
                    accepted.push(f);
                } else {
                    rejected.push(f.name);
                }
            });

            if (accepted.length > 0) {
                onFilesChange([...files, ...accepted]);
            }
            setRejectedNames(rejected);
        },
        [files, onFilesChange],
    );

    const handleRemove = useCallback(
        (index: number) => {
            onFilesChange(files.filter((_, i) => i !== index));
        },
        [files, onFilesChange],
    );

    return (
        <div>
            <FileDragAndDropArea
                onFilesUpload={handleFilesUpload}
                description={null}
                fileInputTestId="import-documents-file-input"
            />

            {rejectedNames.length > 0 && (
                <WarningText>
                    {t('context.import.skippedUnsupported', {
                        count: rejectedNames.length,
                        names: rejectedNames.join(', '),
                    })}
                </WarningText>
            )}

            {files.length > 0 && (
                <>
                    <FileList>
                        {files.map((file, index) => (
                            <FileNode key={file.name} fileName={file.name} border onClose={() => handleRemove(index)} />
                        ))}
                    </FileList>
                    <SecondaryText style={{ marginTop: 4 }}>
                        {t('context.import.filesSelected', { count: files.length })}
                    </SecondaryText>
                </>
            )}
            <SecondaryText style={{ marginTop: 8 }}>
                {t('context.import.supportedFormats', { formats: ACCEPTED_EXTENSIONS_DISPLAY })}
            </SecondaryText>
        </div>
    );
}
