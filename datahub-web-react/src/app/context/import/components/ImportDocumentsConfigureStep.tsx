import React from 'react';

import { HelperText } from '@app/context/import/components/importDocumentsModal.styles';
import { ImportSourceType } from '@app/context/import/import.types';
import FileUploadSource from '@app/context/import/sources/FileUploadSource';

type ImportDocumentsConfigureStepProps = {
    sourceType: ImportSourceType;
    uploadFiles: File[];
    onFilesChange: (files: File[]) => void;
};

const SCHEDULED_SOURCE_COPY: Partial<Record<ImportSourceType, string>> = {
    [ImportSourceType.GITHUB]:
        'Configure a scheduled GitHub Documents ingestion source. Choose repository, branch, and credentials on the next screen.',
    [ImportSourceType.NOTION]:
        'Configure a scheduled Notion ingestion source. Choose workspace credentials and pages on the next screen.',
    [ImportSourceType.CONFLUENCE]:
        'Configure a scheduled Confluence ingestion source. Choose site credentials and spaces on the next screen.',
};

export default function ImportDocumentsConfigureStep({
    sourceType,
    uploadFiles,
    onFilesChange,
}: ImportDocumentsConfigureStepProps) {
    return (
        <>
            {sourceType === ImportSourceType.FILE_UPLOAD && (
                <FileUploadSource files={uploadFiles} onFilesChange={onFilesChange} />
            )}
            {sourceType !== ImportSourceType.FILE_UPLOAD && (
                <HelperText>{SCHEDULED_SOURCE_COPY[sourceType]}</HelperText>
            )}
        </>
    );
}
