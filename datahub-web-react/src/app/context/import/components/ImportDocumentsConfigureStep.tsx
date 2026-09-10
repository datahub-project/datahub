import React from 'react';
import { useTranslation } from 'react-i18next';

import { HelperText } from '@app/context/import/components/importDocumentsModal.styles';
import { ImportSourceType } from '@app/context/import/import.types';
import FileUploadSource from '@app/context/import/sources/FileUploadSource';

type ImportDocumentsConfigureStepProps = {
    sourceType: ImportSourceType;
    uploadFiles: File[];
    onFilesChange: (files: File[]) => void;
};

const SCHEDULED_SOURCE_COPY_KEYS: Partial<Record<ImportSourceType, string>> = {
    [ImportSourceType.GITHUB]: 'context.import.scheduledSource.github',
    [ImportSourceType.NOTION]: 'context.import.scheduledSource.notion',
    [ImportSourceType.CONFLUENCE]: 'context.import.scheduledSource.confluence',
};

export default function ImportDocumentsConfigureStep({
    sourceType,
    uploadFiles,
    onFilesChange,
}: ImportDocumentsConfigureStepProps) {
    const { t } = useTranslation('misc');
    const scheduledSourceCopyKey = SCHEDULED_SOURCE_COPY_KEYS[sourceType];

    return (
        <>
            {sourceType === ImportSourceType.FILE_UPLOAD && (
                <FileUploadSource files={uploadFiles} onFilesChange={onFilesChange} />
            )}
            {scheduledSourceCopyKey && <HelperText>{t(scheduledSourceCopyKey)}</HelperText>}
        </>
    );
}
