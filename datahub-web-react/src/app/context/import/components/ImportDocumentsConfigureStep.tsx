import React from 'react';

import ImportParentSelector from '@app/context/import/ImportParentSelector';
import { HelperText } from '@app/context/import/components/importDocumentsModal.styles';
import { ImportSourceType } from '@app/context/import/import.types';
import FileUploadSource from '@app/context/import/sources/FileUploadSource';
import type { DocumentTreeNode } from '@app/document/DocumentTreeContext';

type ImportDocumentsConfigureStepProps = {
    sourceType: ImportSourceType;
    uploadFiles: File[];
    onFilesChange: (files: File[]) => void;
    parentDocumentUrn: string | null;
    onSelectParent: (urn: string | null) => void;
    getNode?: (urn: string) => DocumentTreeNode | undefined;
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
    parentDocumentUrn,
    onSelectParent,
    getNode,
}: ImportDocumentsConfigureStepProps) {
    return (
        <>
            {sourceType === ImportSourceType.FILE_UPLOAD && (
                <FileUploadSource files={uploadFiles} onFilesChange={onFilesChange} />
            )}
            {sourceType !== ImportSourceType.FILE_UPLOAD && (
                <HelperText>{SCHEDULED_SOURCE_COPY[sourceType]}</HelperText>
            )}
            <ImportParentSelector
                selectedParentUrn={parentDocumentUrn}
                onSelectParent={onSelectParent}
                getNode={getNode}
            />
        </>
    );
}
