import { GithubLogo } from '@phosphor-icons/react/dist/csr/GithubLogo';
import { Upload } from '@phosphor-icons/react/dist/csr/Upload';
import React from 'react';

import {
    HelperText,
    SourceCard,
    SourceGrid,
    SourceIcon,
    SourceLogo,
} from '@app/context/import/components/importDocumentsModal.styles';
import { ImportSourceType } from '@app/context/import/import.types';
import { Text } from '@src/alchemy-components';

import confluenceLogo from '@images/confluencelogo.svg';
import notionLogo from '@images/notionlogo.png';

type ImportDocumentsSourceGridProps = {
    canImportFromScheduledSources: boolean;
    onSelectSource: (source: ImportSourceType) => void;
};

export default function ImportDocumentsSourceGrid({
    canImportFromScheduledSources,
    onSelectSource,
}: ImportDocumentsSourceGridProps) {
    const columnCount = canImportFromScheduledSources ? 2 : 1;

    return (
        <SourceGrid $columns={columnCount}>
            <SourceCard type="button" onClick={() => onSelectSource(ImportSourceType.FILE_UPLOAD)}>
                <SourceIcon>
                    <Upload size={32} />
                </SourceIcon>
                <Text weight="semiBold">Upload Files</Text>
                <HelperText>Upload .md, .txt, .docx files</HelperText>
            </SourceCard>
            {canImportFromScheduledSources && (
                <>
                    <SourceCard type="button" onClick={() => onSelectSource(ImportSourceType.GITHUB)}>
                        <SourceIcon>
                            <GithubLogo size={32} />
                        </SourceIcon>
                        <Text weight="semiBold">GitHub Repository</Text>
                        <HelperText>Schedule imports via an ingestion source</HelperText>
                    </SourceCard>
                    <SourceCard type="button" onClick={() => onSelectSource(ImportSourceType.NOTION)}>
                        <SourceLogo src={notionLogo} alt="Notion" />
                        <Text weight="semiBold">Notion</Text>
                        <HelperText>Schedule imports via an ingestion source</HelperText>
                    </SourceCard>
                    <SourceCard type="button" onClick={() => onSelectSource(ImportSourceType.CONFLUENCE)}>
                        <SourceLogo src={confluenceLogo} alt="Confluence" />
                        <Text weight="semiBold">Confluence</Text>
                        <HelperText>Schedule imports via an ingestion source</HelperText>
                    </SourceCard>
                </>
            )}
        </SourceGrid>
    );
}
