import { Upload } from '@phosphor-icons/react/dist/csr/Upload';
import React from 'react';
import { useTranslation } from 'react-i18next';

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
import githubLogo from '@images/githublogo.png';
import notionLogo from '@images/notionlogo.png';

type ImportDocumentsSourceGridProps = {
    canImportFromScheduledSources: boolean;
    onSelectSource: (source: ImportSourceType) => void;
};

export default function ImportDocumentsSourceGrid({
    canImportFromScheduledSources,
    onSelectSource,
}: ImportDocumentsSourceGridProps) {
    const { t } = useTranslation('misc');
    const columnCount = canImportFromScheduledSources ? 2 : 1;

    return (
        <SourceGrid $columns={columnCount}>
            <SourceCard type="button" onClick={() => onSelectSource(ImportSourceType.FILE_UPLOAD)}>
                <SourceIcon>
                    <Upload size={32} />
                </SourceIcon>
                <Text weight="semiBold">{t('context.import.uploadFilesTitle')}</Text>
                <HelperText>{t('context.import.uploadFilesDescription')}</HelperText>
            </SourceCard>
            {canImportFromScheduledSources && (
                <>
                    <SourceCard type="button" onClick={() => onSelectSource(ImportSourceType.GITHUB)}>
                        <SourceLogo src={githubLogo} alt={t('context.import.githubAlt')} />
                        <Text weight="semiBold">{t('context.import.githubTitle')}</Text>
                        <HelperText>{t('context.import.githubDescription')}</HelperText>
                    </SourceCard>
                    <SourceCard type="button" onClick={() => onSelectSource(ImportSourceType.NOTION)}>
                        <SourceLogo src={notionLogo} alt={t('context.import.notionAlt')} />
                        <Text weight="semiBold">{t('context.import.notionTitle')}</Text>
                        <HelperText>{t('context.import.notionDescription')}</HelperText>
                    </SourceCard>
                    <SourceCard type="button" onClick={() => onSelectSource(ImportSourceType.CONFLUENCE)}>
                        <SourceLogo src={confluenceLogo} alt={t('context.import.confluenceAlt')} />
                        <Text weight="semiBold">{t('context.import.confluenceTitle')}</Text>
                        <HelperText>{t('context.import.confluenceDescription')}</HelperText>
                    </SourceCard>
                </>
            )}
        </SourceGrid>
    );
}
