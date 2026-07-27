import { Upload } from '@phosphor-icons/react/dist/csr/Upload';
import React from 'react';
import { useTranslation } from 'react-i18next';
import { useTheme } from 'styled-components';

import { SourceGrid } from '@app/context/import/components/importDocumentsModal.styles';
import { ImportSourceType } from '@app/context/import/import.types';
import { Card } from '@src/alchemy-components';

import confluenceLogo from '@images/confluencelogo.svg';
import githubLogo from '@images/githublogo.png';
import notionLogo from '@images/notionlogo.png';

const ICON_SIZE = 24;
const LOGO_STYLE: React.CSSProperties = { width: ICON_SIZE, height: ICON_SIZE, objectFit: 'contain' };

type ImportDocumentsSourceGridProps = {
    canImportFromScheduledSources: boolean;
    onSelectSource: (source: ImportSourceType) => void;
};

export default function ImportDocumentsSourceGrid({
    canImportFromScheduledSources,
    onSelectSource,
}: ImportDocumentsSourceGridProps) {
    const { t } = useTranslation('misc');
    const theme = useTheme();
    const columnCount = canImportFromScheduledSources ? 2 : 1;

    return (
        <SourceGrid $columns={columnCount}>
            <Card
                title={t('context.import.uploadFilesTitle')}
                subTitle={t('context.import.uploadFilesDescription')}
                icon={<Upload size={ICON_SIZE} color={theme.colors.iconBrand} />}
                onClick={() => onSelectSource(ImportSourceType.FILE_UPLOAD)}
            />
            {canImportFromScheduledSources && (
                <>
                    <Card
                        title={t('context.import.githubTitle')}
                        subTitle={t('context.import.githubDescription')}
                        icon={<img src={githubLogo} alt={t('context.import.githubAlt')} style={LOGO_STYLE} />}
                        onClick={() => onSelectSource(ImportSourceType.GITHUB)}
                    />
                    <Card
                        title={t('context.import.notionTitle')}
                        subTitle={t('context.import.notionDescription')}
                        icon={<img src={notionLogo} alt={t('context.import.notionAlt')} style={LOGO_STYLE} />}
                        onClick={() => onSelectSource(ImportSourceType.NOTION)}
                    />
                    <Card
                        title={t('context.import.confluenceTitle')}
                        subTitle={t('context.import.confluenceDescription')}
                        icon={<img src={confluenceLogo} alt={t('context.import.confluenceAlt')} style={LOGO_STYLE} />}
                        onClick={() => onSelectSource(ImportSourceType.CONFLUENCE)}
                    />
                </>
            )}
        </SourceGrid>
    );
}
